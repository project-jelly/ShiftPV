package controller

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

type observedSteps struct {
	mu    sync.Mutex
	steps map[string][]time.Duration
}

func (o *observedSteps) record(step string, elapsed time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.steps == nil {
		o.steps = make(map[string][]time.Duration)
	}
	o.steps[step] = append(o.steps[step], elapsed)
}

func (o *observedSteps) count(step string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.steps[step])
}

func TestCreateObservationSeparatesNewAdmissionFromRetry(t *testing.T) {
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	s := capacityService(fake.NewClientset(), "128Mi", nil, probe)
	s.PoolLocks = &poolcapacity.Locker{}
	observations := &observedSteps{}
	s.ObserveStep = observations.record
	r := validCreateRequest("worker-a")
	first, err := s.CreateVolume(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	for step, count := range map[string]int{
		"create_volume_lock_wait": 1, "create_cleanup_fence": 1,
		"create_intent_read": 2, "create_node_lock_wait": 1, "create_pool_list": 1,
		"create_pool_lock_wait": 1, "create_capacity_ledger": 1,
		"create_volume_list": 1, "create_move_list": 1, "create_filesystem_capacity": 1,
		"create_intent_record": 1, "capacity_admission": 1, "create_resume": 0,
		"create_directory": 1, "create_complete": 1, "create_finalize": 1,
		"create_effect": 1, "create_topology": 1,
	} {
		if got := observations.count(step); got != count {
			t.Fatalf("new create step %s count=%d want=%d", step, got, count)
		}
	}
	// A retry must remain visible, while reusing the original capacity hold.
	probe.err = errors.New("retry must not probe capacity again")
	second, err := s.CreateVolume(context.Background(), r)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("retry response=%v error=%v", second, err)
	}
	if observations.count("create_resume") != 1 || observations.count("create_intent_read") != 3 ||
		observations.count("create_intent_record") != 1 || observations.count("create_capacity_ledger") != 1 ||
		observations.count("create_directory") != 2 || probe.callCount() != 1 {
		t.Fatal("retry was hidden or re-entered new capacity admission")
	}
}

type failingLedger struct {
	PoolCapacityRegistry
	failVolumes bool
}

func (r failingLedger) ListVolumes(ctx context.Context) (map[string]volumeapi.State, error) {
	if r.failVolumes {
		return nil, errors.New("injected volume list failure")
	}
	return r.PoolCapacityRegistry.ListVolumes(ctx)
}

func (r failingLedger) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return nil, errors.New("injected move list failure")
}

func TestCreateObservationClosesFailedLedgerStage(t *testing.T) {
	for _, failVolumes := range []bool{true, false} {
		t.Run(map[bool]string{true: "volumes", false: "moves"}[failVolumes], func(t *testing.T) {
			probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
			s := capacityService(fake.NewClientset(), "128Mi", nil, probe)
			s.CapacityPools = failingLedger{s.CapacityPools, failVolumes}
			observations := &observedSteps{}
			s.ObserveStep = observations.record
			if _, err := s.CreateVolume(context.Background(), validCreateRequest("worker-a")); status.Code(err) != codes.Internal {
				t.Fatalf("error=%v", err)
			}
			if observations.count("create_volume_list") != 1 || observations.count("create_capacity_ledger") != 1 || observations.count("capacity_admission") != 1 {
				t.Fatal("failed API stage duration was lost")
			}
			wantMoves := 1
			if failVolumes {
				wantMoves = 0
			}
			if observations.count("create_move_list") != wantMoves || observations.count("create_filesystem_capacity") != 0 ||
				observations.count("create_intent_record") != 0 || probe.callCount() != 0 || s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("failed ledger reached a later stage or ran an effect")
			}
		})
	}
}

type gatedCapacityProbe struct {
	started chan struct{}
	release chan struct{}
}

func (p gatedCapacityProbe) StatFSForPool(ctx context.Context, _ volumeapi.Pool) (poolcapacity.Filesystem, error) {
	close(p.started)
	select {
	case <-p.release:
		return poolcapacity.Filesystem{AvailableBytes: 1 << 30}, nil
	case <-ctx.Done():
		return poolcapacity.Filesystem{}, ctx.Err()
	}
}

func TestCreateLockWaitObservationEndsBeforeProtectedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe := gatedCapacityProbe{started: make(chan struct{}), release: make(chan struct{})}
	s := capacityService(fake.NewClientset(), "128Mi", nil, probe)
	s.PoolLocks = &poolcapacity.Locker{}
	releasePool := s.PoolLocks.Lock("pool-uid")
	defer func() {
		if releasePool != nil {
			releasePool()
		}
	}()
	observations := &observedSteps{}
	poolListed := make(chan struct{})
	s.ObserveStep = func(step string, elapsed time.Duration) {
		observations.record(step, elapsed)
		if step == "create_pool_list" {
			close(poolListed)
		}
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.CreateVolume(ctx, validCreateRequest("worker-a"))
		result <- err
	}()
	select {
	case <-poolListed:
	case <-ctx.Done():
		t.Fatal("create did not reach Pool selection")
	}
	select {
	case <-probe.started:
		t.Fatal("capacity probe bypassed the Pool UID lock")
	default:
	}
	releasePool()
	releasePool = nil
	select {
	case <-probe.started:
	case <-ctx.Done():
		t.Fatal("create did not acquire the released Pool lock")
	}
	// The probe is still blocked: lock wait must already be reported, while
	// capacity/admission durations must not report completion yet.
	if observations.count("create_pool_lock_wait") != 1 || observations.count("create_filesystem_capacity") != 0 || observations.count("capacity_admission") != 0 {
		t.Fatal("lock wait includes protected work or admission finished early")
	}
	close(probe.release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("create did not complete")
	}
}
