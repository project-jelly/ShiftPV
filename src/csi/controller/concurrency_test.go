package controller

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

type independentPoolProbe struct {
	started chan string
	release chan struct{}
}

func (p independentPoolProbe) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	p.started <- pool.UID
	if pool.UID == "pool-uid" {
		select {
		case <-p.release:
		case <-ctx.Done():
			return poolcapacity.Filesystem{}, ctx.Err()
		}
	}
	return poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}, nil
}

func TestConcurrentAdmissionKeepsIndependentPoolHoldsOnSameNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe := independentPoolProbe{started: make(chan string, 2), release: make(chan struct{})}
	s := capacityService(fake.NewClientset(), "8Mi", nil, probe)
	s.PoolLocks = &poolcapacity.Locker{}
	pools := s.CapacityPools.(*fakePoolCapacityRegistry)
	pools.pools = []volumeapi.Pool{pools.pool,
		{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-a", MountPath: "/other", PoolGroup: "other", CapacityLimit: "8Mi"}}
	observations := &observedSteps{}
	s.ObserveStep = observations.record
	type result struct {
		state volumeapi.State
		err   error
	}
	results := make(chan result, 2)
	start := func(name, group string) {
		r := validCreateRequest("worker-a")
		r.Name, r.CapacityRange.RequiredBytes = name, 8<<20
		request, err := parseCreateRequest(r)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			state, err := s.beginCreateWithinPool(ctx, request, map[string]string{PoolGroupKey: group})
			results <- result{state, err}
		}()
	}
	start("first-pool", "default")
	select {
	case uid := <-probe.started:
		if uid != "pool-uid" {
			t.Fatalf("first Pool=%s", uid)
		}
	case <-ctx.Done():
		t.Fatal("first admission did not reach the probe")
	}
	start("second-pool", "other")
	// The second Pool must complete while the first Pool probe is blocked.
	select {
	case got := <-results:
		if got.err != nil || got.state.CurrentCopy == nil || got.state.CurrentCopy.PoolUID != "pool-b-uid" {
			t.Fatalf("independent admission=%#v error=%v", got.state, got.err)
		}
	case <-ctx.Done():
		t.Fatal("independent Pool was serialized behind the blocked probe")
	}
	if observations.count("create_node_lock_wait") != 0 {
		t.Fatal("node-wide admission lock remains")
	}
	close(probe.release)
	for range 1 {
		select {
		case got := <-results:
			if got.err != nil || got.state.CurrentCopy == nil || got.state.CapacityBytes != 8<<20 {
				t.Fatalf("intent=%#v error=%v", got.state, got.err)
			}
			wantUID := "pool-uid"
			if got.state.RequestName == "second-pool" {
				wantUID = "pool-b-uid"
			}
			if got.state.CurrentCopy.PoolUID != wantUID || got.state.InitialNode != "worker-a" {
				t.Fatalf("wrong Pool identity: %#v", got.state)
			}
		case <-ctx.Done():
			t.Fatal("independent Pool admissions did not finish")
		}
	}
	if observations.count("create_pool_lock_wait") != 2 || observations.count("create_intent_record") != 2 {
		t.Fatal("independent reservations were not protected and recorded separately")
	}
	if len(pools.volumes) != 2 {
		t.Fatalf("volume intents=%d, want two", len(pools.volumes))
	}
	for _, uid := range []string{"pool-uid", "pool-b-uid"} {
		reserved, err := poolcapacity.ReservedBytesForPool(pools.volumes, nil, uid)
		if err != nil || reserved != 8<<20 {
			t.Fatalf("Pool %s reserved=%d error=%v", uid, reserved, err)
		}
	}
}

func TestConcurrentAdmissionToSamePoolCannotOversubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, shared := range []bool{true, false} {
		probe := independentPoolProbe{started: make(chan string, 2), release: make(chan struct{})}
		s := capacityService(fake.NewClientset(), "8Mi", nil, probe)
		if shared {
			s.PoolLocks = &poolcapacity.Locker{}
		}
		result := make(chan error, 2)
		start := func(name string) {
			req := validCreateRequest("worker-a")
			req.Name = name
			req.CapacityRange.RequiredBytes = 8 << 20
			parsed, err := parseCreateRequest(req)
			if err != nil {
				t.Fatal(err)
			}
			go func() { _, err := s.beginCreateWithinPool(ctx, parsed, nil); result <- err }()
		}
		start("same-first")
		select {
		case <-probe.started:
		case <-ctx.Done():
			t.Fatal("probe did not start")
		}
		start("same-second")
		close(probe.release)
		success := 0
		for range 2 {
			select {
			case err := <-result:
				if err == nil {
					success++
				} else if status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("same-Pool admission stalled")
			}
		}
		if success != 1 {
			t.Fatalf("shared=%v successful reservations=%d", shared, success)
		}
		pools := s.CapacityPools.(*fakePoolCapacityRegistry)
		reserved, err := poolcapacity.ReservedBytesForPool(pools.volumes, nil, "pool-uid")
		if err != nil || reserved != 8<<20 {
			t.Fatalf("reserved=%d error=%v", reserved, err)
		}
	}
}
