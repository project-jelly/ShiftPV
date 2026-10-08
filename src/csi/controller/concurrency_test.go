package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
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
	var reads atomic.Int32
	secondRead := make(chan struct{})
	observations := &observedSteps{}
	s.ObserveStep = func(step string, elapsed time.Duration) {
		observations.record(step, elapsed)
		if step == "create_intent_read" && reads.Add(1) == 3 {
			close(secondRead)
		}
	}
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
	select {
	case <-secondRead:
	case <-ctx.Done():
		t.Fatal("second admission did not reach the intent read")
	}
	// The current node lock serializes admission even for independent Pools.
	if observations.count("create_node_lock_wait") != 1 {
		t.Fatal("second admission bypassed node serialization")
	}
	close(probe.release)
	for range 2 {
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
