package poolcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func retiringPool() volumeapi.Pool {
	return volumeapi.Pool{Name: "lost", UID: "lost-uid", NodeName: "node", RetirementRequest: "lost-uid",
		Finalizers: []string{volumeapi.PoolProtectionFinalizer}, Status: volumeapi.PoolStatus{RegistrationApproved: true}}
}

func TestPoolRetirementWaitsForReferencesAndNeverReleasesStorage(t *testing.T) {
	pools := &memoryPools{pools: []volumeapi.Pool{retiringPool()}}
	safety := &memorySafety{report: uninstallcheck.Report{Blockers: []uninstallcheck.Blocker{{Kind: "ShiftPVMove", Name: "active"}}}}
	r := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}
	if err := r.ReconcileAll(context.Background()); err != nil || len(pools.retired) != 0 {
		t.Fatalf("live reference retired: retired=%v error=%v", pools.retired, err)
	}
	safety.report, safety.err = uninstallcheck.Report{}, errors.New("read failed")
	if err := r.ReconcileAll(context.Background()); err == nil || len(pools.retired) != 0 {
		t.Fatal("failed observation retired registration")
	}
	safety.err = nil
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	pools.pools[0].RetirementRequest = "" // Losing the response/request cannot reactivate it.
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.retired) != 1 || len(pools.approved) != 0 || len(pools.removed) != 0 || !pools.pools[0].RegistrationRetired() {
		t.Fatalf("retired=%v release=%v remove=%v", pools.retired, pools.approved, pools.removed)
	}
}

func TestPoolRetirementSharesTheAdmissionUIDFence(t *testing.T) {
	pool := retiringPool()
	pools := &memoryPools{pools: []volumeapi.Pool{pool}}
	safety := &memorySafety{called: make(chan struct{}, 1)}
	locks := &poolcapacity.Locker{}
	release := sync.OnceFunc(locks.Lock(pool.UID))
	defer release()
	r := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: locks, Interval: time.Second}
	done := make(chan error, 1)
	go func() { done <- r.ReconcileAll(context.Background()) }()
	select {
	case <-safety.called:
		t.Fatal("retirement observation bypassed ongoing admission")
	case <-time.After(20 * time.Millisecond):
	}
	// Admission persists its reference before dropping the same UID lock.
	safety.report.Blockers = []uninstallcheck.Blocker{{Kind: "ShiftPVVolume", Name: "admitted"}}
	release()
	select {
	case err := <-done:
		if err != nil || len(pools.retired) != 0 {
			t.Fatalf("admitted reference lost: error=%v retired=%v", err, pools.retired)
		}
	case <-time.After(time.Second):
		t.Fatal("retirement did not resume after admission")
	}
}
