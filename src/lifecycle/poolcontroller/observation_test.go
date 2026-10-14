package poolcontroller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDeletionReportsSafetyAndNodeEvidenceWaits(t *testing.T) {
	stamp := metav1.Now()
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", Generation: 4, DeletionTimestamp: &stamp, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	pools := &memoryPools{pools: []volumeapi.Pool{pool}}
	blocker := uninstallcheck.Blocker{Kind: uninstallcheck.PoolInventoryBlockerKind, Name: pool.Name, Reason: "inventory=stale observedGeneration=3 generation=4"}
	safety := &memorySafety{report: uninstallcheck.Report{Blockers: []uninstallcheck.Blocker{blocker}}}
	var observed DeletionObservation
	locks := &poolcapacity.Locker{}
	r := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: locks, Interval: time.Second,
		ObserveDelete: func(p volumeapi.Pool, o DeletionObservation) {
			// Calling the observer while holding the Pool fence would deadlock.
			released := make(chan struct{})
			go func() { unlock := locks.Lock(p.UID); unlock(); close(released) }()
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("observer called under admission fence")
			}
			observed = o
		}}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed.Stage != "SafetyBlocked" || len(observed.Blockers) != 1 || observed.Blockers[0] != blocker || len(pools.approved) != 0 {
		t.Fatalf("safety wait lost cause or approved release: %+v", observed)
	}
	safety.report = uninstallcheck.Report{}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed.Stage != "ReleaseApproval" || len(pools.approved) != 1 {
		t.Fatalf("approval=%+v", observed)
	}
	pools.pools[0].IdentityReleaseApproval = pool.UID
	pools.pools[0].Status.Conditions = []metav1.Condition{{Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionTrue, Reason: "PoolIdentityReleased", ObservedGeneration: 3}}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed.Stage != "NodeReleasePending" || observed.ObservedGeneration != 3 || len(pools.removed) != 0 {
		t.Fatalf("stale node evidence accepted: %+v", observed)
	}
	pools.pools[0].Status.Conditions[0].ObservedGeneration = pool.Generation
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed.Stage != "RemoveProtection" || len(pools.removed) != 1 {
		t.Fatalf("removal=%+v", observed)
	}
	safety.err = errors.New("API unavailable")
	if err := r.ReconcileAll(context.Background()); !errors.Is(err, safety.err) || observed.Stage != "SafetyCheckFailed" || !errors.Is(observed.Err, safety.err) {
		t.Fatalf("safety error lost: %+v %v", observed, err)
	}
}
