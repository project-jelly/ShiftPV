package poolcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type memoryPools struct {
	pools     []volumeapi.Pool
	ensured   []string
	removed   []string
	approved  []string
	listError error
}

func (m *memoryPools) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return m.pools, m.listError
}

func (m *memoryPools) EnsurePoolFinalizer(_ context.Context, name, uid string) error {
	m.ensured = append(m.ensured, name+"/"+uid)
	return nil
}

func (m *memoryPools) RemovePoolFinalizer(_ context.Context, name, uid string) error {
	m.removed = append(m.removed, name+"/"+uid)
	return nil
}

func (m *memoryPools) ApprovePoolIdentityRelease(_ context.Context, name, uid string) error {
	m.approved = append(m.approved, name+"/"+uid)
	return nil
}

type memorySafety struct {
	report uninstallcheck.Report
	err    error
	after  time.Time
	called chan struct{}
}

type memoryQuiesce struct {
	quiescing bool
	err       error
}

func (m memoryQuiesce) Quiescing(context.Context) (string, bool, error) {
	return "attempt", m.quiescing, m.err
}

func (m *memorySafety) CheckPoolDeleteAfter(_ context.Context, _ string, _ types.UID, after time.Time) (uninstallcheck.Report, error) {
	m.after = after
	if m.called != nil {
		m.called <- struct{}{}
	}
	return m.report, m.err
}

func TestReconcileProtectsActivePool(t *testing.T) {
	pools := &memoryPools{pools: []volumeapi.Pool{{Name: "pool", UID: "pool-uid"}}}
	reconciler := &Reconciler{Pools: pools, Safety: &memorySafety{}, Quiesce: memoryQuiesce{}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.ensured) != 1 || pools.ensured[0] != "pool/pool-uid" || len(pools.removed) != 0 {
		t.Fatalf("ensure=%v remove=%v", pools.ensured, pools.removed)
	}
}

func TestReconcileProtectsDuplicateRegistrationsIndependently(t *testing.T) {
	pools := &memoryPools{pools: []volumeapi.Pool{
		{Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a"},
		{Name: "pool-duplicate", UID: "pool-duplicate-uid", NodeName: "node-a"},
	}}
	reconciler := &Reconciler{Pools: pools, Safety: &memorySafety{}, Quiesce: memoryQuiesce{}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.ensured) != 2 || pools.ensured[0] != "pool-a/pool-a-uid" || pools.ensured[1] != "pool-duplicate/pool-duplicate-uid" {
		t.Fatalf("independent finalizers=%v", pools.ensured)
	}
}

func TestReconcileDoesNotReinstallProtectionDuringUninstallQuiesce(t *testing.T) {
	pools := &memoryPools{pools: []volumeapi.Pool{{Name: "pool", UID: "pool-uid"}}}
	reconciler := &Reconciler{Pools: pools, Safety: &memorySafety{}, Quiesce: memoryQuiesce{quiescing: true}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.ensured) != 0 {
		t.Fatalf("protection reinstalled during quiesce: %v", pools.ensured)
	}
}

func TestReconcileWaitsForPostDeleteSafety(t *testing.T) {
	deletedAt := metav1.NewTime(time.Date(2026, time.September, 12, 8, 0, 0, 0, time.UTC))
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", DeletionTimestamp: &deletedAt, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	pools := &memoryPools{pools: []volumeapi.Pool{pool}}
	safety := &memorySafety{report: uninstallcheck.Report{Blockers: []uninstallcheck.Blocker{{Kind: uninstallcheck.PoolInventoryBlockerKind, Name: pool.Name}}}}
	reconciler := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}

	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !safety.after.Equal(deletedAt.Time) || len(pools.removed) != 0 {
		t.Fatalf("after=%s remove=%v", safety.after, pools.removed)
	}

	safety.report = uninstallcheck.Report{}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.approved) != 1 || pools.approved[0] != "pool/pool-uid" || len(pools.removed) != 0 {
		t.Fatalf("approve=%v remove=%v", pools.approved, pools.removed)
	}
	pools.pools[0].IdentityReleaseApproval = pool.UID
	pools.pools[0].Status.Conditions = []metav1.Condition{{
		Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionTrue, ObservedGeneration: pool.Generation, Reason: "PoolIdentityReleased",
	}}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pools.removed) != 1 || pools.removed[0] != "pool/pool-uid" {
		t.Fatalf("remove=%v", pools.removed)
	}
}

func TestReconcileFailsClosedOnObservationError(t *testing.T) {
	deletedAt := metav1.Now()
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", DeletionTimestamp: &deletedAt, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	pools := &memoryPools{pools: []volumeapi.Pool{pool}}
	safety := &memorySafety{err: errors.New("API unavailable")}
	reconciler := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: &poolcapacity.Locker{}, Interval: time.Second}
	if err := reconciler.ReconcileAll(context.Background()); err == nil || len(pools.removed) != 0 {
		t.Fatalf("error=%v remove=%v", err, pools.removed)
	}
}

func TestReconcileSerializesDeletionApprovalWithPoolAdmission(t *testing.T) {
	deletedAt := metav1.Now()
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node-a", DeletionTimestamp: &deletedAt, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	pools := &memoryPools{pools: []volumeapi.Pool{pool}}
	safety := &memorySafety{called: make(chan struct{}, 1)}
	locks := &poolcapacity.Locker{}
	releaseAdmission := sync.OnceFunc(locks.Lock(pool.UID))
	defer releaseAdmission()
	reconciler := &Reconciler{Pools: pools, Safety: safety, Quiesce: memoryQuiesce{}, PoolLocks: locks, Interval: time.Second}
	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	select {
	case <-safety.called:
		t.Fatal("Pool deletion inspected dependencies before in-flight admission released its fence")
	case <-time.After(50 * time.Millisecond):
	}
	releaseAdmission()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pool deletion did not resume after admission released its fence")
	}
	if len(pools.approved) != 1 {
		t.Fatalf("identity release approval=%v", pools.approved)
	}
}

func TestReconcileDeletionDoesNotWaitForOtherPoolAdmissionOnSameNode(t *testing.T) {
	deletedAt := metav1.Now()
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node-a", DeletionTimestamp: &deletedAt, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	otherPool := volumeapi.Pool{Name: "other-pool", UID: "other-pool-uid", NodeName: pool.NodeName, Finalizers: []string{volumeapi.PoolProtectionFinalizer}}
	pools := &memoryPools{pools: []volumeapi.Pool{otherPool, pool}}
	locks := &poolcapacity.Locker{}
	releaseAdmission := locks.Lock(otherPool.UID)
	defer releaseAdmission()
	reconciler := &Reconciler{Pools: pools, Safety: &memorySafety{}, Quiesce: memoryQuiesce{}, PoolLocks: locks, Interval: time.Second}
	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pool deletion waited for admission to another Pool on the same node")
	}
	if len(pools.approved) != 1 || pools.approved[0] != pool.Name+"/"+pool.UID {
		t.Fatalf("identity release approval=%v", pools.approved)
	}
}
