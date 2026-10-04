package readiness

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

var testTime = time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)

type fakeInspector struct{ result Result }

func (f fakeInspector) Inspect(volumeapi.Pool) Result { return f.result }

type fakeRepository struct {
	pool       volumeapi.Pool
	currentUID string
	err        error
	status     volumeapi.PoolStatus
	statusSets int
}

func (f *fakeRepository) PoolForNodeLifecycle(context.Context, string) (volumeapi.Pool, error) {
	return f.pool, f.err
}

func (f *fakeRepository) SetPoolStatus(_ context.Context, name, uid, node string, status volumeapi.PoolStatus) error {
	if name != f.pool.Name || uid != f.pool.UID || node != f.pool.NodeName || f.currentUID != "" && uid != f.currentUID {
		return errors.New("identity mismatch")
	}
	f.status = status
	f.statusSets++
	return nil
}

func TestReconcilePersistsReadyConditionsAndPreservesTransitionTime(t *testing.T) {
	old := metav1.NewTime(testTime.Add(-time.Hour))
	repository := &fakeRepository{pool: volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 3,
		Status: volumeapi.PoolStatus{Conditions: []metav1.Condition{{
			Type: volumeapi.PoolConditionAccessible, Status: metav1.ConditionTrue,
			ObservedGeneration: 2, LastTransitionTime: old, Reason: "DirectoryAccessible", Message: "old",
		}}},
	}}
	ok := Check{OK: true, Known: true, Reason: "DirectoryAccessible", Message: "old"}
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: fakeInspector{Result{Accessible: ok,
			Writable:         Check{OK: true, Known: true, Reason: "Writable", Message: "write"},
			CapacityReadable: Check{OK: true, Known: true, Reason: "CapacityReadable", Message: "capacity"}}},
		Interval: time.Minute, Now: func() time.Time { return testTime },
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.status.ObservedGeneration != 3 || !repository.status.LastProbeTime.Equal(&metav1.Time{Time: testTime}) {
		t.Fatalf("status = %#v", repository.status)
	}
	ready := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "PoolReady" {
		t.Fatalf("ready = %#v", ready)
	}
	accessible := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionAccessible)
	if accessible == nil || !accessible.LastTransitionTime.Equal(&old) || accessible.ObservedGeneration != 3 {
		t.Fatalf("accessible = %#v", accessible)
	}
}

func TestReconcileRequiredMountPointKeepsAnchorAndInvalidatesInventory(t *testing.T) {
	identity := &volumeapi.PoolMountIdentity{Device: "8:2", Root: "/", Source: "/dev/disk-a", Filesystem: "ext4"}
	repository := &fakeRepository{pool: volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1,
		MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint,
	}}
	ok := Check{OK: true, Known: true, Reason: "OK", Message: "ok"}
	inspector := fakeInspector{Result{Accessible: ok, Mounted: ok, Writable: ok, CapacityReadable: ok, MountIdentity: identity}}
	scans := 0
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: inspector,
		Interval: time.Minute, Now: func() time.Time { return testTime },
		Inventory: func(_ context.Context, pool volumeapi.Pool, _ time.Time) volumeapi.PoolInventory {
			if pool.Status.MountIdentity == nil || *pool.Status.MountIdentity != *identity {
				t.Fatalf("first inventory did not receive verified mount identity: %#v", pool.Status.MountIdentity)
			}
			scans++
			return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(testTime), Message: "InventoryInvalid"}
		},
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.status.MountIdentity != nil || scans != 1 {
		t.Fatalf("invalid inventory anchored mount: status=%#v scans=%d", repository.status, scans)
	}
	reconciler.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		scans++
		return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(testTime), Valid: true}
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.status.MountIdentity == nil || *repository.status.MountIdentity != *identity || scans != 2 {
		t.Fatalf("initial status = %#v, scans=%d", repository.status, scans)
	}
	repository.pool.Status = repository.status
	reconciler.Inspector = fakeInspector{Result{Accessible: ok, Mounted: Check{Known: true, Reason: "MountMissing", Message: "unmounted"}}}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "MountMissing" ||
		repository.status.MountIdentity == nil || *repository.status.MountIdentity != *identity ||
		repository.status.Inventory == nil || repository.status.Inventory.Valid || scans != 2 {
		t.Fatalf("unmounted status = %#v, scans=%d", repository.status, scans)
	}
}

func TestReconcileRecordsFailureAndAllowsMissingRegistration(t *testing.T) {
	repository := &fakeRepository{pool: volumeapi.Pool{Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1}}
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: fakeInspector{Result{
			Accessible:       Check{OK: true, Known: true, Reason: "DirectoryAccessible", Message: "accessible"},
			Writable:         Check{Known: true, Reason: "PermissionDenied", Message: "denied"},
			CapacityReadable: Check{OK: true, Known: true, Reason: "CapacityReadable", Message: "capacity"},
		}}, Interval: time.Minute, Now: func() time.Time { return testTime },
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PermissionDenied" {
		t.Fatalf("ready = %#v", ready)
	}
	repository.err = volumeapi.ErrPoolNotFound
	if err := reconciler.Reconcile(context.Background()); err != nil || repository.statusSets != 1 {
		t.Fatalf("missing Pool: sets=%d err=%v", repository.statusSets, err)
	}
}

func TestReconcileKeepsInventoryFreshWhilePoolDeregisters(t *testing.T) {
	deletedAt := metav1.NewTime(testTime.Add(-time.Minute))
	repository := &fakeRepository{pool: volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1, DeletionTimestamp: &deletedAt, IdentityReleaseApproval: "pool-a-uid",
	}}
	ok := Check{OK: true, Known: true, Reason: "OK", Message: "ok"}
	releases := 0
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: fakeInspector{Result{Accessible: ok, Writable: ok, CapacityReadable: ok}},
		Interval: time.Minute, Now: func() time.Time { return testTime },
		Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
			return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(testTime), Valid: true}
		},
		Release: func(_ context.Context, pool volumeapi.Pool) error {
			releases++
			if pool.UID != "pool-a-uid" {
				t.Fatalf("released Pool = %#v", pool)
			}
			return nil
		},
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PoolDeregistering" {
		t.Fatalf("ready=%#v", ready)
	}
	if repository.status.Inventory == nil || !repository.status.Inventory.ObservedAt.Equal(&metav1.Time{Time: testTime}) {
		t.Fatalf("inventory=%#v", repository.status.Inventory)
	}
	released := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if releases != 1 || released == nil || released.Status != metav1.ConditionTrue || released.Reason != "PoolIdentityReleased" {
		t.Fatalf("releases=%d condition=%#v", releases, released)
	}
}

func TestReconcilePreservesPoolIdentityUntilInventoryIsEmpty(t *testing.T) {
	deletedAt := metav1.NewTime(testTime.Add(-time.Minute))
	repository := &fakeRepository{pool: volumeapi.Pool{
		Name: "pool-a", UID: "pool-a-uid", NodeName: "node-a", Generation: 1, DeletionTimestamp: &deletedAt, IdentityReleaseApproval: "pool-a-uid",
	}}
	ok := Check{OK: true, Known: true, Reason: "OK", Message: "ok"}
	releases := 0
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: fakeInspector{Result{Accessible: ok, Writable: ok, CapacityReadable: ok}},
		Interval: time.Minute, Now: func() time.Time { return testTime },
		Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
			return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(testTime), Valid: true, Copies: []volumeapi.CopyObservation{{Marker: "copy"}}}
		},
		Release: func(context.Context, volumeapi.Pool) error { releases++; return nil },
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	released := meta.FindStatusCondition(repository.status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if releases != 0 || released == nil || released.Status != metav1.ConditionFalse || released.Reason != "PoolIdentityRetained" {
		t.Fatalf("releases=%d condition=%#v", releases, released)
	}
}

func TestReconcileRejectsReplacementPoolBeforeStatusWrite(t *testing.T) {
	repository := &fakeRepository{
		pool:       volumeapi.Pool{Name: "pool-a", UID: "observed-uid", NodeName: "node-a", Generation: 1},
		currentUID: "observed-uid",
	}
	reconciler := &Reconciler{
		NodeName: "node-a", Pools: repository, Inspector: fakeInspector{}, Interval: time.Minute,
		Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
			repository.currentUID = "replacement-uid"
			return volumeapi.PoolInventory{Valid: true}
		},
	}
	if err := reconciler.Reconcile(context.Background()); err == nil || err.Error() != "identity mismatch" {
		t.Fatalf("replacement Pool status write error = %v", err)
	}
	if repository.statusSets != 0 {
		t.Fatalf("replacement Pool received %d stale status writes", repository.statusSets)
	}
}

func TestReconcilerValidatesConfiguration(t *testing.T) {
	if err := (&Reconciler{}).Reconcile(context.Background()); err == nil {
		t.Fatal("invalid configuration accepted")
	}
}

func TestReconcilerObserverReceivesExistingProbeAndErrors(t *testing.T) {
	repository := &fakeRepository{pool: volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node"}}
	result := Result{CapacityReadable: Check{OK: true, Known: true}}
	calls := 0
	var observedErr error
	reconciler := &Reconciler{
		NodeName: "node", Pools: repository, Inspector: fakeInspector{result}, Interval: time.Minute,
		Observe: func(pool volumeapi.Pool, got Result, err error) {
			calls++
			observedErr = err
			if err == nil && pool.Name != "" && got != result {
				t.Fatalf("observer did not reuse probe: %+v", got)
			}
		},
	}
	if err := reconciler.Reconcile(context.Background()); err != nil || calls != 1 || repository.statusSets != 1 {
		t.Fatalf("normal observation: calls=%d sets=%d err=%v", calls, repository.statusSets, err)
	}
	repository.pool = volumeapi.Pool{}
	repository.err = volumeapi.ErrPoolNotFound
	if err := reconciler.Reconcile(context.Background()); err != nil || calls != 2 || observedErr != nil {
		t.Fatalf("deleted Pool observation: %v", err)
	}
	repository.err = context.DeadlineExceeded
	if err := reconciler.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) || calls != 3 || !errors.Is(observedErr, err) {
		t.Fatalf("API error observation: %v", err)
	}
}
