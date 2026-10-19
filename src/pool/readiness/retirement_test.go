package readiness

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type retirementInspector struct{ calls int }

func (i *retirementInspector) Inspect(volumeapi.Pool) Result {
	i.calls++
	return Result{}
}
func (i *retirementInspector) InspectCapacityUnit(volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check) {
	i.calls++
	return nil, Check{}
}

func TestRetiredRegistrationDoesNotTouchStorageOrInventAbsence(t *testing.T) {
	oldProbe := metav1.NewTime(testTime.Add(-time.Hour))
	pool := testRegistration(volumeapi.Pool{Name: "lost", UID: "lost-uid", NodeName: "node", Generation: 1,
		RetirementApproval: "lost-uid", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock,
		Status: volumeapi.PoolStatus{RegistrationApproved: true, LastProbeTime: oldProbe}})
	repo, inspector := &fakeRepository{pool: pool}, &retirementInspector{}
	r := &Reconciler{NodeName: "node", Pools: repo, Inspector: inspector, Interval: time.Second, Now: func() time.Time { return testTime },
		Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
			t.Fatal("retirement scanned storage")
			return volumeapi.PoolInventory{}
		},
		Release: func(context.Context, volumeapi.Pool) error {
			t.Fatal("retirement released physical identity")
			return nil
		}}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(repo.status.Conditions, volumeapi.PoolConditionReady)
	if inspector.calls != 0 || ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PoolRegistrationRetired" ||
		repo.status.Inventory == nil || repo.status.Inventory.Valid || !repo.status.LastProbeTime.Equal(&oldProbe) {
		t.Fatalf("retirement invented a storage observation: calls=%d status=%+v", inspector.calls, repo.status)
	}
}

func TestRetiredPoolDeletionStillRequiresNormalNodeRelease(t *testing.T) {
	deleted := metav1.NewTime(testTime.Add(-time.Minute))
	pool := testRegistration(volumeapi.Pool{Name: "lost", UID: "lost-uid", NodeName: "node", Generation: 1,
		RetirementApproval: "lost-uid", IdentityReleaseApproval: "lost-uid", DeletionTimestamp: &deleted,
		Status: volumeapi.PoolStatus{RegistrationApproved: true}})
	repo := &fakeRepository{pool: pool}
	released := 0
	r := &Reconciler{NodeName: "node", Pools: repo, Interval: time.Second, Now: func() time.Time { return testTime },
		Inspector: fakeInspector{result: Result{Accessible: Check{Known: true, OK: true}, Writable: Check{Known: true, OK: true}, CapacityReadable: Check{Known: true, OK: true}}},
		Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
			return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(testTime), Valid: true}
		},
		Release: func(context.Context, volumeapi.Pool) error { released++; return nil }}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(repo.status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if released != 1 || condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("normal deletion did not retain its node evidence path: released=%d condition=%+v", released, condition)
	}
}

func TestTargetedSnapshotUsesCurrentRetirementMetadata(t *testing.T) {
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", Generation: 1}
	r := &Reconciler{NodeName: "node", lastObservations: map[string]Observation{pool.UID: {Pool: pool}}}
	pool.NodeName, pool.RetirementApproval = "node", pool.UID
	snapshot := r.observationSnapshot([]volumeapi.Pool{pool}, nil)
	if len(snapshot) != 1 || !snapshot[0].Pool.RegistrationRetired() {
		t.Fatal("same-generation cached observation hid retirement approval")
	}
}
