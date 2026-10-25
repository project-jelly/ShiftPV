package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

func testBackingConditions(generation int64) []metav1.Condition {
	var conditions []metav1.Condition
	for _, kind := range []string{volumeapi.PoolConditionReady, volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
		conditions = append(conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: generation, Reason: "Verified"})
	}
	return conditions
}

// Only the API observation is injected. The real observer must distinguish
// new candidates from exact source/destination copies and publication proof.
func TestMoveObservationSeparatesExistingBackingFromAllocation(t *testing.T) {
	w := newMobilityWalk()
	now := time.Now().UTC()
	w.reconciler.Now = func() time.Time { return now }
	pools, err := w.repository.ReadyPools(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range pools {
		pool := &pools[i]
		pool.Generation = 1
		pool.CapacityPolicy = volumeapi.PoolCapacityPolicyFixedBlock
		pool.Status.ObservedGeneration = 1
		pool.Status.RegistrationApproved = true
		pool.Status.LastProbeTime = metav1.NewTime(now)
		pool.Status.CapacityUnit = &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Sectors: 4096}}}
		pool.Status.Conditions = testBackingConditions(1)
		pool.Status.Conditions[0].Status = metav1.ConditionFalse
		pool.Status.Conditions[0].Reason = "CapacityPeerUnproven"
		pool.Status.Conditions = append(pool.Status.Conditions,
			metav1.Condition{Type: volumeapi.PoolConditionCapacityBackingVerified, Status: metav1.ConditionTrue, ObservedGeneration: 1},
			metav1.Condition{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: "CapacityPeerUnproven"})
		pool.Status.Inventory.ObservedAt = metav1.NewTime(now)
	}
	w.repository.pools = pools
	w.repository.readyPoolsConfigured = true
	w.repository.readyPools = nil
	index, err := w.reconciler.observePools(w.ctx)
	if err != nil || len(index.ready) != 0 || len(index.existing) != 2 {
		t.Fatalf("existing backing mixed with allocation: %+v %v", index, err)
	}
	state, err := w.repository.Get(w.ctx, w.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	observed := observation{Volume: state}
	if _, err := w.reconciler.observeSource(w.ctx, w.move, index, &observed); err != nil || !observed.FSM.SourceHealthy {
		t.Fatalf("peer failure hid exact source copy: %+v %v", observed.FSM, err)
	}
	if err := w.reconciler.observeCandidates(w.ctx, w.move, index, &observed); err != nil || len(observed.CandidateNodes) != 0 {
		t.Fatalf("peer uncertainty admitted new destination: %+v %v", observed, err)
	}
	destination := pools[1]
	w.move.Status.DestinationPoolUID = destination.UID
	observed.DestinationNode = destination.NodeName
	if _, ready := destinationObservationPool(w.move, index, &observed); ready {
		t.Fatal("unapproved transaction used existing-copy permission")
	}
	w.move.Status.CapacityApproved = true
	if _, ready := destinationObservationPool(w.move, index, &observed); !ready {
		t.Fatal("approved destination was blocked by peer failure")
	}
	copy := *state.CurrentCopy
	copy.PoolName, copy.PoolUID, copy.NodeName = destination.Name, destination.UID, destination.NodeName
	w.move.Status.DestinationCopy = &copy
	observed.Volume.PublishedNodes = []string{copy.NodeName}
	destination.Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &copy, Present: true, Published: true}}
	index.existing[destination.UID] = destination
	observeDestinationPublish(w.move, index, &observed)
	if !observed.FSM.PublishedOnDestination {
		t.Fatal("fresh exact publication was lost")
	}
	for _, failure := range []string{"own backing", "stale inventory", "partial inventory", "unprotected"} {
		t.Run(failure, func(t *testing.T) {
			bad := destination
			bad.Status.Conditions = append([]metav1.Condition(nil), destination.Status.Conditions...)
			inventory := *destination.Status.Inventory
			bad.Status.Inventory = &inventory
			switch failure {
			case "own backing":
				bad.Status.Conditions[len(bad.Status.Conditions)-2].Status = metav1.ConditionFalse
			case "stale inventory":
				bad.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(-time.Hour))
			case "partial inventory":
				bad.Status.Inventory.Truncated = true
			case "unprotected":
				bad.Finalizers = nil
			}
			// ObservePools receives a real snapshot shape, without the memory
			// fixture's automatic identity/finalizer defaults.
			r := *w.reconciler
			r.Repository = &backingSnapshotRepository{Repository: w.repository, pools: []volumeapi.Pool{bad}}
			failedIndex, err := r.observePools(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			result := observed
			observeDestinationPublish(w.move, failedIndex, &result)
			if result.FSM.PublishedOnDestination {
				t.Fatal("uncertain inventory authorized source cleanup")
			}
		})
	}
}

type backingSnapshotRepository struct {
	Repository
	pools []volumeapi.Pool
}

func (r *backingSnapshotRepository) ObservePools(context.Context) (volumeapi.PoolSnapshot, error) {
	return volumeapi.PoolSnapshot{Registered: r.pools}, nil
}
