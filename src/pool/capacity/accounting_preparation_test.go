package capacity

import (
	"math"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func preparationFixture() (volumeapi.State, volumeapi.Move) {
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source", PoolUID: "source-uid", NodeName: "node-a",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing,
	}
	current := source
	state := volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: source.NodeName,
		CurrentCopy: &current, CapacityBytes: 64, ActiveMove: "move"}
	move := volumeapi.Move{Name: "move", UID: "move-uid",
		Spec: volumeapi.MoveSpec{ProtocolVersion: volumeapi.MoveProtocolPreparedV1, VolumeID: source.VolumeID, SourceNode: source.NodeName},
		Status: volumeapi.MoveStatus{
			Phase: "Pending", SourceCopy: &source,
			PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid", ClaimNamespace: "app", ClaimName: "claim", ClaimUID: "claim-uid",
			ConsumerName: "consumer", ConsumerUID: "consumer-uid", PlacementPodUID: "placement-uid",
			Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: "destination", UID: "destination-uid"}, NodeName: "node-b", ReservedBytes: 64},
		}}
	return state, move
}

func TestPreparationReservesExactPoolBeforeAndAfterQuiesce(t *testing.T) {
	for _, phase := range []string{volumeapi.PhaseReady, volumeapi.PhaseMoving} {
		for _, node := range []string{"node-a", "node-b"} {
			t.Run(phase+"/"+node, func(t *testing.T) {
				state, move := preparationFixture()
				state.Phase, move.Status.Preparation.NodeName = phase, node
				for pool, want := range map[string]int64{"source-uid": 64, "destination-uid": 64, "other-pool-on-same-node": 0} {
					got, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state}, []volumeapi.Move{move}, pool)
					if err != nil || got != want {
						t.Fatalf("pool=%s got=%d want=%d err=%v", pool, got, want, err)
					}
				}
			})
		}
	}
}

func TestPreparationHoldSurvivesTerminalDiagnostic(t *testing.T) {
	for _, phase := range []string{"Blocked", "Failed", "Succeeded"} {
		t.Run(phase, func(t *testing.T) {
			state, move := preparationFixture()
			move.Status.Phase, move.Status.RecoveryPhase, move.Status.CapacityReason = phase, "Recovered", "RecoverySettled"
			got, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state}, []volumeapi.Move{move}, "destination-uid")
			if err != nil || got != 64 {
				t.Fatalf("terminal phase dropped preparation: got=%d err=%v", got, err)
			}
		})
	}
}

func TestPreparationConvertsToExecutionReservationExactlyOnce(t *testing.T) {
	state, move := preparationFixture()
	assert := func(source, destination int64) {
		t.Helper()
		for pool, want := range map[string]int64{"source-uid": source, "destination-uid": destination} {
			got, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state}, []volumeapi.Move{move}, pool)
			if err != nil || got != want {
				t.Fatalf("pool=%s got=%d want=%d err=%v", pool, got, want, err)
			}
		}
	}
	assert(64, 64)
	move.Status.Preparation = nil
	move.Status.CapacityApproved = true
	move.Status.DestinationNode, move.Status.DestinationPoolUID = "node-b", "destination-uid"
	assert(64, 64)
	destination := *state.CurrentCopy
	destination.PoolName, destination.PoolUID, destination.NodeName, destination.CopyID = "destination", "destination-uid", "node-b", "destination-copy"
	state.OwnerNode, state.CurrentCopy = destination.NodeName, &destination
	assert(64, 64)
	move.Status.Phase, move.Status.CleanupPhase, state.ActiveMove = "Succeeded", "Completed", ""
	assert(0, 64)
}

func TestPreparationRejectsContradictoryOrStaleReservation(t *testing.T) {
	for name, mutate := range map[string]func(*volumeapi.State, *volumeapi.Move){
		"missing active move":    func(s *volumeapi.State, _ *volumeapi.Move) { s.ActiveMove = "" },
		"other active move":      func(s *volumeapi.State, _ *volumeapi.Move) { s.ActiveMove = "new-move" },
		"recreated volume":       func(s *volumeapi.State, _ *volumeapi.Move) { s.UID = "new-volume-uid" },
		"different current copy": func(s *volumeapi.State, _ *volumeapi.Move) { s.CurrentCopy.CopyID = "new-copy" },
		"missing current copy":   func(s *volumeapi.State, _ *volumeapi.Move) { s.CurrentCopy = nil },
		"wrong owner":            func(s *volumeapi.State, _ *volumeapi.Move) { s.OwnerNode = "other-node" },
		"different size":         func(s *volumeapi.State, _ *volumeapi.Move) { s.CapacityBytes++ },
		"deletion in progress":   func(s *volumeapi.State, _ *volumeapi.Move) { s.Deleting = true },
		"deleting phase":         func(s *volumeapi.State, _ *volumeapi.Move) { s.Phase = volumeapi.PhaseDeleting },
		"missing source copy":    func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.SourceCopy = nil },
		"source node mismatch":   func(_ *volumeapi.State, m *volumeapi.Move) { m.Spec.SourceNode = "other-node" },
		"source volume mismatch": func(_ *volumeapi.State, m *volumeapi.Move) {
			m.Status.SourceCopy.VolumeID = "shiftpv-fedcba9876543210fedcba9876543210"
		},
		"legacy protocol":     func(_ *volumeapi.State, m *volumeapi.Move) { m.Spec.ProtocolVersion = "" },
		"unknown protocol":    func(_ *volumeapi.State, m *volumeapi.Move) { m.Spec.ProtocolVersion = "prepared-v2" },
		"empty move uid":      func(_ *volumeapi.State, m *volumeapi.Move) { m.UID = "" },
		"missing target uid":  func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.Preparation.Pool.UID = "" },
		"missing target name": func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.Preparation.Pool.Name = "" },
		"missing target node": func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.Preparation.NodeName = "" },
		"same source pool": func(_ *volumeapi.State, m *volumeapi.Move) {
			m.Status.Preparation.Pool.UID = m.Status.SourceCopy.PoolUID
		},
		"zero reservation":      func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.Preparation.ReservedBytes = 0 },
		"negative reservation":  func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.Preparation.ReservedBytes = -1 },
		"already approved":      func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.CapacityApproved = true },
		"disk plan exists":      func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.CopyOperationID = "copy-op" },
		"different destination": func(_ *volumeapi.State, m *volumeapi.Move) { m.Status.DestinationPoolUID = "other-pool" },
		"false completed proof": func(_ *volumeapi.State, m *volumeapi.Move) {
			m.Status.Phase, m.Status.CleanupPhase = "Succeeded", "Completed"
		},
	} {
		t.Run(name, func(t *testing.T) {
			state, move := preparationFixture()
			mutate(&state, &move)
			for _, pool := range []string{"source-uid", "destination-uid", "unrelated-pool"} {
				if _, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state}, []volumeapi.Move{move}, pool); err == nil {
					t.Fatalf("inconsistent reservation advertised free capacity for %s", pool)
				}
			}
		})
	}
}

func TestPreparationMissingVolumeAndOverflowFailClosed(t *testing.T) {
	state, move := preparationFixture()
	if _, err := ReservedBytesForPool(nil, []volumeapi.Move{move}, "destination-uid"); err == nil || !strings.Contains(err.Error(), "no volume state") {
		t.Fatalf("missing volume was accepted: %v", err)
	}
	other := volumeapi.State{OwnerNode: "node-b", CapacityBytes: math.MaxInt64,
		CurrentCopy: &volume.CopyIdentity{NodeName: "node-b", PoolUID: "destination-uid"}}
	if _, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state, "other": other}, []volumeapi.Move{move}, "destination-uid"); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("overflowing reservation was accepted: %v", err)
	}
}

func TestPreparationMustBeRemovedBeforeClearingActiveMove(t *testing.T) {
	state, move := preparationFixture()
	move.Status.Preparation = nil
	for _, active := range []string{move.Name, ""} {
		state.ActiveMove = active
		got, err := ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: state}, []volumeapi.Move{move}, "destination-uid")
		if err != nil || got != 0 {
			t.Fatalf("removed preparation kept a reservation: activeMove=%q got=%d err=%v", active, got, err)
		}
	}
}
