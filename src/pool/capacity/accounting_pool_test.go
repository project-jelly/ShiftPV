package capacity

import (
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestReservedBytesForPoolSeparatesTwoPoolsOnOneNode(t *testing.T) {
	copyA := &volume.CopyIdentity{PoolUID: "pool-a", NodeName: "node-a"}
	copyB := &volume.CopyIdentity{PoolUID: "pool-b", NodeName: "node-a"}
	volumes := map[string]volumeapi.State{
		"a": {OwnerNode: "node-a", CurrentCopy: copyA, CapacityBytes: 64},
		"b": {OwnerNode: "node-a", CurrentCopy: copyB, CapacityBytes: 128},
	}
	for poolUID, want := range map[string]int64{"pool-a": 64, "pool-b": 128} {
		got, err := ReservedBytesForPool(volumes, nil, poolUID)
		if err != nil || got != want {
			t.Fatalf("%s reserved=%d want=%d err=%v", poolUID, got, want, err)
		}
	}
}

func TestReservedBytesForPoolTransfersMoveHoldAcrossOwnerCommit(t *testing.T) {
	source := &volume.CopyIdentity{PoolUID: "pool-a", NodeName: "node-a"}
	destination := &volume.CopyIdentity{PoolUID: "pool-b", NodeName: "node-b"}
	state := volumeapi.State{OwnerNode: "node-a", CurrentCopy: source, CapacityBytes: 64, ActiveMove: "move"}
	move := volumeapi.Move{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "v"}, Status: volumeapi.MoveStatus{
		SourceCopy: source, DestinationPoolUID: destination.PoolUID, CapacityApproved: true,
	}}
	assert := func(wantA, wantB int64) {
		t.Helper()
		for poolUID, want := range map[string]int64{"pool-a": wantA, "pool-b": wantB} {
			got, err := ReservedBytesForPool(map[string]volumeapi.State{"v": state}, []volumeapi.Move{move}, poolUID)
			if err != nil || got != want {
				t.Fatalf("%s reserved=%d want=%d err=%v", poolUID, got, want, err)
			}
		}
	}
	assert(64, 64)
	state.OwnerNode, state.CurrentCopy = "node-b", destination
	assert(64, 64)
	move.Status.Phase, move.Status.CleanupPhase = "Succeeded", "Completed"
	state.ActiveMove = ""
	assert(0, 64)
}

func TestReservedBytesForPoolKeepsBothHoldsForSameNodeCrossPoolMove(t *testing.T) {
	source := &volume.CopyIdentity{PoolUID: "pool-a", NodeName: "node-a"}
	destination := &volume.CopyIdentity{PoolUID: "pool-b", NodeName: "node-a"}
	state := volumeapi.State{OwnerNode: "node-a", CurrentCopy: source, CapacityBytes: 64, ActiveMove: "move"}
	move := volumeapi.Move{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "v"}, Status: volumeapi.MoveStatus{
		SourceCopy: source, DestinationPoolUID: destination.PoolUID, CapacityApproved: true,
	}}
	for _, current := range []*volume.CopyIdentity{source, destination} {
		state.CurrentCopy = current
		for _, poolUID := range []string{"pool-a", "pool-b"} {
			got, err := ReservedBytesForPool(map[string]volumeapi.State{"v": state}, []volumeapi.Move{move}, poolUID)
			if err != nil || got != 64 {
				t.Fatalf("owner=%s pool=%s reserved=%d err=%v", current.PoolUID, poolUID, got, err)
			}
		}
	}
}

func TestReservedBytesForPoolRejectsUnidentifiedHold(t *testing.T) {
	state := volumeapi.State{OwnerNode: "node-a", CurrentCopy: &volume.CopyIdentity{PoolUID: "pool-a", NodeName: "node-a"},
		CapacityBytes: 64, ActiveMove: "move"}
	move := volumeapi.Move{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "v"}, Status: volumeapi.MoveStatus{CapacityApproved: true}}
	if _, err := ReservedBytesForPool(map[string]volumeapi.State{"v": state}, []volumeapi.Move{move}, "pool-a"); err == nil ||
		!strings.Contains(err.Error(), "incomplete capacity hold identity") {
		t.Fatalf("unidentified Move hold was accepted: %v", err)
	}
}
