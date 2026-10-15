package controller

import (
	"context"
	"slices"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// The handoff occurs between the two LIST responses. A reader holding the
// source Pool lock may overlap this outgoing move: only the destination is
// locked by approval and owner commit.
type handoffSnapshotRegistry struct {
	capacityQueryRegistry
	reads                     []string
	beforeVolume, afterVolume volumeapi.State
	beforeMove, afterMove     volumeapi.Move
}

func (r *handoffSnapshotRegistry) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	state := r.beforeVolume
	if len(r.reads) != 0 {
		state = r.afterVolume
	}
	r.reads = append(r.reads, "volumes")
	return map[string]volumeapi.State{r.beforeMove.Spec.VolumeID: state}, nil
}

func (r *handoffSnapshotRegistry) ListMoves(context.Context) ([]volumeapi.Move, error) {
	move := r.beforeMove
	if len(r.reads) != 0 {
		move = r.afterMove
	}
	r.reads = append(r.reads, "moves")
	return []volumeapi.Move{move}, nil
}

func TestAuthorizingCapacityReadsVolumesBeforeMovesAcrossOutgoingHandoff(t *testing.T) {
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source", PoolUID: "source-uid", NodeName: "node-a",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing}
	destination := source
	destination.PoolName, destination.PoolUID, destination.NodeName, destination.CopyID = "destination", "destination-uid", "node-b", "destination-copy"
	beforeVolume := volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: source.NodeName,
		CurrentCopy: &source, CapacityBytes: 64, ActiveMove: "move"}
	afterVolume := beforeVolume
	afterVolume.Phase, afterVolume.OwnerNode, afterVolume.CurrentCopy = volumeapi.PhaseReady, destination.NodeName, &destination
	beforeMove := volumeapi.Move{Name: "move", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: source.VolumeID, SourceNode: source.NodeName},
		Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity", SourceCopy: &source}}
	afterMove := beforeMove
	afterMove.Status.Phase, afterMove.Status.CapacityApproved = "CleaningSource", true
	afterMove.Status.DestinationNode, afterMove.Status.DestinationPoolUID = destination.NodeName, destination.PoolUID
	newRegistry := func() *handoffSnapshotRegistry {
		return &handoffSnapshotRegistry{beforeVolume: beforeVolume, afterVolume: afterVolume, beforeMove: beforeMove, afterMove: afterMove}
	}

	registry := newRegistry()
	service := &Service{CapacityPools: registry}
	reserved, err := service.poolReservedBytes(context.Background(), source.PoolUID, "new-volume")
	if err != nil || reserved != 64 || !slices.Equal(registry.reads, []string{"volumes", "moves"}) {
		t.Fatalf("source reservation lost across handoff: reserved=%d reads=%v err=%v", reserved, registry.reads, err)
	}

	// Reversing the reads would combine an unapproved Move with the new owner:
	// neither record then accounts for the still-retained source copy. This
	// fixture demonstrates why preserving the production read order is required.
	reversed := newRegistry()
	moves, _ := reversed.ListMoves(context.Background())
	volumes, _ := reversed.ListVolumes(context.Background())
	missing, err := poolcapacity.ReservedBytesForPool(volumes, moves, source.PoolUID)
	if err != nil || missing != 0 {
		t.Fatalf("fixture no longer demonstrates the reverse-order undercount: reserved=%d err=%v", missing, err)
	}
}
