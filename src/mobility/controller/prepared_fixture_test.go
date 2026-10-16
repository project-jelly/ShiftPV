package controller

import (
	"context"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// Explicitly select the supported execution contract. This is never applied by
// the repository fake, so malformed and historical inputs stay malformed/old.
func preparedFixtureMove(move volumeapi.Move) volumeapi.Move {
	move.Spec.ProtocolVersion = volumeapi.MoveProtocolPreparedV1
	move.ResourceVersion = "1"
	move.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
	move.Status.PersistentVolumeName, move.Status.PersistentVolumeUID = "pv", "pv-uid"
	move.Status.ClaimNamespace, move.Status.ClaimName, move.Status.ClaimUID = "workload", "claim", "claim-uid"
	move.Status.ConsumerName, move.Status.ConsumerUID = "consumer", "consumer-uid"
	move.Status.PlacementPodUID = "placement-uid"
	if move.Status.SourceCopy == nil {
		source, _, _ := testCopyIdentities(move.Spec.VolumeID, move.Spec.SourceNode, "destination")
		move.Status.SourceCopy = &source
	}
	if move.Status.IncomingCopy != nil {
		move.Status.IncomingCopy.CopyID, _ = volumeapi.MoveCopyIDs(move.UID)
	}
	if move.Status.DestinationCopy != nil {
		_, move.Status.DestinationCopy.CopyID = volumeapi.MoveCopyIDs(move.UID)
	}
	return move
}

func prepareCapacityMove(repo *memoryRepository, move *volumeapi.Move, pool volumeapi.Pool) {
	if move.UID == "" {
		move.UID = move.Name + "-uid"
	}
	*move = preparedFixtureMove(*move)
	state, _ := repo.Get(context.Background(), move.Spec.VolumeID)
	move.Status.SourceCopy = state.CurrentCopy
	move.Status.Phase = "WaitingForCapacity"
	move.Status.DestinationNode, move.Status.DestinationPoolUID = pool.NodeName, pool.UID
	move.Status.Preparation = &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: pool.Name, UID: pool.UID}, NodeName: pool.NodeName, ReservedBytes: state.CapacityBytes}
	for i := range repo.moves {
		if repo.moves[i].Name == move.Name {
			repo.moves[i] = *move
		}
	}
}
