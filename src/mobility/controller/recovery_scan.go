package controller

import (
	"context"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// ensureRollbackScan opens a causal fence only after Retiring has stopped the
// transaction's writers. A separate read-back pass must observe the persisted
// fence before inventory can authorize cleanup or release the capacity hold.
func (r *Reconciler) ensureRollbackScan(ctx context.Context, move *volumeapi.Move) (bool, error) {
	if err := validRollbackIntent(*move); err != nil {
		return false, needsRecoveryCleanupReview("destination cleanup intent is incomplete: %v", err)
	}
	if move.Status.RollbackRequiredGeneration > 0 {
		return true, nil
	}
	pools, err := r.Repository.Pools(ctx)
	if err != nil {
		return false, err
	}
	if _, err := rollbackDestinationPool(*move, pools); err != nil {
		return false, err
	}
	return r.openRollbackScan(ctx, move, *move.Status.IncomingCopy)
}

func (r *Reconciler) openRollbackScan(ctx context.Context, move *volumeapi.Move, target volume.CopyIdentity) (bool, error) {
	generation, err := r.Repository.RequestPoolScan(ctx, target)
	if err != nil {
		return false, err
	}
	if generation <= 0 {
		return false, fmt.Errorf("rollback scan request returned no generation")
	}
	previous := move.Status
	move.Status.RollbackRequiredGeneration = generation
	return false, r.persistMoveStatus(ctx, move, previous)
}
