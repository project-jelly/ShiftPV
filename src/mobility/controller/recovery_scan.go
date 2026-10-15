package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
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

// Older controllers could persist a hold before its execution plan. Derive
// the expected copies only to prove absence after quiescence; never persist
// that inferred plan or use it to authorize deletion of an existing artifact.
func (r *Reconciler) settleUnstartedApproval(ctx context.Context, move *volumeapi.Move, state volumeapi.State) (bool, error) {
	if move.Status.SourceCopy == nil || state.CurrentCopy == nil || *move.Status.SourceCopy != *state.CurrentCopy {
		return false, needsRecoveryCleanupReview("recorded source identity differs from current authority")
	}
	plan := *move
	if err := r.prepareMoveCopyIdentities(ctx, &plan); err != nil {
		return false, err
	}
	if err := validRollbackIntent(plan); err != nil {
		return false, err
	}
	if r.Cleanups == nil {
		return false, fmt.Errorf("recovery cleanup store is not configured")
	}
	if _, err := r.Cleanups.Get(ctx, moveCleanupAuthority(*move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
		if err != nil {
			return false, err
		}
		return false, needsRecoveryCleanupReview("unstarted approval carries a cleanup journal")
	}
	pools, err := r.Repository.Pools(ctx)
	if err != nil {
		return false, err
	}
	destination, err := rollbackDestinationPool(plan, pools)
	if err != nil {
		return false, err
	}
	if move.Status.RollbackRequiredGeneration <= 0 {
		return r.openRollbackScan(ctx, move, *plan.Status.IncomingCopy)
	}
	if err := r.rollbackInventoryFresh(destination, move.Status.RollbackRequiredGeneration); err != nil {
		return false, err
	}
	_, present, err := rollbackPresentArtifact(plan, destination)
	if err != nil {
		return false, err
	}
	if present {
		return false, needsRecoveryCleanupReview("unstarted approval has a destination artifact without durable execution intent")
	}
	return true, nil
}
