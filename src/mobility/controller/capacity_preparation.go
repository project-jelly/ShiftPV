package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// The logical hold already consumes requested bytes. Conversion replaces it
// with execution approval in one status CAS; it never adds the bytes again.
func (r *Reconciler) convertPreparation(ctx context.Context, move *volumeapi.Move) error {
	if err := volumeapi.ValidateMovePreparation(*move); err != nil {
		return err
	}
	if err := r.measureSourceCopy(ctx, move); err != nil {
		return err
	}
	hold := move.Status.Preparation
	unlock := r.PoolLocks.Lock(hold.Pool.UID)
	defer unlock()
	pool, err := r.Repository.ReadyPoolForIdentity(ctx, hold.Pool.Name, hold.Pool.UID, hold.NodeName)
	if err != nil {
		return err
	}
	source := move.Status.SourceCopy
	sourcePool, err := r.Repository.PoolForIdentity(ctx, source.PoolName, source.PoolUID, source.NodeName)
	if err != nil {
		return err
	}
	if pool.PoolGroup != sourcePool.PoolGroup || volumeapi.PoolHasServingVolume(pool, move.Spec.VolumeID) {
		return fmt.Errorf("prepared destination group or serving inventory changed")
	}
	_, _, physicalPending, _, err := r.destinationCapacityForPool(ctx, *move, pool)
	if err != nil {
		return err
	}
	stats, err := r.CapacityProbe.StatFSForPool(ctx, pool)
	if err != nil {
		return fmt.Errorf("inspect prepared destination filesystem: %w", err)
	}
	previous := move.Status
	if physicalPending > stats.AvailableBytes || move.Status.SourceBytes > stats.AvailableBytes-physicalPending {
		move.Status.CapacityReason = "DestinationFilesystemSpace"
		return r.persistMoveStatus(ctx, move, previous)
	}
	// A limit edit cannot revoke already reserved logical bytes. Fresh physical
	// space is still required because a logical reservation is not a disk quota.
	planned := *move
	if err := prepareMoveCopyIdentitiesForPool(&planned, pool); err != nil {
		return err
	}
	planned.Status.Preparation = nil
	planned.Status.CapacityApproved = true
	planned.Status.CapacityReason = ""
	*move = planned
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		return errors.Join(errCapacityApprovalPersistence, err)
	}
	return nil
}

// Called with the destination Pool lock held and Volumes then Moves freshly
// listed. Never subtract a hold merely because a stale caller names it.
func preparedCapacitySnapshot(expected volumeapi.Move, state volumeapi.State, moves []volumeapi.Move, pool volumeapi.Pool) error {
	hold := expected.Status.Preparation
	if hold == nil || hold.Pool.Name != pool.Name || hold.Pool.UID != pool.UID || hold.NodeName != pool.NodeName ||
		state.CapacityBytes != hold.ReservedBytes || state.Phase != volumeapi.PhaseMoving ||
		state.ActiveMove != expected.Name || len(state.PublishedNodes) != 0 {
		return fmt.Errorf("prepared execution no longer has its exact quiesced reservation")
	}
	if err := cancellationSource(expected, state); err != nil {
		return err
	}
	for _, current := range moves {
		if current.Name == expected.Name {
			if current.UID == expected.UID && current.ResourceVersion == expected.ResourceVersion && current.Spec == expected.Spec &&
				!current.Deleting && reflect.DeepEqual(current.Status.Preparation, hold) {
				return nil
			}
			return fmt.Errorf("%w: prepared execution Move changed", volumeapi.ErrStateConflict)
		}
	}
	return fmt.Errorf("prepared execution Move disappeared")
}
