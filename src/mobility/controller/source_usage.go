package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var errSourceUsagePending = errors.New("source usage observation is pending")

func (r *Reconciler) measureSourceCopy(ctx context.Context, move *volumeapi.Move) error {
	if r.SourceUsage == nil {
		return fmt.Errorf("source usage observation is not configured")
	}
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return err
	}
	copy, err := sourceUsageCopy(*move, state)
	if err != nil {
		return err
	}
	if move.Status.SourceBytes <= 0 {
		measured, done, err := r.SourceUsage.AdvanceSourceUsage(ctx, *move, copy)
		if err != nil {
			return fmt.Errorf("measure source volume usage: %w", err)
		}
		if !done {
			return errSourceUsagePending
		}
		if measured <= 0 {
			return fmt.Errorf("source volume usage must be positive, got %d", measured)
		}
		stored, err := r.Repository.RecordSourceUsage(ctx, *move, copy, measured)
		if err != nil {
			// An accepted write may lose its response. Keep the result Pod and
			// re-observe the Move before another write or helper retirement.
			return errors.Join(errMoveStatusPersistence, err)
		}
		*move = stored
	}
	if move.Status.SourceCopy == nil {
		return fmt.Errorf("persisted source measurement has no copy identity")
	}
	done, err := r.finalizeSourceUsage(ctx, *move)
	if err != nil {
		return err
	}
	if !done {
		return errSourceUsagePending
	}
	return nil
}

func (r *Reconciler) finalizeSourceUsage(ctx context.Context, move volumeapi.Move) (bool, error) {
	if r.SourceUsage == nil {
		return false, fmt.Errorf("source usage finalization is not configured")
	}
	return r.SourceUsage.FinalizeSourceUsage(ctx, move)
}

func sourceUsageCopy(move volumeapi.Move, state volumeapi.State) (volume.CopyIdentity, error) {
	copy := state.CurrentCopy
	if state.Phase != volumeapi.PhaseMoving || state.ActiveMove != move.Name || state.OwnerNode != move.Spec.SourceNode ||
		state.Deleting || state.HasCleanup || len(state.PublishedNodes) != 0 ||
		copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing || copy.NodeName != move.Spec.SourceNode ||
		copy.VolumeID != move.Spec.VolumeID || copy.VolumeUID != state.UID {
		return volume.CopyIdentity{}, fmt.Errorf("source volume has no locked unpublished serving-copy identity")
	}
	if move.Status.SourceCopy == nil || *move.Status.SourceCopy != *copy {
		return volume.CopyIdentity{}, fmt.Errorf("source copy identity changed before capacity admission")
	}
	return *copy, nil
}
