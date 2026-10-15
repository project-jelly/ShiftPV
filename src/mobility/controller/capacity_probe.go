package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// ErrCapacityProbePending leaves the current reservation and source authority
// intact while the bounded destination observation becomes available.
var ErrCapacityProbePending = errors.New("destination capacity probe is pending")

func preparationCapacityReason(unavailable bool) string {
	if unavailable {
		return "DestinationCapacityProbeUnavailable"
	}
	return "DestinationReservationLimit"
}

// This checks transport availability before disruption, not execution space.
// Conversion must still measure fresh space after the source is quiesced.
func (r *Reconciler) startPreparedSource(ctx context.Context, move volumeapi.Move, source volumeapi.State) error {
	hold := move.Status.Preparation
	if hold == nil || hold.Pool.UID == "" || r.PoolLocks == nil || r.CapacityProbe == nil {
		return fmt.Errorf("prepared disruption requires its Pool identity, locker and capacity probe")
	}
	unlock, acquired := r.PoolLocks.TryLock(hold.Pool.UID)
	if !acquired {
		return errPoolBusy
	}
	defer unlock()
	pool, err := r.Repository.ReadyPoolForIdentity(ctx, hold.Pool.Name, hold.Pool.UID, hold.NodeName)
	if err != nil {
		return err
	}
	if !pool.Status.CapacityProbeSupported {
		return ErrCapacityProbePending
	}
	if _, err := r.CapacityProbe.StatFSForPool(ctx, pool); err != nil {
		return fmt.Errorf("probe prepared destination before disruption: %w", err)
	}
	next := source
	next.Phase = volumeapi.PhaseMoving
	return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, source, next)
}
