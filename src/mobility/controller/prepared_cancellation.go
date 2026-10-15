package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Cancellation first fences the Move writer. It never evicts a consumer or
// releases a reservation in the caller's observation of the preparing path.
func (r *Reconciler) requestPreparationCancellation(ctx context.Context, move *volumeapi.Move, reason string) error {
	if err := cancellablePreparation(*move); err != nil {
		return err
	}
	if move.Status.Phase == string(fsm.PhaseCanceling) || move.Status.Phase == string(fsm.PhaseCancelled) {
		return nil
	}
	previous := move.Status
	move.Status.Phase, move.Status.Reason = string(fsm.PhaseCanceling), reason
	move.Status.Message = "cancelling preparation before destination execution approval"
	return r.persistMoveStatus(ctx, move, previous)
}

// Re-observe after each write. In particular, releasing the logical hold must
// precede clearing ActiveMove: the capacity ledger binds a live hold to that
// exact lock. A crash between those writes resumes from Cancelled.
func (r *Reconciler) reconcileCancellation(ctx context.Context, move volumeapi.Move) error {
	if err := cancellablePreparation(move); err != nil {
		return err
	}
	phase := fsm.Phase(move.Status.Phase)
	if phase != fsm.PhaseCanceling && phase != fsm.PhaseCancelled {
		return fmt.Errorf("Move is not cancelling preparation")
	}
	if phase == fsm.PhaseCancelled && move.Status.Preparation != nil {
		return fmt.Errorf("cancelled Move still holds a preparation reservation")
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return err
	}

	state, err := r.cancellationVolume(ctx, move)
	if err != nil {
		return err
	}
	locked := state.ActiveMove == move.Name
	if phase == fsm.PhaseCanceling && !locked && move.Status.SourceCopy != nil {
		// An initial claim PUT may still arrive after its caller timed out.
		// Acquire the exact Ready source for settlement, then release it below
		// in a later pass. Those real writes fence the delayed claim before the
		// journal can disappear; an unchanged status PUT is not an RV fence.
		next := state
		next.ActiveMove = move.Name
		return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, state, next)
	}
	// A timed-out StartingDisruption PUT may arrive after Cancelled but
	// before unlock. Restore it here too, preserving current publication.
	if locked && state.Phase == volumeapi.PhaseMoving {
		next := state
		next.Phase = volumeapi.PhaseReady
		return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, state, next)
	}

	absent, err := r.cancelPreparationPlacement(ctx, move)
	if err != nil || !absent {
		return err
	}
	if phase == fsm.PhaseCanceling {
		previous := move.Status
		move.Status.Phase = string(fsm.PhaseCancelled)
		move.Status.Preparation = nil
		move.Status.CapacityReason = "PreparationCancelled"
		move.Status.Message = "preparation cancelled; source authority preserved"
		return r.persistMoveStatus(ctx, &move, previous)
	}
	if locked {
		next := state
		next.ActiveMove = ""
		return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, state, next)
	}
	// An old settled cancellation may coexist with a later Move or owner. It
	// owns only its journal now and must never restore that newer Volume state.
	return r.reconcileTerminalMove(ctx, move)
}

func (r *Reconciler) cancellationVolume(ctx context.Context, move volumeapi.Move) (volumeapi.State, error) {
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil && !apierrors.IsNotFound(err) {
		return volumeapi.State{}, fmt.Errorf("observe cancellation source: %w", err)
	}
	if err == nil && state.ActiveMove == move.Name {
		return state, cancellationSource(move, state)
	}
	if move.Status.Phase == string(fsm.PhaseCancelled) {
		return state, nil // The old journal cannot modify a later Volume owner.
	}
	if move.Status.Preparation != nil {
		return state, fmt.Errorf("preparation reservation has lost its exact ActiveMove")
	}
	if state.ActiveMove != "" {
		return state, fmt.Errorf("cancellation source is owned by another Move")
	}
	if move.Status.SourceCopy == nil {
		return state, nil // No plan, hold, disk authority or owned Volume lock.
	}
	if err != nil {
		return state, fmt.Errorf("observe planned cancellation source: %w", err)
	}
	if err := cancellationSource(move, state); err != nil {
		return state, err
	}
	if state.Phase != volumeapi.PhaseReady {
		return state, fmt.Errorf("unlocked cancellation source is not Ready")
	}
	return state, nil
}

func cancellablePreparation(move volumeapi.Move) error {
	if move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 || move.Name == "" || move.UID == "" {
		return fmt.Errorf("cancellation requires an identified prepared Move")
	}
	status := move.Status
	if status.CapacityApproved || status.IncomingCopy != nil || status.DestinationCopy != nil ||
		status.CopyOperationID != "" || status.PromotionOperationID != "" || status.CopyJobName != "" ||
		status.PromotionJobName != "" || status.CleanupPhase != "" || status.RollbackRequiredGeneration != 0 ||
		status.RecoveryPhase != "" || status.RecoveryOwner != "" {
		return fmt.Errorf("preparation cancellation cannot release destination execution or recovery authority")
	}
	return nil
}

func cancellationSource(move volumeapi.Move, state volumeapi.State) error {
	source := move.Status.SourceCopy
	if source == nil || source.Validate() != nil || source.Role != volume.RoleServing ||
		source.VolumeID != move.Spec.VolumeID || source.NodeName != move.Spec.SourceNode ||
		state.UID != source.VolumeUID || state.OwnerNode != move.Spec.SourceNode ||
		state.CurrentCopy == nil || *state.CurrentCopy != *source || state.Deleting || state.HasCleanup ||
		(state.Phase != volumeapi.PhaseReady && state.Phase != volumeapi.PhaseMoving) {
		return fmt.Errorf("cancellation cannot prove the exact authoritative source copy")
	}
	return nil
}

func (r *Reconciler) cancelPreparationPlacement(ctx context.Context, move volumeapi.Move) (bool, error) {
	names := namesFor(move.Name)
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("observe cancelled placement: %w", err)
	}
	if err := validatePlacementIdentity(pod, move, names); err != nil {
		return false, err
	}
	if pod.UID == "" || (move.Status.PlacementPodUID != "" && string(pod.UID) != move.Status.PlacementPodUID) {
		return false, fmt.Errorf("cancelled placement Pod UID changed")
	}
	if pod.DeletionTimestamp != nil {
		return false, nil
	}
	uid := pod.UID
	if err := r.Client.CoreV1().Pods(r.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("delete cancelled placement: %w", err)
	}
	// A successful DELETE can leave a terminating Pod. Only a later GET
	// returning NotFound permits releasing its logical reservation.
	return false, nil
}
