package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Cancellation reads only source authority and its exact placement. Lost
// workload/PV/PVC/Node objects cannot prevent releasing an unapproved hold.
func (r *Reconciler) observeCancellation(ctx context.Context, move volumeapi.Move) (observation, error) {
	result := observation{Names: namesFor(move.Name)}
	if err := cancellablePreparation(move); err != nil {
		return result, err
	}
	if move.Status.Phase == string(fsm.PhaseCancelled) && move.Status.Preparation != nil {
		return result, fmt.Errorf("cancelled Move still holds a preparation reservation")
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return result, err
	}
	state, err := r.cancellationVolume(ctx, move)
	if err != nil {
		return result, err
	}
	result.Volume = state
	locked := state.ActiveMove == move.Name
	if move.Status.Phase == string(fsm.PhaseCanceling) && !locked && move.Status.SourceCopy != nil || locked && state.Phase == volumeapi.PhaseMoving {
		return result, nil
	}
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, result.Names.PlacementPod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		result.FSM.PlacementAbsent = true
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("observe cancelled placement: %w", err)
	}
	if err := validatePlacementOwner(pod, move, result.Names); err != nil {
		return result, err
	}
	if pod.UID == "" {
		return result, fmt.Errorf("cancelled placement Pod has no UID")
	}
	result.Placement = pod
	return result, nil
}

func (r *Reconciler) deleteCancelledPlacement(ctx context.Context, move volumeapi.Move, pod *corev1.Pod) error {
	if pod == nil || pod.DeletionTimestamp != nil {
		return nil
	}
	if err := validatePlacementOwner(pod, move, namesFor(move.Name)); err != nil {
		return err
	}
	uid := pod.UID
	if err := r.Client.CoreV1().Pods(r.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete cancelled placement: %w", err)
	}
	// DELETE success is not absence. The next GET must return NotFound.
	return nil
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
