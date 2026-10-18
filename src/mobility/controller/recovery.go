package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	recoveryQuiescing  = fsm.RecoveryQuiescing
	recoveryVerifying  = fsm.RecoveryVerifying
	recoveryRetiring   = fsm.RecoveryRetiring
	recoveryResuming   = fsm.RecoveryResuming
	recoveryCompleting = fsm.RecoveryCompleting
	recoveryRecovered  = fsm.RecoveryRecovered
)

// Recovery keeps the failed Move phase and the current authoritative owner.
// Only Decide selects recovery progression; these reads grant no new owner.
func (r *Reconciler) observeRecovery(ctx context.Context, move volumeapi.Move) (observation, error) {
	result := observation{Names: namesFor(move.Name)}
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return result, err
	}
	result.Volume = state
	if move.Status.RecoveryPhase == recoveryCompleting && state.Phase == volumeapi.PhaseReady && state.ActiveMove == "" && state.OwnerNode == move.Status.RecoveryOwner {
		result.FSM.RecoveryUnlockObserved = true
		return result, nil
	}
	if reason, err := recoveryGuards(move, state); err != nil {
		return result, recoveryObservationError{reason, err}
	}
	if err := r.recoveryNodes(ctx, move, state); err != nil {
		return result, recoveryObservationError{"NodeNotReady", err}
	}
	claim, err := r.recordedWorkloadClaim(ctx, move)
	if err != nil {
		return result, recoveryObservationError{"BindingMismatch", err}
	}
	result.Claim = claim
	return result, nil
}

type recoveryObservationError struct {
	reason string
	error
}

func (e recoveryObservationError) Unwrap() error { return e.error }

// recoveryGuards holds the invariants that must still be true before any
// recovery step runs. It returns the exact status reason and error the caller
// records, or an empty reason and nil when recovery may proceed.
func recoveryGuards(move volumeapi.Move, state volumeapi.State) (string, error) {
	if state.ActiveMove != move.Name || (state.OwnerNode != move.Spec.SourceNode && state.OwnerNode != move.Status.DestinationNode) || state.OwnerNode == "" {
		return "AuthorityMismatch", fmt.Errorf("expected this move and source or committed destination owner")
	}
	if move.Status.RecoveryOwner != "" && state.OwnerNode != move.Status.RecoveryOwner {
		return "AuthorityMismatch", fmt.Errorf("owner changed since recovery request")
	}
	if move.Status.DestinationNode == "" && (move.Status.CopyJobName != "" || move.Status.PromotionJobName != "") {
		return "DestinationUnknown", fmt.Errorf("disk work was recorded without a destination; operator investigation required")
	}
	canBeReady := move.Status.RecoveryPhase == recoveryResuming || move.Status.RecoveryPhase == recoveryRetiring || move.Status.RecoveryPhase == recoveryCompleting
	if state.Phase != volumeapi.PhaseBlocked && !(canBeReady && state.Phase == volumeapi.PhaseReady) {
		return "StateMismatch", fmt.Errorf("cannot recover volume in phase %q", state.Phase)
	}
	for _, published := range state.PublishedNodes {
		if published != state.OwnerNode {
			return "ForeignPublication", fmt.Errorf("node %q is still published; confirm unpublish before recovery", published)
		}
	}
	return "", nil
}

func (r *Reconciler) executeRecovery(ctx context.Context, move *volumeapi.Move, observed observation, action fsm.Action) (bool, error) {
	switch action {
	case fsm.ActionBeginRecovery:
		move.Status.RecoveryOwner = observed.Volume.OwnerNode
		return true, nil
	case fsm.ActionQuiesceRecovery:
		return r.quiesceMove(ctx, *move)
	case fsm.ActionVerifyRecovery:
		return r.recoveryJob(ctx, *move)
	case fsm.ActionRetireRecovery:
		return r.settleRecoveryArtifacts(ctx, move, observed.Volume)
	case fsm.ActionResumeRecovery:
		return r.resumeRecovery(ctx, *move, observed.Volume, observed.Claim)
	case fsm.ActionCompleteRecovery:
		return r.completeRecovery(ctx, *move, observed.Volume)
	default:
		return false, fmt.Errorf("unsupported recovery action %q", action)
	}
}

func (r *Reconciler) resumeRecovery(ctx context.Context, move volumeapi.Move, state volumeapi.State, claim *corev1.PersistentVolumeClaim) (bool, error) {
	if state.Phase == volumeapi.PhaseBlocked {
		next := state
		next.Phase = volumeapi.PhaseReady
		return false, r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseBlocked, move.Name, state.OwnerNode, next)
	}
	placed, err := r.recoverPlacement(ctx, move, claim)
	return placed && slices.Contains(state.PublishedNodes, state.OwnerNode), err
}

func (r *Reconciler) completeRecovery(ctx context.Context, move volumeapi.Move, state volumeapi.State) (bool, error) {
	done, err := r.removeRecoveryJobs(ctx, move)
	if err != nil || !done {
		return false, err
	}
	next := state
	next.ActiveMove = ""
	err = r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseReady, move.Name, state.OwnerNode, next)
	return err == nil, err
}

func (r *Reconciler) recoveryNodes(ctx context.Context, move volumeapi.Move, state volumeapi.State) error {
	for _, name := range []string{move.Spec.SourceNode, move.Status.DestinationNode} {
		if name == "" {
			continue
		}
		node, err := r.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !admission.NodeReady(node) || node.DeletionTimestamp != nil {
			return fmt.Errorf("node %q must be Ready", name)
		}
		if name == state.OwnerNode && node.Spec.Unschedulable {
			return fmt.Errorf("uncordon current owner %q before requesting service recovery", name)
		}
	}
	if state.CurrentCopy == nil {
		return fmt.Errorf("recovery owner copy identity is missing")
	}
	for _, copy := range []*volume.CopyIdentity{state.CurrentCopy, move.Status.SourceCopy, move.Status.IncomingCopy, move.Status.DestinationCopy} {
		if copy == nil {
			continue
		}
		if _, err := r.poolMountPath(ctx, copy); err != nil {
			return err
		}
	}
	return nil
}

// Do not edit workload templates, bypass PDBs, or re-add a removed scheduling gate.
func (r *Reconciler) recoverPlacement(ctx context.Context, move volumeapi.Move, claim *corev1.PersistentVolumeClaim) (bool, error) {
	pods, err := r.Client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	placed := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if terminalPod(pod) || !podUsesClaim(pod, claim.Name) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			return false, nil
		}
		if pod.Spec.NodeName == move.Status.RecoveryOwner && !hasPlacementHold(pod) {
			placed = true
			continue
		}
		if metav1.GetControllerOf(pod) == nil {
			return false, fmt.Errorf("bare consumer %q must be handled by its operator", pod.Name)
		}
		uid := pod.UID
		err := r.Client.PolicyV1().Evictions(pod.Namespace).Evict(ctx, &policyv1.Eviction{
			ObjectMeta:    metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
		})
		if apierrors.IsNotFound(err) {
			err = nil
		}
		return false, err
	}
	return placed, nil
}

func (r *Reconciler) recoveryError(ctx context.Context, move volumeapi.Move, reason string, err error) error {
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	if errors.Is(err, errMoveStatusPersistence) {
		return err
	}
	previous := move.Status
	move.Status.RecoveryReason = reason
	move.Status.RecoveryMessage = err.Error()
	move.Status.Message = recoveryErrorMessage(reason, err)
	if statusErr := r.persistMoveStatus(ctx, &move, previous); statusErr != nil {
		return fmt.Errorf("%w (record recovery error: %v)", err, statusErr)
	}
	return err
}
