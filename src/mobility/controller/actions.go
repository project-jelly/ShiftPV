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
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func (r *Reconciler) execute(ctx context.Context, move *volumeapi.Move, observed observation, decision fsm.Decision) error {
	switch decision.Action {
	case fsm.ActionWait:
		return nil
	case fsm.ActionEnsurePlacement:
		return r.ensurePlacement(ctx, move, observed)
	case fsm.ActionEnsureCapacity:
		return r.ensureCapacity(ctx, move, observed)
	case fsm.ActionDeletePlacement:
		return r.deletePlacement(ctx, *move)
	case fsm.ActionReleasePlacement:
		return r.releasePlacement(ctx, move, observed)
	case fsm.ActionEnsureCopy:
		return r.ensureCopy(ctx, move, observed)
	case fsm.ActionEnsurePromotion:
		return r.ensurePromotion(ctx, move, observed)
	case fsm.ActionCommitOwner:
		return r.commitOwner(ctx, move, observed)
	case fsm.ActionEnsureCleanup:
		return r.ensureCleanupContract(ctx, move)
	case fsm.ActionConfirmCleanup:
		return r.acknowledgeCleanup(ctx, *move)
	case fsm.ActionMarkSucceeded:
		return r.markSucceeded(ctx, move, observed)
	case fsm.ActionMarkBlocked:
		return r.markBlocked(ctx, move, observed)
	default:
		return fmt.Errorf("unsupported mobility action %q", decision.Action)
	}
}

func (r *Reconciler) evictConsumer(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.Consumer == nil {
		move.Status.EvictionRequested = true
		return nil
	}
	err := r.Client.PolicyV1().Evictions(observed.Consumer.Namespace).Evict(ctx, &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: observed.Consumer.Name, Namespace: observed.Consumer.Namespace},
		DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &observed.Consumer.UID}},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("evict consumer Pod: %w", err)
	}
	move.Status.EvictionRequested = true
	return nil
}

func (r *Reconciler) releasePlacement(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.Replacement == nil {
		return nil
	}
	name := observed.Replacement.Name
	namespace := observed.Replacement.Namespace
	uid := observed.Replacement.UID
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := r.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pod.UID != uid {
			return fmt.Errorf("replacement Pod identity changed before placement release")
		}
		if err := r.validateReplacementRelease(ctx, *move, pod); err != nil {
			return err
		}
		selected := pod.Spec.NodeSelector[corev1.LabelHostname]
		if selected != "" && selected != move.Status.DestinationNode {
			return fmt.Errorf("replacement Pod requires node %q, not committed destination %q", selected, move.Status.DestinationNode)
		}
		if !hasPlacementHold(pod) {
			if selected == move.Status.DestinationNode && pod.Annotations[placementAnnotationKey] == "owner" {
				return nil
			}
			return fmt.Errorf("replacement Pod no longer has the placement hold")
		}
		if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" {
			return fmt.Errorf("replacement Pod is not safely held before scheduling")
		}
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}
		// Pin and release in one RV-fenced update; a recreated consumer never
		// becomes schedulable between these two effects.
		pod.Spec.NodeSelector[corev1.LabelHostname] = move.Status.DestinationNode
		pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
			return gate.Name == placementHoldName
		})
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[placementAnnotationKey] = "owner"
		_, err = r.Client.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("release replacement Pod placement hold: %w", err)
	}
	move.Status.ReplacementName = name
	move.Status.ReplacementUID = string(uid)
	return nil
}

func (r *Reconciler) validateReplacementRelease(ctx context.Context, move volumeapi.Move, pod *corev1.Pod) error {
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return err
	}
	if !hasCommittedDestinationAuthority(move, state, false) {
		return fmt.Errorf("placement release requires the exact committed destination authority")
	}
	claim, err := r.recordedWorkloadClaim(ctx, move)
	if err != nil {
		return err
	}
	if pod.Namespace != claim.Namespace || len(pvcNames(pod.Spec)) != 1 || !podUsesClaim(pod, claim.Name) {
		return fmt.Errorf("replacement Pod no longer uses the recorded claim exclusively")
	}
	if string(pod.UID) != move.Status.ReplacementUID {
		if !ownerPlacementCandidate(pod) {
			return fmt.Errorf("recreated replacement Pod is not managed and safely held")
		}
		_, reason, err := r.consumerTemplate(ctx, pod)
		if err != nil {
			return fmt.Errorf("observe recreated replacement controller: %w", err)
		}
		if reason != "" {
			return fmt.Errorf("recreated replacement controller is unavailable: %s", reason)
		}
	}
	return nil
}

func (r *Reconciler) ensureCopy(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.DestinationNode == "" || observed.Replacement == nil || !hasPlacementHold(observed.Replacement) {
		return fmt.Errorf("scheduled destination and held replacement Pod are required")
	}
	if !move.Status.CapacityApproved || move.Status.SourceBytes <= 0 {
		return fmt.Errorf("destination capacity is not approved")
	}
	if observed.DestinationNode != move.Status.DestinationNode {
		return fmt.Errorf("observed destination differs from the approved plan")
	}
	if err := validatePreparedStage(*move); err != nil {
		return err
	}
	previous := move.Status
	move.Status.ReplacementName = observed.Replacement.Name
	move.Status.ReplacementUID = string(observed.Replacement.UID)
	// Persist the replacement identity before pinning or starting disk-side work.
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		return err
	}
	if err := r.pinReplacement(ctx, *move); err != nil {
		return fmt.Errorf("pin held replacement Pod: %w", err)
	}
	return r.ensureCopyResources(ctx, *move, observed.Names)
}

func (r *Reconciler) ensurePromotion(ctx context.Context, move *volumeapi.Move, observed observation) error {
	previous := move.Status
	if move.Status.DestinationNode == "" || observed.DestinationNode != move.Status.DestinationNode {
		return fmt.Errorf("observed destination differs from the approved plan")
	}
	move.Status.PromotionJobName = observed.Names.PromotionJob
	if move.Status.IncomingCopy == nil || move.Status.DestinationCopy == nil || move.Status.PromotionOperationID == "" {
		return fmt.Errorf("promotion identity is missing")
	}
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		return err
	}
	return r.ensurePromotionJob(ctx, *move, observed.Names)
}

func (r *Reconciler) commitOwner(ctx context.Context, move *volumeapi.Move, observed observation) error {
	destination := move.Status.DestinationNode
	if destination == "" {
		return fmt.Errorf("destination node is empty")
	}
	if observed.FSM.OwnerCommitted {
		return confirmCommittedCopy(move, observed)
	}
	if r.PoolLocks == nil {
		return fmt.Errorf("Pool capacity locker is required for owner handoff")
	}
	destinationPoolUID, err := ownerCommitPoolUID(*move, observed.Volume, destination)
	if err != nil {
		return err
	}
	if err := r.requireScheduledPlacement(ctx, *move); err != nil {
		return err
	}
	destinationCopy := *move.Status.DestinationCopy
	next := volumeapi.State{UID: observed.Volume.UID, Phase: volumeapi.PhaseReady, OwnerNode: destination,
		ActiveMove: move.Name, CurrentCopy: &destinationCopy, PublishedNodes: append([]string(nil), observed.Volume.PublishedNodes...)}
	// Capacity readers list Volumes before Moves under the destination Pool lock.
	// Keep the incoming owner change from falling between those reads. A source
	// reader may conservatively retain its old Volume reservation during handoff.
	unlock, acquired := r.PoolLocks.TryLock(destinationPoolUID)
	if !acquired {
		return errPoolBusy
	}
	defer unlock()
	err = r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseMoving, move.Name, move.Spec.SourceNode, next)
	if err != nil && !errors.Is(err, volumeapi.ErrStateConflict) {
		return err
	}
	if err != nil {
		return r.confirmCommitConflict(ctx, *move, destination, destinationCopy, err)
	}
	return nil
}

// ownerCommitPoolUID binds the destination ledger key to the exact copy being
// handed off. Node names are not capacity identities.
func ownerCommitPoolUID(move volumeapi.Move, state volumeapi.State, destination string) (string, error) {
	source, target := move.Status.SourceCopy, move.Status.DestinationCopy
	if source == nil || source.Validate() != nil || source.Role != volume.RoleServing ||
		source.VolumeID != move.Spec.VolumeID || source.VolumeUID != state.UID || source.NodeName != move.Spec.SourceNode ||
		state.CurrentCopy == nil || *state.CurrentCopy != *source {
		return "", fmt.Errorf("source serving-copy identity does not match owner handoff")
	}
	if target == nil || target.Validate() != nil || target.Role != volume.RoleServing || target.NodeName != destination ||
		target.VolumeID != move.Spec.VolumeID || target.VolumeUID != source.VolumeUID ||
		target.InstallationID != source.InstallationID || target.PoolUID != move.Status.DestinationPoolUID || target.PoolUID == source.PoolUID {
		return "", fmt.Errorf("destination serving-copy identity does not match owner handoff")
	}
	return target.PoolUID, nil
}

// confirmCommittedCopy checks that an already committed owner still serves the
// exact destination copy this transaction promoted.
func confirmCommittedCopy(move *volumeapi.Move, observed observation) error {
	if move.Status.DestinationCopy == nil || observed.Volume.CurrentCopy == nil || *observed.Volume.CurrentCopy != *move.Status.DestinationCopy {
		return fmt.Errorf("committed owner has a different serving-copy identity")
	}
	return nil
}

// confirmCommitConflict re-reads the lock after a losing CAS. A conflict that
// already records this transaction's exact outcome is this controller's own
// earlier write replayed; anything else is returned as the conflict it is.
func (r *Reconciler) confirmCommitConflict(ctx context.Context, move volumeapi.Move, destination string, destinationCopy volume.CopyIdentity, conflict error) error {
	current, getErr := r.Repository.Get(ctx, move.Spec.VolumeID)
	if getErr != nil {
		return getErr
	}
	identityMismatch := current.CurrentCopy == nil || *current.CurrentCopy != destinationCopy
	if current.Phase != volumeapi.PhaseReady || current.OwnerNode != destination || current.ActiveMove != move.Name || identityMismatch {
		return conflict
	}
	return nil
}

// prepareMoveCopyIdentitiesForPool binds the full execution plan to the exact
// approved Pool. The plan must be durable with the hold, before any disk work.
func prepareMoveCopyIdentitiesForPool(move *volumeapi.Move, pool volumeapi.Pool) error {
	if !volume.ValidIdentityToken(move.UID) || move.Status.SourceCopy == nil || move.Status.SourceCopy.Validate() != nil {
		return fmt.Errorf("move source identity is missing")
	}
	if pool.Name == "" || pool.UID == "" || pool.UID != move.Status.DestinationPoolUID || pool.NodeName != move.Status.DestinationNode {
		return fmt.Errorf("destination Pool identity changed or is missing")
	}
	incoming, destination := volumeapi.MoveTransactionCopies(move.UID, move.Spec.VolumeID, volumeapi.MoveDestinationAnchor{
		InstallationID: move.Status.SourceCopy.InstallationID, PoolName: pool.Name, PoolUID: pool.UID,
		NodeName: move.Status.DestinationNode, VolumeUID: move.Status.SourceCopy.VolumeUID,
	})
	if incoming.Validate() != nil || destination.Validate() != nil {
		return fmt.Errorf("generated move copy identity is invalid")
	}
	if move.Status.IncomingCopy != nil && *move.Status.IncomingCopy != incoming || move.Status.DestinationCopy != nil && *move.Status.DestinationCopy != destination {
		return fmt.Errorf("move copy identity changed")
	}
	copyJob := namesFor(move.Name).CopyJob
	if move.Status.CopyJobName != "" && move.Status.CopyJobName != copyJob ||
		move.Status.CopyOperationID != "" && move.Status.CopyOperationID != volumeapi.MoveCopyOperationID(move.UID) ||
		move.Status.PromotionOperationID != "" && move.Status.PromotionOperationID != volumeapi.MovePromotionOperationID(move.UID) {
		return fmt.Errorf("move operation identity changed")
	}
	move.Status.IncomingCopy, move.Status.DestinationCopy = &incoming, &destination
	move.Status.CopyJobName = copyJob
	move.Status.CopyOperationID = volumeapi.MoveCopyOperationID(move.UID)
	move.Status.PromotionOperationID = volumeapi.MovePromotionOperationID(move.UID)
	return nil
}

func (r *Reconciler) markSucceeded(ctx context.Context, move *volumeapi.Move, observed observation) error {
	complete, failed, err := r.cleanupState(ctx, *move)
	if err != nil {
		return err
	}
	if !complete || failed {
		return fmt.Errorf("completion requires settled cleanup receipt")
	}
	if !completionAllowed(*move, observed.Volume, observed.VolumeMissing) {
		return fmt.Errorf("completion requires durable cleanup evidence and matching destination authority")
	}
	if err := r.deleteTransferResources(ctx, *move, observed.Names); err != nil {
		return err
	}
	if observed.VolumeMissing || observed.Volume.ActiveMove == "" {
		return nil
	}
	next := observed.Volume
	next.ActiveMove = ""
	return r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseReady, move.Name, observed.Volume.OwnerNode, next)
}

func completionAllowed(move volumeapi.Move, state volumeapi.State, missing bool) bool {
	if move.Status.Phase != string(fsm.PhaseCompleting) || !validDestinationAuthorityIntent(move) {
		return false
	}
	return missing || hasCommittedDestinationAuthority(move, state, true)
}

func hasCommittedDestinationAuthority(move volumeapi.Move, state volumeapi.State, allowReleased bool) bool {
	return state.Phase == volumeapi.PhaseReady && matchesCommittedDestinationIdentity(move, state, allowReleased)
}

func matchesCommittedDestinationIdentity(move volumeapi.Move, state volumeapi.State, allowReleased bool) bool {
	destination := move.Status.DestinationCopy
	if !validDestinationAuthorityIntent(move) ||
		state.UID != destination.VolumeUID || state.OwnerNode != move.Status.DestinationNode ||
		state.CurrentCopy == nil || *state.CurrentCopy != *destination {
		return false
	}
	return state.ActiveMove == move.Name || allowReleased && state.ActiveMove == ""
}

func validDestinationAuthorityIntent(move volumeapi.Move) bool {
	destination := move.Status.DestinationCopy
	return move.Name != "" && destination != nil && destination.Validate() == nil && destination.Role == volume.RoleServing &&
		move.Status.DestinationNode != "" && move.Status.DestinationNode != move.Spec.SourceNode &&
		move.Status.DestinationPoolUID != "" && destination.PoolUID == move.Status.DestinationPoolUID &&
		destination.VolumeID == move.Spec.VolumeID && destination.NodeName == move.Status.DestinationNode
}

// reconcileMove records decision.Reason on the Move after this action returns,
// so the blocked reason is not assigned here.
func (r *Reconciler) markBlocked(ctx context.Context, move *volumeapi.Move, observed observation) error {
	move.Status.Message = "mobility stopped without automatic rollback"
	if observed.Volume.Phase == volumeapi.PhaseBlocked && observed.Volume.ActiveMove == move.Name {
		return nil
	}
	if observed.Volume.Phase == volumeapi.PhaseReady && observed.Volume.ActiveMove == "" && fsm.Phase(move.Status.Phase) == fsm.PhasePending {
		return nil
	}
	if (observed.Volume.Phase != volumeapi.PhaseReady || observed.Volume.ActiveMove != move.Name) &&
		(observed.Volume.Phase != volumeapi.PhaseMoving || observed.Volume.ActiveMove != move.Name) {
		return fmt.Errorf("cannot block volume in phase %q with active move %q", observed.Volume.Phase, observed.Volume.ActiveMove)
	}
	next := observed.Volume
	next.Phase = volumeapi.PhaseBlocked
	next.ActiveMove = move.Name
	err := r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, observed.Volume.Phase, observed.Volume.ActiveMove, observed.Volume.OwnerNode, next)
	if err == nil || !errors.Is(err, volumeapi.ErrStateConflict) {
		return err
	}
	current, getErr := r.Repository.Get(ctx, move.Spec.VolumeID)
	if getErr != nil {
		return getErr
	}
	if current.Phase == volumeapi.PhaseBlocked && current.ActiveMove == move.Name {
		return nil
	}
	return err
}
