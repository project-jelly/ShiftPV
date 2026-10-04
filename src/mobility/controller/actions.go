package controller

import (
	"context"
	"errors"
	"fmt"

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
	case fsm.ActionLockVolume:
		return r.lockVolume(ctx, move, observed)
	case fsm.ActionEvictConsumer:
		return r.evictConsumer(ctx, move, observed)
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

func (r *Reconciler) lockVolume(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.PV == nil || observed.Claim == nil || observed.Consumer == nil {
		return fmt.Errorf("mobility binding or consumer disappeared before volume lock")
	}
	if observed.Volume.OwnerNode != move.Spec.SourceNode {
		return fmt.Errorf("volume owner %q does not match move source %q", observed.Volume.OwnerNode, move.Spec.SourceNode)
	}
	if observed.Volume.CurrentCopy == nil || observed.Volume.CurrentCopy.Role != volume.RoleServing || observed.Volume.CurrentCopy.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("source volume has no exact serving-copy identity")
	}
	if move.Status.SourceCopy != nil && *move.Status.SourceCopy != *observed.Volume.CurrentCopy {
		return fmt.Errorf("source copy identity changed before volume lock")
	}
	if observed.Volume.CurrentCopy != nil {
		copy := *observed.Volume.CurrentCopy
		move.Status.SourceCopy = &copy
	}
	if !observed.FSM.VolumeLocked {
		next := volumeapi.State{
			UID:            observed.Volume.UID,
			Phase:          volumeapi.PhaseMoving,
			OwnerNode:      move.Spec.SourceNode,
			ActiveMove:     move.Name,
			PublishedNodes: append([]string(nil), observed.Volume.PublishedNodes...),
		}
		err := r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseReady, "", move.Spec.SourceNode, next)
		if err != nil && !errors.Is(err, volumeapi.ErrStateConflict) {
			return err
		}
		if err != nil {
			current, getErr := r.Repository.Get(ctx, move.Spec.VolumeID)
			if getErr != nil {
				return getErr
			}
			if current.Phase != volumeapi.PhaseMoving || current.ActiveMove != move.Name || current.OwnerNode != move.Spec.SourceNode {
				return err
			}
		}
	}
	move.Status.PersistentVolumeName = observed.PV.Name
	move.Status.ClaimNamespace = observed.Claim.Namespace
	move.Status.ClaimName = observed.Claim.Name
	move.Status.ConsumerName = observed.Consumer.Name
	move.Status.ConsumerUID = string(observed.Consumer.UID)
	move.Status.CandidateNodes = append([]string(nil), observed.CandidateNodes...)
	return nil
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
		if pod.UID != uid || (move.Status.ReplacementUID != "" && string(pod.UID) != move.Status.ReplacementUID) {
			return fmt.Errorf("replacement Pod identity changed before placement release")
		}
		if pod.Spec.NodeSelector["kubernetes.io/hostname"] != move.Status.DestinationNode {
			return fmt.Errorf("replacement Pod is not pinned to destination %q", move.Status.DestinationNode)
		}
		gates := pod.Spec.SchedulingGates[:0]
		for _, gate := range pod.Spec.SchedulingGates {
			if gate.Name != placementHoldName {
				gates = append(gates, gate)
			}
		}
		if len(gates) == len(pod.Spec.SchedulingGates) {
			if pod.Annotations[placementAnnotationKey] == "owner" {
				return nil
			}
			if pod.Annotations == nil {
				pod.Annotations = map[string]string{}
			}
			pod.Annotations[placementAnnotationKey] = "owner"
			_, err = r.Client.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
			return err
		}
		pod.Spec.SchedulingGates = gates
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
	return nil
}

func (r *Reconciler) ensureCopy(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.DestinationNode == "" || observed.Replacement == nil || !hasPlacementHold(observed.Replacement) {
		return fmt.Errorf("scheduled destination and held replacement Pod are required")
	}
	if !move.Status.CapacityApproved || move.Status.SourceBytes <= 0 {
		return fmt.Errorf("destination capacity is not approved")
	}
	previous := move.Status
	move.Status.DestinationNode = observed.DestinationNode
	move.Status.ReplacementName = observed.Replacement.Name
	move.Status.ReplacementUID = string(observed.Replacement.UID)
	move.Status.CopyJobName = observed.Names.CopyJob
	if err := r.prepareMoveCopyIdentities(ctx, move); err != nil {
		return err
	}
	// Persist the destination before starting disk-side work, including API retries.
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
	if move.Status.DestinationNode == "" {
		move.Status.DestinationNode = observed.DestinationNode
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
		destination = observed.DestinationNode
	}
	if destination == "" {
		return fmt.Errorf("destination node is empty")
	}
	if observed.FSM.OwnerCommitted {
		return confirmCommittedCopy(move, observed)
	}
	if err := r.requireScheduledPlacement(ctx, *move); err != nil {
		return err
	}
	if move.Status.DestinationCopy == nil || move.Status.DestinationCopy.Role != volume.RoleServing || move.Status.DestinationCopy.NodeName != destination {
		return fmt.Errorf("destination serving-copy identity is missing")
	}
	next := volumeapi.State{UID: observed.Volume.UID, Phase: volumeapi.PhaseReady, OwnerNode: destination, ActiveMove: move.Name, PublishedNodes: append([]string(nil), observed.Volume.PublishedNodes...)}
	var destinationCopy volume.CopyIdentity
	if move.Status.DestinationCopy != nil {
		destinationCopy = *move.Status.DestinationCopy
		next.CurrentCopy = &destinationCopy
	}
	err := r.Repository.CompareAndSetState(ctx, move.Spec.VolumeID, volumeapi.PhaseMoving, move.Name, move.Spec.SourceNode, next)
	if err != nil && !errors.Is(err, volumeapi.ErrStateConflict) {
		return err
	}
	if err != nil {
		return r.confirmCommitConflict(ctx, *move, destination, destinationCopy, err)
	}
	return nil
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

func (r *Reconciler) prepareMoveCopyIdentities(ctx context.Context, move *volumeapi.Move) error {
	if move.UID == "" || move.Status.SourceCopy == nil || move.Status.SourceCopy.Validate() != nil || move.Status.DestinationNode == "" || move.Status.DestinationPoolUID == "" {
		return fmt.Errorf("move source identity or destination is missing")
	}
	var pool volumeapi.Pool
	pools, err := r.Repository.Pools(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range pools {
		if candidate.NodeName == move.Status.DestinationNode && candidate.UID == move.Status.DestinationPoolUID {
			pool = candidate
			break
		}
	}
	if pool.Name == "" || pool.UID == "" {
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
	move.Status.IncomingCopy, move.Status.DestinationCopy = &incoming, &destination
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
	destination := move.Status.DestinationCopy
	if !validDestinationAuthorityIntent(move) ||
		state.UID != destination.VolumeUID || state.Phase != volumeapi.PhaseReady || state.OwnerNode != move.Status.DestinationNode ||
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
