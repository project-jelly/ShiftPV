package controller

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Preparation owns no disk effects. Its source remains publishable until a
// durable StartingDisruption intent precedes the source Moving transition.
func (r *Reconciler) reconcilePreparation(ctx context.Context, move volumeapi.Move) error {
	if move.Status.CapacityApproved || !noDestinationEffectIntent(move) || move.Status.CleanupPhase != "" || move.Status.RollbackRequiredGeneration != 0 {
		return fmt.Errorf("preparation cannot run with destination execution authority")
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return err
	}
	if move.Deleting {
		return r.requestPreparationCancellation(ctx, &move, "MoveDeleting")
	}
	if !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		// The finalizer write changes resourceVersion. Observe it before any
		// plan, Volume claim, reservation or disruptive action.
		return r.Repository.AddMoveFinalizer(ctx, move.Name, move.UID)
	}
	probe := move
	probe.Status.CandidateNodes = nil // Eligibility is live until disruption.
	observed := observation{Names: namesFor(move.Name)}
	pools, _, err := r.observeEligibility(ctx, probe, &observed)
	if err != nil {
		return err
	}
	if err := preparationSourceAuthority(move, observed.Volume); err != nil {
		return err
	}
	if !observed.SourceCordoned {
		return r.requestPreparationCancellation(ctx, &move, "SourceNotCordoned")
	}
	if move.Status.SourceCopy == nil {
		return r.initializePreparation(ctx, &move, observed)
	}
	return r.continuePreparation(ctx, move, observed, pools)
}

func (r *Reconciler) continuePreparation(ctx context.Context, move volumeapi.Move, observed observation, pools poolIndex) error {
	if !preparedBindingMatches(move, observed.PV, observed.Claim) {
		return r.requestPreparationCancellation(ctx, &move, "VolumeBindingChanged")
	}
	if move.Status.Phase == string(fsm.PhaseStartingDisruption) {
		return r.startPreparedDisruption(ctx, &move, observed)
	}
	if observed.Consumer == nil || observed.Consumer.DeletionTimestamp != nil {
		return r.requestPreparationCancellation(ctx, &move, "ConsumerChanged")
	}
	if move.Status.Preparation != nil {
		if observed.Volume.ActiveMove != move.Name {
			return fmt.Errorf("preparation reservation lost its source claim")
		}
		if !observed.FSM.SourceHealthy || observed.FSM.PreflightDeferred || !observed.FSM.PreconditionsValid {
			return r.requestPreparationCancellation(ctx, &move, preparationReason(observed.FSM.UnsafeReason, "PreparationChanged"))
		}
		return r.finishPreparation(ctx, &move, observed)
	}
	if !observed.FSM.SourceHealthy || observed.FSM.PreflightDeferred || !observed.FSM.PreconditionsValid {
		if err := r.deletePlacement(ctx, move); err != nil {
			return err
		}
		return r.preparationWait(ctx, &move, preparationReason(observed.FSM.UnsafeReason, "PreparationDeferred"))
	}
	if observed.Volume.ActiveMove == "" {
		next := observed.Volume
		next.ActiveMove = move.Name
		return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, observed.Volume, next)
	}
	return r.prepareDestination(ctx, &move, observed, pools)
}

func preparationSourceAuthority(move volumeapi.Move, state volumeapi.State) error {
	copy := state.CurrentCopy
	if state.Deleting || state.HasCleanup || state.OwnerNode != move.Spec.SourceNode ||
		copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing ||
		copy.VolumeUID != state.UID || copy.VolumeID != move.Spec.VolumeID || copy.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("prepared source authority is unavailable or changed")
	}
	if state.ActiveMove != "" && state.ActiveMove != move.Name {
		return fmt.Errorf("prepared source belongs to another Move")
	}
	if state.Phase != volumeapi.PhaseReady &&
		!(move.Status.Phase == string(fsm.PhaseStartingDisruption) && state.Phase == volumeapi.PhaseMoving && state.ActiveMove == move.Name) {
		return fmt.Errorf("prepared source has unexpected phase %q", state.Phase)
	}
	if move.Status.SourceCopy != nil && *move.Status.SourceCopy != *copy {
		return fmt.Errorf("prepared source serving-copy identity changed")
	}
	return nil
}

func preparedBindingMatches(move volumeapi.Move, pv *corev1.PersistentVolume, claim *corev1.PersistentVolumeClaim) bool {
	return validBinding(pv, claim, move.Spec.VolumeID) &&
		pv.Name == move.Status.PersistentVolumeName && string(pv.UID) == move.Status.PersistentVolumeUID &&
		claim.Namespace == move.Status.ClaimNamespace && claim.Name == move.Status.ClaimName && string(claim.UID) == move.Status.ClaimUID
}

func (r *Reconciler) initializePreparation(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.FSM.SourceAuthorityInvalid {
		return r.requestPreparationCancellation(ctx, move, preparationReason(observed.FSM.UnsafeReason, "VolumeBindingChanged"))
	}
	if !observed.FSM.PreconditionsValid || observed.FSM.PreflightDeferred {
		return r.preparationWait(ctx, move, preparationReason(observed.FSM.UnsafeReason, "PreparationDeferred"))
	}
	if observed.PV == nil || observed.PV.UID == "" || observed.Claim == nil || observed.Claim.UID == "" ||
		observed.Consumer == nil || observed.Consumer.UID == "" || observed.Consumer.DeletionTimestamp != nil {
		return fmt.Errorf("preparation requires exact live binding and consumer identities")
	}
	previous := move.Status
	source := *observed.Volume.CurrentCopy
	move.Status.SourceCopy = &source
	move.Status.PersistentVolumeName, move.Status.PersistentVolumeUID = observed.PV.Name, string(observed.PV.UID)
	move.Status.ClaimNamespace, move.Status.ClaimName, move.Status.ClaimUID = observed.Claim.Namespace, observed.Claim.Name, string(observed.Claim.UID)
	move.Status.ConsumerName, move.Status.ConsumerUID = observed.Consumer.Name, string(observed.Consumer.UID)
	move.Status.CandidateNodes = append([]string(nil), observed.CandidateNodes...)
	move.Status.Phase, move.Status.Reason = string(fsm.PhasePreparing), ""
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) preparationWait(ctx context.Context, move *volumeapi.Move, reason string) error {
	previous := move.Status
	if move.Status.Phase == "" {
		move.Status.Phase = string(fsm.PhasePending)
	}
	move.Status.Reason = reason
	move.Status.Message = "automatic retry; preparing destination without interrupting the source: " + reason
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) prepareDestination(ctx context.Context, move *volumeapi.Move, observed observation, pools poolIndex) error {
	candidates, err := r.preparationCandidates(ctx, *move, observed, pools)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		if err := r.deletePlacement(ctx, *move); err != nil {
			return err
		}
		return r.preparationWait(ctx, move, "DestinationReservationLimit")
	}
	placement, reason, err := r.ensurePreparedPlacement(ctx, *move, observed, candidates)
	if err != nil {
		return err
	}
	if reason != "" {
		return r.requestPreparationCancellation(ctx, move, reason)
	}
	if placement == nil || placement.Spec.NodeName == "" || placement.DeletionTimestamp != nil {
		return r.preparationWait(ctx, move, "WaitingForPreparedPlacement")
	}
	poolsOnNode, err := r.destinationPools(ctx, *move, placement.Spec.NodeName)
	if err != nil {
		return err
	}
	for _, pool := range poolsOnNode {
		granted, err := r.grantPreparation(ctx, move, pool, placement)
		if err != nil || granted {
			return err
		}
	}
	// This is a conclusive rejection, not a failed/ambiguous status write.
	// Do not keep scheduler resources on a node that granted no storage hold.
	if err := r.deletePlacement(ctx, *move); err != nil {
		return err
	}
	return r.preparationWait(ctx, move, "DestinationReservationLimit")
}

// This filter is advisory. A node must have one Pool that fits the whole
// Volume; fragments in different Pools cannot back a single PVC.
func (r *Reconciler) preparationCandidates(ctx context.Context, move volumeapi.Move, observed observation, pools poolIndex) ([]string, error) {
	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return nil, err
	}
	source, exists := indexedCopyPool(pools.registered, move.Status.SourceCopy)
	if !exists || observed.Volume.CapacityBytes <= 0 {
		return nil, fmt.Errorf("prepared source Pool or capacity is missing")
	}
	var candidates []string
	for _, node := range observed.CandidateNodes {
		for _, pool := range pools.ready {
			if pool.NodeName != node || pool.PoolGroup != source.PoolGroup ||
				volumeapi.PoolHasServingVolume(pool, move.Spec.VolumeID) {
				continue
			}
			fits, err := preparationPoolFits(pool, observed.Volume.CapacityBytes, volumes, moves)
			if err != nil {
				return nil, err
			}
			if fits {
				candidates = append(candidates, node)
				break
			}
		}
	}
	return candidates, nil
}

func preparationPoolFits(pool volumeapi.Pool, requested int64, volumes map[string]volumeapi.State, moves []volumeapi.Move) (bool, error) {
	limit, err := poolcapacity.LimitBytes(pool)
	if err != nil {
		return false, err
	}
	reserved, err := poolcapacity.ReservedBytesForPool(volumes, moves, pool.UID)
	if err != nil {
		return false, err
	}
	return requested > 0 && requested <= limit-reserved, nil
}

func (r *Reconciler) grantPreparation(ctx context.Context, move *volumeapi.Move, pool volumeapi.Pool, placement *corev1.Pod) (bool, error) {
	if r.PoolLocks == nil || placement.UID == "" || pool.UID == "" || pool.NodeName != placement.Spec.NodeName {
		return false, fmt.Errorf("preparation requires a capacity locker and exact placement Pod UID")
	}
	unlock, acquired := r.PoolLocks.TryLock(pool.UID)
	if !acquired {
		return false, errPoolBusy
	}
	defer unlock()
	currentPool, err := r.Repository.ReadyPoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName)
	if err != nil {
		return false, err
	}
	if valid, err := r.preparationPoolGroup(ctx, *move, currentPool); err != nil || !valid {
		return false, err
	}
	if err := r.preparationPlacementCurrent(ctx, *move, placement, pool.NodeName); err != nil {
		return false, err
	}
	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return false, err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return false, err
	}
	state := volumes[move.Spec.VolumeID]
	if err := preparationSourceAuthority(*move, state); err != nil {
		return false, err
	}
	if state.ActiveMove != move.Name || state.Phase != volumeapi.PhaseReady {
		return false, fmt.Errorf("preparation requires its Ready source claim")
	}
	if err := preparationMoveSnapshot(*move, moves); err != nil {
		return false, err
	}
	fits, err := preparationPoolFits(currentPool, state.CapacityBytes, volumes, moves)
	if err != nil || !fits {
		return false, err
	}
	previous := move.Status
	move.Status.Preparation = &volumeapi.MovePreparation{
		Pool:     volumeapi.PoolReference{Name: currentPool.Name, UID: currentPool.UID},
		NodeName: currentPool.NodeName, ReservedBytes: state.CapacityBytes,
	}
	move.Status.DestinationNode, move.Status.DestinationPoolUID = currentPool.NodeName, currentPool.UID
	move.Status.PlacementPodUID = string(placement.UID)
	move.Status.CandidateNodes = []string{currentPool.NodeName}
	move.Status.Reason, move.Status.CapacityReason = "", ""
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Reconciler) preparationPlacementCurrent(ctx context.Context, move volumeapi.Move, expected *corev1.Pod, nodeName string) error {
	current, err := r.Client.CoreV1().Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current.UID != expected.UID || validatePlacementIdentity(current, move, namesFor(move.Name)) != nil ||
		current.DeletionTimestamp != nil || current.Spec.NodeName != nodeName || terminalPod(current) {
		return fmt.Errorf("preparation placement changed before reservation")
	}
	return nil
}

func preparationMoveSnapshot(expected volumeapi.Move, moves []volumeapi.Move) error {
	for _, current := range moves {
		if current.Name != expected.Name {
			continue
		}
		if current.UID != expected.UID || current.ResourceVersion != expected.ResourceVersion || current.Spec != expected.Spec ||
			current.Deleting || !slices.Contains(current.Finalizers, volumeapi.MoveProtectionFinalizer) || current.Status.Preparation != nil {
			return fmt.Errorf("%w: preparation Move changed before reservation", volumeapi.ErrStateConflict)
		}
		return nil
	}
	return fmt.Errorf("preparation Move is missing")
}

func (r *Reconciler) preparationPoolGroup(ctx context.Context, move volumeapi.Move, pool volumeapi.Pool) (bool, error) {
	source := move.Status.SourceCopy
	if source == nil {
		return false, fmt.Errorf("preparation source copy is missing")
	}
	sourcePool, err := r.Repository.PoolForIdentity(ctx, source.PoolName, source.PoolUID, source.NodeName)
	if err != nil {
		return false, err
	}
	return sourcePool.PoolGroup == pool.PoolGroup && !volumeapi.PoolHasServingVolume(pool, move.Spec.VolumeID), nil
}

func (r *Reconciler) finishPreparation(ctx context.Context, move *volumeapi.Move, observed observation) error {
	reason, err := r.validatePreparedPlacement(ctx, *move, observed)
	if err != nil {
		return err
	}
	if reason != "" {
		return r.requestPreparationCancellation(ctx, move, reason)
	}
	previous := move.Status
	move.Status.Phase, move.Status.Reason = string(fsm.PhaseStartingDisruption), ""
	// No effects in the same pass. A new observation must see this durable
	// intent before moving the Volume or submitting an eviction.
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) startPreparedDisruption(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.Volume.ActiveMove != move.Name || move.Status.Preparation == nil {
		return fmt.Errorf("disruption requires its source claim and durable preparation")
	}
	if observed.Replacement != nil && (observed.Replacement.Spec.NodeName != "" || !hasPlacementHold(observed.Replacement)) {
		return r.requestPreparationCancellation(ctx, move, "UnexpectedConsumerPlacement")
	}
	if !observed.FSM.SourceHealthy || observed.FSM.PreflightDeferred ||
		(observed.Consumer != nil && !observed.FSM.PreconditionsValid) {
		return r.requestPreparationCancellation(ctx, move, preparationReason(observed.FSM.UnsafeReason, "PreparationChanged"))
	}
	reason, err := r.validatePreparedPlacement(ctx, *move, observed)
	if err != nil {
		return err
	}
	if reason != "" {
		return r.requestPreparationCancellation(ctx, move, reason)
	}
	if observed.Volume.Phase == volumeapi.PhaseReady {
		next := observed.Volume
		next.Phase = volumeapi.PhaseMoving
		if err := r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, observed.Volume, next); err != nil {
			return err
		}
	}
	previous := move.Status
	if err := r.evictConsumer(ctx, move, observed); err != nil {
		return err
	}
	move.Status.Phase, move.Status.Reason = string(fsm.PhaseWaitingForUnpublish), ""
	return r.persistMoveStatus(ctx, move, previous)
}

func preparationReason(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}
