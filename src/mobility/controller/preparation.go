package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Preparation observes only the prerequisites needed before disruption. The
// decision model chooses the phase; these observations grant no disk authority.
func (r *Reconciler) observePreparation(ctx context.Context, move volumeapi.Move) (observation, error) {
	result := observation{Names: namesFor(move.Name)}
	if move.Status.CapacityApproved || !noDestinationEffectIntent(move) || move.Status.CleanupPhase != "" || move.Status.RollbackRequiredGeneration != 0 {
		return result, fmt.Errorf("preparation cannot run with destination execution authority")
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return result, err
	}
	if move.Deleting {
		result.FSM.PreparationChangedReason = "MoveDeleting"
		return result, nil
	}
	if !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		return result, nil
	}
	probe := move
	probe.Status.CandidateNodes = nil // Eligibility is live until disruption.
	pools, _, err := r.observeEligibility(ctx, probe, &result)
	if err != nil {
		return result, err
	}
	result.Pools = pools
	if err := preparationSourceAuthority(move, result.Volume); err != nil {
		return result, err
	}
	return r.observePreparationPlan(ctx, move, result)
}

func (r *Reconciler) observePreparationPlan(ctx context.Context, move volumeapi.Move, result observation) (observation, error) {
	if !result.SourceCordoned {
		result.FSM.PreparationChangedReason = "SourceNotCordoned"
		return result, nil
	}
	if move.Status.SourceCopy == nil {
		if result.FSM.SourceAuthorityInvalid {
			result.FSM.PreparationChangedReason = preparationReason(result.FSM.UnsafeReason, "VolumeBindingChanged")
		}
		return result, nil
	}
	if !preparedBindingMatches(move, result.PV, result.Claim) {
		result.FSM.PreparationChangedReason = "VolumeBindingChanged"
		return result, nil
	}
	if move.Status.Phase != string(fsm.PhaseStartingDisruption) && (result.Consumer == nil || result.Consumer.DeletionTimestamp != nil) {
		result.FSM.PreparationChangedReason = "ConsumerChanged"
		return result, nil
	}
	if move.Status.Preparation == nil {
		if preparationDeferred(result) {
			if err := r.observePlacement(ctx, move, &result); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	if result.Volume.ActiveMove != move.Name {
		return result, fmt.Errorf("preparation reservation lost its source claim")
	}
	if result.Replacement != nil && (result.Replacement.Spec.NodeName != "" || !hasPlacementHold(result.Replacement)) {
		result.FSM.PreparationChangedReason = "UnexpectedConsumerPlacement"
		return result, nil
	}
	if !result.FSM.SourceHealthy || result.FSM.PreflightDeferred ||
		(result.Consumer != nil && !result.FSM.PreconditionsValid) {
		result.FSM.PreparationChangedReason = preparationReason(result.FSM.UnsafeReason, "PreparationChanged")
		return result, nil
	}
	reason, err := r.validatePreparedPlacement(ctx, move, result)
	result.FSM.PreparationChangedReason = reason
	return result, err
}

func preparationDeferred(observed observation) bool {
	return !observed.FSM.SourceHealthy || observed.FSM.PreflightDeferred || !observed.FSM.PreconditionsValid
}

func (r *Reconciler) claimPreparationSource(ctx context.Context, move volumeapi.Move, state volumeapi.State) error {
	next := state
	next.ActiveMove = move.Name
	return r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, state, next)
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
		return preparationChanged(preparationReason(observed.FSM.UnsafeReason, "VolumeBindingChanged"))
	}
	if !observed.FSM.PreconditionsValid || observed.FSM.PreflightDeferred {
		return errLifecyclePending
	}
	if observed.PV == nil || observed.PV.UID == "" || observed.Claim == nil || observed.Claim.UID == "" ||
		observed.Consumer == nil || observed.Consumer.UID == "" || observed.Consumer.DeletionTimestamp != nil {
		return fmt.Errorf("preparation requires exact live binding and consumer identities")
	}
	source := *observed.Volume.CurrentCopy
	move.Status.SourceCopy = &source
	move.Status.PersistentVolumeName, move.Status.PersistentVolumeUID = observed.PV.Name, string(observed.PV.UID)
	move.Status.ClaimNamespace, move.Status.ClaimName, move.Status.ClaimUID = observed.Claim.Namespace, observed.Claim.Name, string(observed.Claim.UID)
	move.Status.ConsumerName, move.Status.ConsumerUID = observed.Consumer.Name, string(observed.Consumer.UID)
	move.Status.CandidateNodes = append([]string(nil), observed.CandidateNodes...)
	return nil
}

func (r *Reconciler) prepareDestination(ctx context.Context, move *volumeapi.Move, observed observation, pools poolIndex) error {
	candidates, unavailable, err := r.preparationCandidates(ctx, *move, observed, pools)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		if err := r.deletePlacement(ctx, *move); err != nil {
			return err
		}
		move.Status.Reason = preparationCapacityReason(unavailable)
		return nil
	}
	placement, reason, err := r.ensurePreparedPlacement(ctx, *move, observed, candidates)
	if err != nil {
		return err
	}
	if reason != "" {
		return preparationChanged(reason)
	}
	if placement == nil || placement.Spec.NodeName == "" || placement.DeletionTimestamp != nil {
		move.Status.Reason = "WaitingForPreparedPlacement"
		return nil
	}
	poolsOnNode, err := r.destinationPools(ctx, *move, placement.Spec.NodeName)
	if err != nil {
		return err
	}
	unavailable = false // Diagnose the fresh grant snapshot, not the advisory filter.
	for _, pool := range poolsOnNode {
		granted, err := r.grantPreparation(ctx, move, pool, placement)
		if errors.Is(err, ErrCapacityProbePending) {
			unavailable = true
			continue
		}
		if err != nil || granted {
			return err
		}
	}
	// This is a conclusive rejection, not a failed/ambiguous status write.
	// Do not keep scheduler resources on a node that granted no storage hold.
	if err := r.deletePlacement(ctx, *move); err != nil {
		return err
	}
	move.Status.Reason = preparationCapacityReason(unavailable)
	return nil
}

// This filter is advisory. A node must have one Pool that fits the whole
// Volume; fragments in different Pools cannot back a single PVC.
func (r *Reconciler) preparationCandidates(ctx context.Context, move volumeapi.Move, observed observation, pools poolIndex) ([]string, bool, error) {
	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return nil, false, err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return nil, false, err
	}
	source, exists := indexedCopyPool(pools.registered, move.Status.SourceCopy)
	if !exists || observed.Volume.CapacityBytes <= 0 {
		return nil, false, fmt.Errorf("prepared source Pool or capacity is missing")
	}
	var candidates []string
	unavailable := false
	for _, node := range observed.CandidateNodes {
		for _, pool := range pools.ready {
			if pool.NodeName != node || pool.PoolGroup != source.PoolGroup ||
				volumeapi.PoolHasServingVolume(pool, move.Spec.VolumeID) {
				continue
			}
			fits, err := preparationPoolFits(pool, observed.Volume.CapacityBytes, volumes, moves)
			if err != nil {
				return nil, false, err
			}
			if fits {
				if !pool.Status.CapacityProbeSupported {
					unavailable = true
					continue
				}
				candidates = append(candidates, node)
				break
			}
		}
	}
	return candidates, unavailable, nil
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
	if !currentPool.Status.CapacityProbeSupported {
		return false, ErrCapacityProbePending
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

// The StartingDisruption intent was observed before this effect. Revalidate
// placement and capacity at the effect boundary without selecting a new phase.
func (r *Reconciler) startPreparedDisruption(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.Volume.ActiveMove != move.Name || move.Status.Preparation == nil {
		return fmt.Errorf("disruption requires its source claim and durable preparation")
	}
	reason, err := r.validatePreparedPlacement(ctx, *move, observed)
	if err != nil {
		return err
	}
	if reason != "" {
		return preparationChanged(reason)
	}
	if observed.Volume.Phase == volumeapi.PhaseReady {
		if err := r.startPreparedSource(ctx, *move, observed.Volume); err != nil {
			return err
		}
	}
	return r.evictConsumer(ctx, move, observed)
}

func preparationReason(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}
