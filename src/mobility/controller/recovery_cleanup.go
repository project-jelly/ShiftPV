package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const recoveryCapacitySettled = "RecoverySettled"

var errRecoveryCleanupNeedsReview = errors.New("recovery cleanup needs review")

// settleRecoveryArtifacts is the only recovery step allowed to release a
// Move's temporary capacity hold. Source-owner recovery rolls back the one
// exact destination transaction artifact, while destination-owner recovery
// finishes the ordinary retained-source cleanup. A durable Retiring phase and
// a generation-fenced Pool inventory make the already-absent case safe without
// inventing a destructive cleanup receipt.
func (r *Reconciler) settleRecoveryArtifacts(ctx context.Context, move *volumeapi.Move, state volumeapi.State) (bool, error) {
	if move.Status.CapacityReason == recoveryCapacitySettled {
		if move.Status.CapacityApproved {
			return false, needsRecoveryCleanupReview("capacity hold is marked settled but remains approved")
		}
		return true, nil
	}
	var settled bool
	var err error
	switch state.OwnerNode {
	case move.Spec.SourceNode:
		settled, err = r.settleSourceRollback(ctx, move, state)
	case move.Status.DestinationNode:
		settled, err = r.settleDestinationCleanup(ctx, move, state)
	default:
		return false, needsRecoveryCleanupReview("authoritative owner is neither the source nor recorded destination")
	}
	if err != nil || !settled {
		return false, err
	}

	previous := move.Status
	move.Status.CapacityApproved = false
	move.Status.CapacityReason = recoveryCapacitySettled
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		return false, err
	}
	// Force a read-back boundary between releasing the hold and opening the
	// Volume mount guard. This also makes controller restarts harmless here.
	return false, nil
}

// settleSourceRollback settles precommit recovery, where the source is still
// authoritative. It proves the exact source authority the abort returns to and
// then rolls back the one destination transaction artifact, if any was ever
// written. It reports whether the hold may now be released.
func (r *Reconciler) settleSourceRollback(ctx context.Context, move *volumeapi.Move, state volumeapi.State) (bool, error) {
	if state.Phase != volumeapi.PhaseBlocked || state.ActiveMove != move.Name || state.CurrentCopy == nil ||
		state.CurrentCopy.Validate() != nil || state.CurrentCopy.Role != volume.RoleServing ||
		state.CurrentCopy.NodeName != move.Spec.SourceNode || state.CurrentCopy.VolumeID != move.Spec.VolumeID || state.CurrentCopy.VolumeUID != state.UID {
		return false, needsRecoveryCleanupReview("precommit source authority is incomplete")
	}
	if noDestinationEffectIntent(*move) {
		if move.Status.CapacityApproved {
			return false, needsRecoveryCleanupReview("approved destination hold has no exact artifact identities")
		}
		return true, nil
	}
	if move.Status.SourceCopy == nil || *state.CurrentCopy != *move.Status.SourceCopy {
		return false, needsRecoveryCleanupReview("recorded source identity differs from current authority")
	}
	if ready, err := r.ensureRollbackScan(ctx, move); err != nil || !ready {
		return false, err
	}
	target, present, err := r.rollbackArtifact(ctx, *move)
	if err != nil {
		return false, err
	}
	if !present {
		return r.settledRollbackJournal(ctx, *move)
	}
	spec := cleanupapi.Spec{
		OperationID: volumeapi.MoveRollbackOperationID(move.UID),
		Target:      target,
		Reason:      "MoveRollback",
		Authority:   cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID},
	}
	complete, err := r.ensureRecoveryCleanup(ctx, *move, spec)
	if err != nil || !complete {
		return false, err
	}
	return true, nil
}

// settleDestinationCleanup settles postcommit recovery, where the destination
// is authoritative and recovery only converges forward. Actual destination
// publication must be proven by both the Volume state and a Ready Pool scan
// before the ordinary retained-source cleanup may run. It reports whether the
// hold may now be released.
func (r *Reconciler) settleDestinationCleanup(ctx context.Context, move *volumeapi.Move, state volumeapi.State) (bool, error) {
	if state.Phase != volumeapi.PhaseReady || state.ActiveMove != move.Name || move.Status.DestinationCopy == nil ||
		state.CurrentCopy == nil || *state.CurrentCopy != *move.Status.DestinationCopy || state.UID != move.Status.DestinationCopy.VolumeUID ||
		!slices.Contains(state.PublishedNodes, move.Status.DestinationNode) || slices.Contains(state.PublishedNodes, move.Spec.SourceNode) {
		return false, fmt.Errorf("waiting for exact destination publication before source cleanup")
	}
	// The read failure and the ordinary not-yet-published wait are separate
	// facts. Only the first one carries a cause, so only the first one wraps.
	destinationPool, err := r.Repository.ReadyPoolForNode(ctx, move.Status.DestinationNode)
	if err != nil {
		return false, fmt.Errorf("waiting for exact destination scanner publication before source cleanup: %w", err)
	}
	if !volumeapi.PoolHasPublishedCopy(destinationPool, move.Status.DestinationCopy) {
		return false, fmt.Errorf("waiting for exact destination scanner publication before source cleanup")
	}
	spec, err := moveCleanupSpec(*move)
	if err != nil {
		return false, needsRecoveryCleanupReview("postcommit cleanup identity is incomplete: %v", err)
	}
	complete, err := r.ensureRecoveryCleanup(ctx, *move, spec)
	if err != nil || !complete {
		return false, err
	}
	return true, nil
}

// rollbackArtifact returns the only exact destination transaction artifact
// visible in an inventory from the requested post-Retiring generation. A
// valid, complete inventory containing neither identity proves that no cleanup
// effect is required. Any conflicting or problem observation stays untouched.
func (r *Reconciler) rollbackArtifact(ctx context.Context, move volumeapi.Move) (volume.CopyIdentity, bool, error) {
	destination, err := r.rollbackInventoryPool(ctx, move)
	if err != nil {
		return volume.CopyIdentity{}, false, err
	}
	return rollbackPresentArtifact(move, destination)
}

// rollbackInventoryPool resolves the one registered destination Pool whose
// identity still matches the approved hold and whose inventory is a fresh,
// complete observation from the requested post-Retiring generation. Nothing
// here inspects copies: it only decides which observation may be trusted.
func (r *Reconciler) rollbackInventoryPool(ctx context.Context, move volumeapi.Move) (*volumeapi.Pool, error) {
	if err := validRollbackIntent(move); err != nil {
		return nil, needsRecoveryCleanupReview("destination cleanup intent is incomplete: %v", err)
	}
	pools, err := r.Repository.Pools(ctx)
	if err != nil {
		return nil, err
	}
	destination, err := rollbackDestinationPool(move, pools)
	if err != nil {
		return nil, err
	}
	if err := r.rollbackInventoryFresh(destination, move.Status.RollbackRequiredGeneration); err != nil {
		return nil, err
	}
	return destination, nil
}

// rollbackDestinationPool resolves the one registered Pool on the destination
// node whose identity still matches every approved hold.
func rollbackDestinationPool(move volumeapi.Move, pools []volumeapi.Pool) (*volumeapi.Pool, error) {
	var destination *volumeapi.Pool
	for index := range pools {
		pool := &pools[index]
		if pool.NodeName != move.Status.DestinationNode {
			continue
		}
		if destination != nil {
			return nil, needsRecoveryCleanupReview("multiple destination Pools are registered")
		}
		destination = pool
	}
	if destination == nil || destination.Name != move.Status.IncomingCopy.PoolName || destination.UID != move.Status.DestinationPoolUID ||
		destination.UID != move.Status.IncomingCopy.PoolUID || destination.UID != move.Status.DestinationCopy.PoolUID {
		return nil, needsRecoveryCleanupReview("destination Pool identity changed")
	}
	if !slices.Contains(destination.Finalizers, volumeapi.PoolProtectionFinalizer) {
		return nil, needsRecoveryCleanupReview("destination Pool lacks lifecycle protection")
	}
	return destination, nil
}

// rollbackInventoryFresh reports whether the destination Pool is cleanup-ready
// and carries a fresh, complete inventory from the requested post-Retiring
// generation. A stale or partial observation is a wait, not a contradiction.
func (r *Reconciler) rollbackInventoryFresh(destination *volumeapi.Pool, requiredGeneration int64) error {
	if requiredGeneration <= 0 || destination.Generation < requiredGeneration || destination.Status.ObservedGeneration != destination.Generation {
		return fmt.Errorf("waiting for destination inventory at rollback scan generation %d", requiredGeneration)
	}
	now := r.now()
	staleAfter := r.PoolReadinessStaleAfter
	if staleAfter <= 0 {
		staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
	}
	if ready, reason := destination.CleanupReadyAt(now, staleAfter); !ready {
		return fmt.Errorf("destination Pool is not cleanup-ready: %s", reason)
	}
	inventory := destination.Status.Inventory
	if inventory == nil || !inventory.Valid || inventory.Truncated || inventory.Message != "" ||
		inventory.ObservedAt.IsZero() || now.Before(inventory.ObservedAt.Time) || now.Sub(inventory.ObservedAt.Time) > staleAfter {
		return fmt.Errorf("waiting for a fresh complete destination inventory at the rollback scan fence")
	}
	return nil
}

// rollbackPresentArtifact decides which exact transaction artifact, if any, the
// trusted inventory still shows on the destination. Anything ambiguous,
// conflicting, or already published keeps its data and goes to review instead.
func rollbackPresentArtifact(move volumeapi.Move, destination *volumeapi.Pool) (volume.CopyIdentity, bool, error) {
	inventory := destination.Status.Inventory
	incomingPresent, destinationPresent := false, false
	for _, observed := range inventory.Copies {
		if observed.Problem != "" {
			return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("destination inventory contains an ambiguous observation")
		}
		if !observed.Present {
			continue
		}
		if observed.Identity == nil {
			return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("destination inventory contains an unidentified artifact")
		}
		identity := *observed.Identity
		if identity.Validate() != nil || identity.PoolName != destination.Name || identity.PoolUID != destination.UID || identity.NodeName != destination.NodeName {
			return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("destination inventory contains an invalid copy identity")
		}
		switch identity {
		case *move.Status.IncomingCopy:
			incomingPresent = true
		case *move.Status.DestinationCopy:
			destinationPresent = true
		default:
			if identity.VolumeID == move.Spec.VolumeID || identity.VolumeUID == move.Status.SourceCopy.VolumeUID ||
				(identity.Role == volume.RoleIncoming && identity.CopyID == move.Status.IncomingCopy.CopyID) {
				return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("destination inventory contains a conflicting volume artifact")
			}
			// Other healthy volumes on the same Pool are outside this rollback.
			continue
		}
		if observed.Published {
			return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("destination inventory contains a published artifact")
		}
	}
	if incomingPresent && destinationPresent {
		return volume.CopyIdentity{}, false, needsRecoveryCleanupReview("both incoming and promoted destination artifacts are present")
	}
	if incomingPresent {
		return *move.Status.IncomingCopy, true, nil
	}
	if destinationPresent {
		return *move.Status.DestinationCopy, true, nil
	}
	return volume.CopyIdentity{}, false, nil
}

func noDestinationEffectIntent(move volumeapi.Move) bool {
	return move.Status.IncomingCopy == nil && move.Status.DestinationCopy == nil && move.Status.CopyJobName == "" &&
		move.Status.PromotionJobName == "" && move.Status.CopyOperationID == "" && move.Status.PromotionOperationID == ""
}

// validRollbackIntent rechecks the exact Move transaction a rollback cleanup may
// be derived from. Each sub-predicate names the term it owns, so a NeedsReview
// entry records which identity contradicted the durable intent.
func validRollbackIntent(move volumeapi.Move) error {
	if move.Status.RecoveryPhase != recoveryRetiring {
		return needsRecoveryCleanupReview("rollback scan requires durable Retiring intent")
	}

	if err := rollbackSourceIntent(move); err != nil {
		return err
	}
	if err := rollbackDestinationHold(move); err != nil {
		return err
	}
	if err := rollbackTransactionCopies(move); err != nil {
		return err
	}
	return rollbackExecutorIntent(move)
}

// rollbackSourceIntent requires the exact retained source copy the rollback
// returns authority to.
func rollbackSourceIntent(move volumeapi.Move) error {
	source := move.Status.SourceCopy
	switch {
	case move.Name == "":
		return volumeapi.MoveIdentityMismatch("Name")
	case source == nil || source.Validate() != nil:
		return volumeapi.MoveIdentityMismatch("SourceCopy")
	case source.Role != volume.RoleServing:
		return volumeapi.MoveIdentityMismatch("SourceCopy.Role")
	case source.NodeName != move.Spec.SourceNode:
		return volumeapi.MoveIdentityMismatch("SourceCopy.NodeName")
	case source.VolumeID != move.Spec.VolumeID:
		return volumeapi.MoveIdentityMismatch("SourceCopy.VolumeID")
	}
	return nil
}

// rollbackDestinationHold requires the approved destination reservation the
// transaction artifacts were allowed to be written under.
func rollbackDestinationHold(move volumeapi.Move) error {
	switch {
	case !move.Status.CapacityApproved:
		return volumeapi.MoveIdentityMismatch("CapacityApproved")
	case move.Status.SourceBytes <= 0:
		return volumeapi.MoveIdentityMismatch("SourceBytes")
	case move.Status.DestinationNode == "" || move.Status.DestinationNode == move.Spec.SourceNode:
		return volumeapi.MoveIdentityMismatch("DestinationNode")
	case move.Status.DestinationPoolUID == "":
		return volumeapi.MoveIdentityMismatch("DestinationPoolUID")
	}
	return nil
}

// rollbackTransactionCopies defers to the volumeapi rule that owns the exact
// copy and operation names. Rollback reads no destination Pool object, so the
// Pool name is anchored to the incoming copy and only has to agree across the
// two transaction copies; the Pool UID is still pinned to the approved hold.
func rollbackTransactionCopies(move volumeapi.Move) error {
	source, incoming := move.Status.SourceCopy, move.Status.IncomingCopy
	if incoming == nil {
		return volumeapi.MoveIdentityMismatch("IncomingCopy")
	}
	if move.Status.DestinationCopy == nil {
		return volumeapi.MoveIdentityMismatch("DestinationCopy")
	}
	return volumeapi.ValidateMoveTransactionIdentities(move, volumeapi.MoveDestinationAnchor{
		InstallationID: source.InstallationID, PoolName: incoming.PoolName, PoolUID: move.Status.DestinationPoolUID,
		NodeName: move.Status.DestinationNode, VolumeUID: source.VolumeUID,
	})
}

// rollbackExecutorIntent requires the deterministic Job names this Move owns,
// since a rollback artifact may only exist because one of them ran.
func rollbackExecutorIntent(move volumeapi.Move) error {
	names := namesFor(move.Name)
	switch {
	case move.Status.CopyJobName != names.CopyJob:
		return volumeapi.MoveIdentityMismatch("CopyJobName")
	case move.Status.PromotionJobName != "" && move.Status.PromotionJobName != names.PromotionJob:
		return volumeapi.MoveIdentityMismatch("PromotionJobName")
	}
	return nil
}

// settledRollbackJournal answers the already-absent case once a rollback
// journal exists. An absent artifact proves only that the purge effect ran; the
// journal still owns the API receipt and the post-receipt absence fence that
// close the operation. Declaring settlement here would release the hold and the
// parent finalizer while the journal is mid-flight, and ReconcileAll never
// returns a Recovered Move to recovery, so the journal would stay unfinished
// forever. Only a Move that never recorded an intent settles without one.
func (r *Reconciler) settledRollbackJournal(ctx context.Context, move volumeapi.Move) (bool, error) {
	if r.Cleanups == nil {
		return false, fmt.Errorf("recovery cleanup store is not configured")
	}
	current, err := r.Cleanups.Get(ctx, moveCleanupAuthority(move))
	if errors.Is(err, cleanupapi.ErrNoJournal) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if current.Status.Phase == cleanupapi.PhaseCompleted {
		return true, nil
	}
	if current.Spec.Reason != "MoveRollback" || current.Spec.OperationID != volumeapi.MoveRollbackOperationID(move.UID) {
		return false, r.reviewCleanupJournal(ctx, current, "CleanupIntentMismatch",
			fmt.Sprintf("rollback settlement found cleanup operation %q for reason %q", current.Spec.OperationID, current.Spec.Reason))
	}
	// Recovery is still Retiring here, so the hold is intact and an executor may
	// legitimately run; only a lost Pool identity is a contradiction.
	complete, err := r.ensureRecoveryCleanup(ctx, move, current.Spec)
	if contradictedCleanup(err) && !errors.Is(err, errRecoveryCleanupNeedsReview) {
		return false, r.reviewCleanupJournal(ctx, current, "CleanupPoolIdentityChanged", err.Error())
	}
	return complete, err
}

func needsRecoveryCleanupReview(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", errRecoveryCleanupNeedsReview, fmt.Sprintf(format, arguments...))
}

func (r *Reconciler) ensureRecoveryCleanup(ctx context.Context, move volumeapi.Move, spec cleanupapi.Spec) (bool, error) {
	if r.Cleanups == nil {
		return false, fmt.Errorf("recovery cleanup store is not configured")
	}
	err := r.ensureCleanupSpec(ctx, &move, spec)
	current, getErr := r.Cleanups.Get(ctx, spec.Authority)
	if getErr == nil && current.Status.Phase == cleanupapi.PhaseNeedsReview {
		return false, needsRecoveryCleanupReview("cleanup journal entered NeedsReview: %s: %s", current.Status.Reason, current.Status.Message)
	}
	if err != nil {
		if errors.Is(err, cleanupapi.ErrConflict) {
			return false, needsRecoveryCleanupReview("cleanup journal conflicts with the approved recovery target: %v", err)
		}
		return false, err
	}
	if getErr != nil {
		return false, getErr
	}
	return current.Status.Phase == cleanupapi.PhaseCompleted, nil
}
