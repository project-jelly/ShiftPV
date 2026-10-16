package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

var errLifecyclePending = errors.New("lifecycle action requires re-observation")

type preparationChanged string

func (p preparationChanged) Error() string { return string(p) }

func lifecycleState(move volumeapi.Move) fsm.State {
	return fsm.State{Phase: fsm.Phase(move.Status.Phase), RecoveryPhase: move.Status.RecoveryPhase}
}

func lifecycleFacts(move volumeapi.Move, observed *observation) {
	facts := &observed.FSM
	facts.ProtectionPresent = slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer)
	facts.SourcePlanRecorded = move.Status.SourceCopy != nil
	facts.SourceClaimed = observed.Volume.ActiveMove == move.Name
	facts.PreparationReserved = move.Status.Preparation != nil
	facts.DestinationIntentRecorded = !noDestinationEffectIntent(move)
	facts.CapacityApproved = move.Status.CapacityApproved
	facts.SourceMoving = observed.Volume.Phase == volumeapi.PhaseMoving
	facts.ConsumerExists = observed.Consumer != nil
	facts.ReplacementExists = observed.Replacement != nil
	facts.ReplacementHeld = observed.Replacement != nil && hasPlacementHold(observed.Replacement) && observed.Replacement.Spec.NodeName == ""
	facts.RecoveryRequested = move.Spec.Recovery == "ResumeOwner"
	facts.RecoveryOwnerIsDestination = observed.Volume.OwnerNode != "" && observed.Volume.OwnerNode == move.Status.DestinationNode
}

func preparationLifecycle(move volumeapi.Move) bool {
	switch fsm.Phase(move.Status.Phase) {
	case "", fsm.PhasePending, fsm.PhasePreparing, fsm.PhaseStartingDisruption, fsm.PhaseCanceling, fsm.PhaseCancelled:
		return true
	default:
		return false
	}
}

// Recovery observations are needed only after preparation can no longer be
// cancelled. An unapproved Blocked record must first fence its Move writer;
// missing recovery prerequisites cannot prevent that intent from being saved.
func needsRecoveryObservation(move volumeapi.Move) bool {
	if move.Status.Phase != string(fsm.PhaseBlocked) || move.Spec.Recovery != "ResumeOwner" || move.Status.RecoveryPhase == recoveryRecovered {
		return false
	}
	return move.Status.CapacityApproved ||
		!noDestinationEffectIntent(move) || move.Status.RecoveryPhase != ""
}

// Reconcile observes only the facts needed by the persisted lifecycle stage.
// Every action receives its next state from the same pure decision model.
func (r *Reconciler) reconcileMove(ctx context.Context, move volumeapi.Move) error {
	var usageErr error
	if needsRecoveryObservation(move) {
		_, usageErr = r.finalizeSourceUsage(ctx, move)
		if ctx.Err() != nil {
			return errors.Join(usageErr, ctx.Err())
		}
	}
	observed, err := r.observeLifecycle(ctx, move)
	if err != nil {
		if preparationLifecycle(move) {
			return err
		}
		if move.Status.Phase == string(fsm.PhaseBlocked) && move.Spec.Recovery == "ResumeOwner" {
			reason := "ObservationFailed"
			var observationErr recoveryObservationError
			if errors.As(err, &observationErr) {
				reason = observationErr.reason
			}
			return errors.Join(r.recoveryError(ctx, move, reason, err), usageErr)
		}
		return r.recordMoveError(ctx, &move, move.Status, "ObservationFailed", "failed to observe Kubernetes state", err)
	}
	lifecycleFacts(move, &observed)
	return errors.Join(r.advanceMove(ctx, &move, observed), usageErr)
}

func (r *Reconciler) observeLifecycle(ctx context.Context, move volumeapi.Move) (observation, error) {
	phase := fsm.Phase(move.Status.Phase)
	switch {
	case phase == fsm.PhaseSucceeded, phase == fsm.PhaseCancelled,
		phase == fsm.PhaseBlocked && move.Status.RecoveryPhase == recoveryRecovered:
		if phase == fsm.PhaseCancelled {
			return r.observeCancellation(ctx, move)
		}
		return observation{}, nil
	case phase == fsm.PhaseCanceling:
		return r.observeCancellation(ctx, move)
	case phase == fsm.PhaseBlocked:
		if needsRecoveryObservation(move) {
			return r.observeRecovery(ctx, move)
		}
		return observation{}, nil
	}
	switch phase {
	case "", fsm.PhasePending, fsm.PhasePreparing, fsm.PhaseStartingDisruption:
		return r.observePreparation(ctx, move)
	}
	if err := validatePreparedStage(move); err != nil {
		return observation{}, err
	}
	observed, err := r.observe(ctx, move)
	if err == nil && !move.Status.CapacityApproved && noDestinationEffectIntent(move) {
		if move.Deleting {
			observed.FSM.PreparationChangedReason = "MoveDeleting"
		} else {
			observed.FSM.PreparationChangedReason, err = r.preparedExecutionChanged(ctx, move, observed)
		}
	}
	return observed, err
}

func (r *Reconciler) advanceMove(ctx context.Context, move *volumeapi.Move, observed observation) error {
	previous := move.Status
	decision, err := fsm.Decide(lifecycleState(*move), observed.FSM)
	if err != nil {
		if preparationLifecycle(*move) {
			return err
		}
		return r.recordMoveError(ctx, move, previous, "ActionFailed", "failed to decide the next mobility action", err)
	}
	if decision.Next.Phase == fsm.PhaseCanceling {
		if err := cancellablePreparation(*move); err != nil {
			return err
		}
	}
	version := move.ResourceVersion
	done, err := r.executeLifecycle(ctx, move, observed, decision)
	if err != nil {
		var changed preparationChanged
		if errors.As(err, &changed) {
			return r.requestPreparationCancellation(ctx, move, string(changed))
		}
		if errors.Is(err, errCapacityApprovalPersistence) || errors.Is(err, errMoveStatusPersistence) || preparationLifecycle(*move) {
			return err
		}
		if move.Status.Phase == string(fsm.PhaseBlocked) && move.Spec.Recovery == "ResumeOwner" {
			reason := "RecoveryStepFailed"
			if errors.Is(err, errRecoveryCleanupNeedsReview) {
				reason = "CleanupNeedsReview"
			}
			return r.recoveryError(ctx, *move, reason, err)
		}
		return r.recordMoveError(ctx, move, previous, "ActionFailed", "failed to execute the current mobility action", err)
	}
	if !done {
		return nil
	}
	// A narrow evidence CAS may already have advanced RV under a Pool lock.
	// Apply only the remaining lifecycle delta to that accepted version.
	if move.ResourceVersion != version {
		previous = move.Status
	}
	r.applyMoveDecision(move, decision)
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) applyMoveDecision(move *volumeapi.Move, decision fsm.Decision) {
	before := lifecycleState(*move)
	move.Status.Phase, move.Status.RecoveryPhase = string(decision.Next.Phase), decision.Next.RecoveryPhase
	if before.RecoveryPhase != decision.Next.RecoveryPhase {
		move.Status.RecoveryReason, move.Status.RecoveryMessage = "", ""
		move.Status.Message = recoveryPhaseMessage(move.Status.RecoveryPhase, move.Status.RecoveryOwner)
		return
	}
	if before.Phase == fsm.PhaseBlocked && decision.Next.Phase == fsm.PhaseBlocked && move.Spec.Recovery == "ResumeOwner" {
		return
	}
	if before.Phase == fsm.PhaseCanceling || before.Phase == fsm.PhaseCancelled {
		// Settlement preserves why cancellation began, including its terminal
		// transition. The next phase updates the message, not that cause.
		if before.Phase != decision.Next.Phase {
			move.Status.Message = mobilityMessage(decision.Next.Phase, move.Status.Reason)
		}
		return
	}
	if decision.Action != fsm.ActionReservePreparation || decision.Reason != "" {
		move.Status.Reason = decision.Reason
	}
	move.Status.Message = mobilityMessage(decision.Next.Phase, move.Status.Reason)
}

func (r *Reconciler) requestPreparationCancellation(ctx context.Context, move *volumeapi.Move, reason string) error {
	if err := cancellablePreparation(*move); err != nil {
		return err
	}
	if move.Status.Phase == string(fsm.PhaseCanceling) || move.Status.Phase == string(fsm.PhaseCancelled) {
		return nil
	}
	observed := observation{}
	lifecycleFacts(*move, &observed)
	observed.FSM.PreparationChangedReason = reason
	return r.advanceMove(ctx, move, observed)
}

func actionComplete(err error) (bool, error) {
	if errors.Is(err, errLifecyclePending) || errors.Is(err, errCleanupPending) || errors.Is(err, errSourceUsagePending) {
		return false, nil
	}
	return err == nil, err
}

func (r *Reconciler) executeLifecycle(ctx context.Context, move *volumeapi.Move, observed observation, decision fsm.Decision) (bool, error) {
	switch decision.Action {
	case fsm.ActionProtectMove:
		return false, r.Repository.AddMoveFinalizer(ctx, move.Name, move.UID)
	case fsm.ActionInitializePreparation:
		return actionComplete(r.initializePreparation(ctx, move, observed))
	case fsm.ActionClaimSource:
		return false, r.claimPreparationSource(ctx, *move, observed.Volume)
	case fsm.ActionReservePreparation:
		return actionComplete(r.prepareDestination(ctx, move, observed, observed.Pools))
	case fsm.ActionStartDisruption:
		return actionComplete(r.startPreparedDisruption(ctx, move, observed))
	case fsm.ActionRestoreSource:
		next := observed.Volume
		next.Phase = volumeapi.PhaseReady
		return false, r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, observed.Volume, next)
	case fsm.ActionReleasePreparation:
		move.Status.Preparation = nil
		move.Status.CapacityReason = "PreparationCancelled"
		return true, nil
	case fsm.ActionReleaseSource:
		next := observed.Volume
		next.ActiveMove = ""
		return false, r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, observed.Volume, next)
	case fsm.ActionDeletePlacement:
		if decision.Next.Phase == fsm.PhaseCanceling || decision.Next.Phase == fsm.PhaseCancelled {
			return false, r.deleteCancelledPlacement(ctx, *move, observed.Placement)
		}
	case fsm.ActionFinalizeHistory:
		if moveCleanupPending(*move) {
			return false, r.settleTerminalCleanup(ctx, *move)
		}
		if fsm.Phase(move.Status.Phase) != fsm.PhaseCancelled && !volumeapi.MoveCleanupSettled(*move) {
			return false, fmt.Errorf("retain terminal move: cleanup and capacity are not settled")
		}
		return false, r.reconcileTerminalMove(ctx, *move)
	case fsm.ActionFinalizeSourceUsage:
		_, err := r.finalizeSourceUsage(ctx, *move)
		return false, err
	case fsm.ActionBeginRecovery, fsm.ActionQuiesceRecovery, fsm.ActionVerifyRecovery,
		fsm.ActionRetireRecovery, fsm.ActionResumeRecovery, fsm.ActionCompleteRecovery:
		return r.executeRecovery(ctx, move, observed, decision.Action)
	}
	return actionComplete(r.execute(ctx, move, observed, decision))
}
