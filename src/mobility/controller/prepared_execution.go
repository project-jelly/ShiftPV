package controller

import (
	"context"
	"fmt"
	"slices"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// Prepared Moves join the copy FSM only after their durable preparation and
// source disruption stages. Legacy Moves keep their original entry path.
func (r *Reconciler) reconcilePreparedStage(ctx context.Context, move volumeapi.Move) (bool, error) {
	if legacyMoveProtocol(move) {
		return false, nil
	}
	phase := fsm.Phase(move.Status.Phase)
	switch phase {
	case fsm.PhaseCanceling, fsm.PhaseCancelled:
		return true, r.reconcileCancellation(ctx, move)
	case "", fsm.PhasePending, fsm.PhasePreparing, fsm.PhaseStartingDisruption:
		return true, r.reconcilePreparation(ctx, move)
	case fsm.PhaseLocking, fsm.PhaseEvicting:
		return true, fmt.Errorf("prepared Move cannot use legacy disruption phase %q", phase)
	}
	if err := volumeapi.ValidatePreparedMovePlan(move); err != nil {
		return true, err
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return true, err
	}
	if move.Status.CapacityApproved || !noDestinationEffectIntent(move) {
		return false, validatePreparedExecutionPlan(move)
	}
	if phase == fsm.PhaseBlocked {
		return true, r.requestPreparationCancellation(ctx, &move, "UnapprovedMoveBlocked")
	}
	if move.Status.Preparation == nil || !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		return true, fmt.Errorf("prepared execution requires its protected durable reservation")
	}
	switch phase {
	case fsm.PhaseWaitingForUnpublish, fsm.PhaseWaitingForReplacement, fsm.PhaseWaitingForDestination, fsm.PhaseWaitingForCapacity:
		if move.Deleting {
			return true, r.requestPreparationCancellation(ctx, &move, "MoveDeleting")
		}
		return false, nil
	default:
		return true, fmt.Errorf("prepared Move has no execution approval in phase %q", phase)
	}
}

func validatePreparedExecutionPlan(move volumeapi.Move) error {
	if move.Status.IncomingCopy == nil || move.Status.Preparation != nil || move.Status.PlacementPodUID == "" {
		return fmt.Errorf("prepared execution requires the complete approved disk plan")
	}
	return volumeapi.ValidateMoveTransactionIdentities(move, volumeapi.MoveDestinationAnchor{
		InstallationID: move.Status.SourceCopy.InstallationID, VolumeUID: move.Status.SourceCopy.VolumeUID,
		PoolName: move.Status.IncomingCopy.PoolName, PoolUID: move.Status.DestinationPoolUID, NodeName: move.Status.DestinationNode,
	})
}

// Before approval there are no destination disk effects. Conditions that no
// longer permit the prepared placement cancel that hold and restore the source.
func (r *Reconciler) preparedExecutionChanged(ctx context.Context, move volumeapi.Move, observed observation) (string, error) {
	if !preparedBindingMatches(move, observed.PV, observed.Claim) {
		return "VolumeBindingChanged", nil
	}
	if !observed.SourceCordoned {
		return "SourceNotCordoned", nil
	}
	if observed.FSM.SourceAuthorityInvalid || !observed.FSM.SourceHealthy || observed.FSM.PreflightDeferred || observed.FSM.UnsafeReason == "AdmissionNotEnabled" {
		return preparationReason(observed.FSM.UnsafeReason, "SourceUnavailable"), nil
	}
	if observed.Replacement != nil && (observed.Replacement.Spec.NodeName != "" || !hasPlacementHold(observed.Replacement)) {
		return "UnexpectedConsumerPlacement", nil
	}
	return r.validatePreparedPlacement(ctx, move, observed)
}
