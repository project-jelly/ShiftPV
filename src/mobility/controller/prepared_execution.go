package controller

import (
	"context"
	"fmt"
	"slices"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// Admission facts are checked before observing the execution graph. This does
// not dispatch a separate lifecycle or select a next phase.
func validatePreparedStage(move volumeapi.Move) error {
	if err := volumeapi.ValidatePreparedMovePlan(move); err != nil {
		return err
	}
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return err
	}
	if move.Status.CapacityApproved || !noDestinationEffectIntent(move) {
		return validatePreparedExecutionPlan(move)
	}
	if move.Status.Phase == string(fsm.PhaseBlocked) {
		return nil
	}
	if move.Status.Preparation == nil || !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		return fmt.Errorf("prepared execution requires its protected durable reservation")
	}
	switch fsm.Phase(move.Status.Phase) {
	case fsm.PhaseWaitingForUnpublish, fsm.PhaseWaitingForReplacement, fsm.PhaseWaitingForDestination, fsm.PhaseWaitingForCapacity:
		return nil
	default:
		return fmt.Errorf("prepared Move has no execution approval in phase %q", move.Status.Phase)
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

// Before approval, retain the disrupted workload's reservation across transient
// availability changes; confirmed identity changes are handled by the FSM.
func (r *Reconciler) preparedExecutionChanged(ctx context.Context, move volumeapi.Move, observed observation) (string, bool, error) {
	if !preparedBindingMatches(move, observed.PV, observed.Claim) {
		return "VolumeBindingChanged", false, nil
	}
	if observed.FSM.SourceAuthorityInvalid {
		return preparationReason(observed.FSM.UnsafeReason, "SourceAuthorityInvalid"), false, nil
	}
	if !observed.FSM.SourceHealthy {
		return preparationReason(observed.FSM.UnsafeReason, "SourceUnavailable"), true, nil
	}
	if !observed.SourceCordoned {
		return "SourceNotCordoned", false, nil
	}
	if observed.FSM.PreflightDeferred || observed.FSM.UnsafeReason == "AdmissionNotEnabled" {
		return preparationReason(observed.FSM.UnsafeReason, "PreparationUnavailable"), true, nil
	}
	if observed.Replacement != nil && (observed.Replacement.Spec.NodeName != "" || !hasPlacementHold(observed.Replacement)) {
		return "UnexpectedConsumerPlacement", false, nil
	}
	return r.validatePreparedPlacement(ctx, move, observed)
}
