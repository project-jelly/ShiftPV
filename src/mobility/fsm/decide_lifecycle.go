package fsm

import "fmt"

func validateState(current State, observation Observation) error {
	if !known(current.Phase) {
		return fmt.Errorf("unknown mobility phase %q", current.Phase)
	}
	if current.RecoveryPhase != "" {
		if current.Phase != PhaseBlocked || !knownRecovery(current.RecoveryPhase) {
			return fmt.Errorf("invalid recovery state %+v", current)
		}
	}
	switch current.Phase {
	case PhasePreparing, PhaseStartingDisruption, PhaseCanceling, PhaseCancelled:
		if observation.CapacityApproved || observation.DestinationIntentRecorded {
			return fmt.Errorf("phase %q cannot release destination execution authority", current.Phase)
		}
	}
	return validatePreparationEvidence(current.Phase, observation)
}

func validatePreparationEvidence(phase Phase, observation Observation) error {
	if phase == PhasePending && (observation.CapacityApproved || observation.DestinationIntentRecorded) {
		return fmt.Errorf("pending preparation cannot carry destination execution authority")
	}
	if phase == PhaseCancelled && observation.PreparationReserved {
		return fmt.Errorf("cancelled Move still holds its preparation reservation")
	}
	return nil
}

// A cancellation intent can fence its Move writer without a fresh Volume
// observation. Progressing preparation or settling that cancellation still
// requires the reserved source claim to be observed intact.
func validateReservedSource(observation Observation) error {
	if observation.PreparationReserved && (!observation.SourcePlanRecorded || !observation.SourceClaimed) {
		return fmt.Errorf("preparation reservation lost its recorded source claim")
	}
	return nil
}

// Lifecycle routing precedes execution health checks. Cancellation does not
// require a live workload binding, and terminal history cannot regain authority.
func decideLifecycle(current State, observation Observation) (Decision, bool, error) {
	if canRequestSettlement(current, observation) {
		return Decision{Next: State{Phase: PhaseBlocked}, Action: ActionRequestSettlement, Reason: "WorkloadRetired"}, true, nil
	}
	switch current.Phase {
	case PhaseSucceeded:
		return Decision{Next: current, Action: ActionFinalizeHistory}, true, nil
	case PhaseCanceling, PhaseCancelled:
		decision, err := decideCancellation(current, observation)
		return decision, true, err
	case PhaseBlocked:
		if current.RecoveryPhase == RecoveryRecovered {
			return Decision{Next: current, Action: ActionFinalizeHistory}, true, nil
		}
		if beforeExecutionApproval(observation) && current.RecoveryPhase == "" {
			decision, err := transition(current.Phase, PhaseCanceling, ActionWait, "UnapprovedMoveBlocked")
			return decision, true, err
		}
		if observation.RecoveryRequested {
			decision, err := decideRecovery(current, observation)
			return decision, true, err
		}
		return Decision{Next: current, Action: ActionFinalizeSourceUsage}, true, nil
	}
	if beforeExecutionApproval(observation) && observation.PreparationChangedReason != "" {
		decision, err := transition(current.Phase, PhaseCanceling, ActionWait, observation.PreparationChangedReason)
		return decision, true, err
	}
	switch current.Phase {
	case PhasePending, PhasePreparing:
		decision, err := decidePreparation(current.Phase, observation)
		return decision, true, err
	case PhaseStartingDisruption:
		decision, err := decideStartingDisruption(current.Phase, observation)
		return decision, true, err
	}
	return Decision{}, false, nil
}

func canRequestSettlement(current State, observation Observation) bool {
	if !observation.WorkloadRetired || current.RecoveryPhase == RecoveryRecovered || beforeExecutionApproval(observation) {
		return false
	}
	switch current.Phase {
	case PhaseSucceeded, PhaseCancelled, PhaseCanceling:
		return false
	}
	return true
}

func decidePreparation(current Phase, observation Observation) (Decision, error) {
	if err := validateReservedSource(observation); err != nil {
		return Decision{}, err
	}
	if !observation.ProtectionPresent {
		return transition(current, current, ActionProtectMove, "")
	}
	if observation.SourceAuthorityInvalid {
		return blockedWith(observation, "SourceAuthorityInvalid"), nil
	}
	if !observation.SourceHealthy || !observation.PreconditionsValid || observation.PreflightDeferred {
		if observation.PreparationReserved {
			return blockedWith(observation, "PreparationChanged"), nil
		}
		action := ActionWait
		if observation.PlacementExists {
			action = ActionDeletePlacement
		}
		return transition(current, current, action, reasonOr(observation.UnsafeReason, "PreparationDeferred"))
	}
	if !observation.SourcePlanRecorded {
		return transition(current, PhasePreparing, ActionInitializePreparation, "")
	}
	if current == PhasePending {
		return transition(current, PhasePreparing, ActionWait, "")
	}
	if !observation.SourceClaimed {
		return transition(current, current, ActionClaimSource, "")
	}
	if !observation.PreparationReserved {
		return transition(current, current, ActionReservePreparation, "")
	}
	// This is a durable intent transition; no eviction runs in this pass.
	return transition(current, PhaseStartingDisruption, ActionWait, "")
}

func decideStartingDisruption(current Phase, observation Observation) (Decision, error) {
	if !observation.ProtectionPresent || !observation.SourcePlanRecorded || !observation.SourceClaimed || !observation.PreparationReserved {
		return Decision{}, fmt.Errorf("disruption requires the protected plan, source claim and reservation")
	}
	if observation.SourceAuthorityInvalid || !observation.SourceHealthy || observation.PreflightDeferred ||
		(observation.ConsumerExists && !observation.PreconditionsValid) {
		return blockedWith(observation, "PreparationChanged"), nil
	}
	if observation.ReplacementExists && !observation.ReplacementHeld {
		return transition(current, PhaseCanceling, ActionWait, "UnexpectedConsumerPlacement")
	}
	return transition(current, PhaseWaitingForUnpublish, ActionStartDisruption, "")
}

func decideCancellation(current State, observation Observation) (Decision, error) {
	if err := validateReservedSource(observation); err != nil {
		return Decision{}, err
	}
	if current.Phase == PhaseCanceling && observation.SourcePlanRecorded && !observation.SourceClaimed {
		return Decision{Next: current, Action: ActionClaimSource}, nil
	}
	if observation.SourceClaimed && observation.SourceMoving {
		return Decision{Next: current, Action: ActionRestoreSource}, nil
	}
	if !observation.PlacementAbsent {
		return Decision{Next: current, Action: ActionDeletePlacement}, nil
	}
	if current.Phase == PhaseCanceling {
		// ReleasePreparation only stages hold removal. The controller applies
		// it together with Next in one CAS, before clearing the Volume claim.
		return transition(current.Phase, PhaseCancelled, ActionReleasePreparation, "")
	}
	if observation.SourceClaimed {
		return Decision{Next: current, Action: ActionReleaseSource}, nil
	}
	return Decision{Next: current, Action: ActionFinalizeHistory}, nil
}

func decideRecovery(current State, observation Observation) (Decision, error) {
	next := current
	action := ActionWait
	switch current.RecoveryPhase {
	case "":
		next.RecoveryPhase, action = RecoveryQuiescing, ActionBeginRecovery
	case RecoveryQuiescing:
		next.RecoveryPhase, action = RecoveryVerifying, ActionQuiesceRecovery
	case RecoveryVerifying:
		next.RecoveryPhase, action = RecoveryRetiring, ActionVerifyRecovery
		if observation.RecoveryOwnerIsDestination && !observation.SettlementOnly {
			next.RecoveryPhase = RecoveryResuming
		}
	case RecoveryRetiring:
		next.RecoveryPhase, action = RecoveryResuming, ActionRetireRecovery
		if observation.RecoveryOwnerIsDestination || observation.SettlementOnly {
			next.RecoveryPhase = RecoveryCompleting
		}
	case RecoveryResuming:
		next.RecoveryPhase, action = RecoveryCompleting, ActionResumeRecovery
		if observation.RecoveryOwnerIsDestination {
			next.RecoveryPhase = RecoveryRetiring
		}
	case RecoveryCompleting:
		next.RecoveryPhase, action = RecoveryRecovered, ActionCompleteRecovery
		if observation.RecoveryUnlockObserved {
			action = ActionWait
		}
	default:
		return Decision{}, fmt.Errorf("unknown recovery phase %q", current.RecoveryPhase)
	}
	return Decision{Next: next, Action: action}, nil
}

func knownRecovery(phase string) bool {
	switch phase {
	case RecoveryQuiescing, RecoveryVerifying, RecoveryRetiring, RecoveryResuming, RecoveryCompleting, RecoveryRecovered:
		return true
	default:
		return false
	}
}
