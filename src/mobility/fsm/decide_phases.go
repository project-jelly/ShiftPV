package fsm

// This file holds the per-phase halves of Decide. Decide itself runs the three
// cross-phase preludes in their original order and then dispatches here; each
// helper starts where its old `case` arm started, with every guard kept in the
// order it was evaluated before.

// lifecycleGuard makes Completing answer only to durable completion evidence.
// Terminal history and recovery have already been routed by decideLifecycle.
func lifecycleGuard(current Phase, observation Observation) (Decision, bool, error) {
	if current == PhaseCompleting {
		if observation.CompletionReady {
			decision, err := transition(current, PhaseSucceeded, ActionMarkSucceeded, "")
			return decision, true, err
		}
		decision, err := transition(current, current, ActionWait, "CompletionAuthorityMismatch")
		return decision, true, err
	}
	return Decision{}, false, nil
}

// authorityGuard fails the transaction closed when the source can no longer
// back it: a revoked authority always, a sick source only while authority has
// not yet moved to the destination.
func authorityGuard(current Phase, observation Observation) (Decision, bool, error) {
	if observation.SourceAuthorityInvalid {
		return blockedWith(observation, "SourceAuthorityInvalid"), true, nil
	}
	if beforeCommit(current) && !observation.OwnerCommitted && !observation.SourceHealthy {
		return blockedWith(observation, "SourceUnavailable"), true, nil
	}
	return Decision{}, false, nil
}

func decideWaitingForUnpublish(current Phase, observation Observation) (Decision, error) {
	if observation.PublishedOnSource {
		return transition(current, current, ActionWait, "")
	}
	return transition(current, PhaseWaitingForReplacement, ActionWait, "")
}

func decideWaitingForReplacement(current Phase, observation Observation) (Decision, error) {
	if !observation.ReplacementExists {
		return transition(current, current, ActionWait, "")
	}
	if !observation.ReplacementHeld {
		return blockedWith(observation, "PlacementHoldLost"), nil
	}
	return transition(current, PhaseWaitingForDestination, ActionEnsurePlacement, "")
}

func decideWaitingForDestination(current Phase, observation Observation) (Decision, error) {
	// Intentionally not destinationGuard: no placement exists yet, so an
	// unavailable destination is not a wait condition here and a blocked one
	// reports DestinationUnavailable rather than InvalidDestination.
	if observation.DestinationBlocked {
		return blockedWith(observation, "DestinationUnavailable"), nil
	}
	if !observation.ReplacementExists {
		return transition(current, current, ActionWait, "")
	}
	if !observation.ReplacementHeld {
		return blockedWith(observation, "PlacementHoldLost"), nil
	}
	if !observation.DestinationScheduled {
		return transition(current, current, ActionEnsurePlacement, "")
	}
	if observation.DestinationUnavailable {
		return transition(current, PhaseWaitingForCapacity, ActionWait, "DestinationUnavailable")
	}
	// Approval consumes a freshly observed WaitingForCapacity reservation.
	return transition(current, PhaseWaitingForCapacity, ActionWait, "")
}

func decideWaitingForCapacity(current Phase, observation Observation) (Decision, error) {
	if decision, guarded, err := destinationGuard(current, observation); guarded {
		return decision, err
	}
	if observation.CapacityBlocked {
		return blockedWith(observation, "DestinationCapacityInsufficient"), nil
	}
	if observation.CapacityApproved {
		return transition(current, PhaseCopying, ActionEnsureCopy, "")
	}
	return transition(current, current, ActionEnsureCapacity, "")
}

func decideCopying(current Phase, observation Observation) (Decision, error) {
	if decision, guarded, err := destinationGuard(current, observation); guarded {
		return decision, err
	}
	if observation.CopyFailed {
		return blockedWith(observation, "CopyFailed"), nil
	}
	if decision, guarded, err := placementGuard(current, observation); guarded {
		return decision, err
	}
	if observation.CopyComplete {
		return transition(current, PhasePromoting, ActionEnsurePromotion, "")
	}
	return transition(current, current, ActionEnsureCopy, "")
}

func decidePromoting(current Phase, observation Observation) (Decision, error) {
	if decision, guarded, err := destinationGuard(current, observation); guarded {
		return decision, err
	}
	if observation.PromotionFailed {
		return blockedWith(observation, "PromotionFailed"), nil
	}
	if decision, guarded, err := placementGuard(current, observation); guarded {
		return decision, err
	}
	if observation.PromotionComplete {
		return transition(current, PhaseCommitting, ActionCommitOwner, "")
	}
	return transition(current, current, ActionEnsurePromotion, "")
}

func decideCommitting(current Phase, observation Observation) (Decision, error) {
	if observation.OwnerCommitted {
		// Authority already moved, so a blocked destination can no longer
		// invalidate it; only an unavailable one delays releasing the hold.
		if observation.DestinationUnavailable {
			return transition(current, current, ActionWait, "DestinationUnavailable")
		}
		return transition(current, PhaseReleasingDestination, ActionDeletePlacement, "")
	}
	if decision, guarded, err := destinationGuard(current, observation); guarded {
		return decision, err
	}
	if decision, guarded, err := placementGuard(current, observation); guarded {
		return decision, err
	}
	return transition(current, current, ActionCommitOwner, "")
}

func decideReleasingDestination(current Phase, observation Observation) (Decision, error) {
	if observation.DestinationUnavailable {
		return transition(current, current, ActionWait, "DestinationUnavailable")
	}
	if observation.PlacementExists {
		return transition(current, current, ActionDeletePlacement, "")
	}
	if observation.ReplacementExists && observation.ReplacementHeld {
		return transition(current, PhaseWaitingForDestinationPublish, ActionReleasePlacement, "")
	}
	return transition(current, PhaseWaitingForDestinationPublish, ActionWait, "")
}

func decideWaitingForDestinationPublish(current Phase, observation Observation) (Decision, error) {
	if observation.CleanupFailed {
		return blockedWith(observation, "CleanupFailed"), nil
	}
	if observation.DestinationUnavailable {
		return transition(current, current, ActionWait, "DestinationUnavailable")
	}
	if !observation.PublishedOnDestination {
		return transition(current, current, ActionWait, "")
	}
	return transition(current, PhaseCleaningSource, ActionEnsureCleanup, "")
}

func decideCleaningSource(current Phase, observation Observation) (Decision, error) {
	if observation.DestinationUnavailable {
		return transition(current, current, ActionWait, "DestinationUnavailable")
	}
	if observation.CleanupFailed {
		return blockedWith(observation, "CleanupFailed"), nil
	}
	if observation.CleanupComplete {
		return transition(current, PhaseCompleting, ActionConfirmCleanup, "")
	}
	return transition(current, current, ActionEnsureCleanup, "")
}
