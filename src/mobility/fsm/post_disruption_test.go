package fsm

import "testing"

func TestTemporaryPreparationChangeWaitsAfterDisruptionIntentAndCanResume(t *testing.T) {
	tests := []struct {
		phase, resumed Phase
		action         Action
	}{
		{PhaseStartingDisruption, PhaseWaitingForUnpublish, ActionStartDisruption},
		{PhaseWaitingForUnpublish, PhaseWaitingForReplacement, ActionWait},
		{PhaseWaitingForReplacement, PhaseWaitingForDestination, ActionEnsurePlacement},
		{PhaseWaitingForDestination, PhaseWaitingForCapacity, ActionWait},
		{PhaseWaitingForCapacity, PhaseWaitingForCapacity, ActionEnsureCapacity},
	}
	for _, test := range tests {
		for _, reason := range []string{"DestinationNotReady", "DestinationCordoned", "DestinationNoSchedule"} {
			t.Run(string(test.phase)+"/"+reason, func(t *testing.T) {
				observed := preparedObservation()
				observed.ReplacementExists, observed.ReplacementHeld, observed.DestinationScheduled = true, true, true
				observed.PreparationChangedReason, observed.PreparationUnavailable = reason, true
				before := observed
				state := State{Phase: test.phase}
				// StartingDisruption is already a durable boundary: an eviction
				// may have succeeded even when no later write was observed.
				for range 3 {
					decision := assertDecision(t, state, observed, state, ActionWait)
					if decision.Reason != reason {
						t.Fatalf("temporary preparation diagnosis lost: %+v", decision)
					}
				}
				if observed != before {
					t.Fatal("waiting changed the source claim or preparation reservation")
				}
				observed.PreparationChangedReason, observed.PreparationUnavailable = "", false
				assertDecision(t, state, observed, State{Phase: test.resumed}, test.action)
			})
		}
	}
}

func TestPreparationChangeCanCancelOnlyBeforeDisruptionIntent(t *testing.T) {
	for _, phase := range []Phase{PhasePending, PhasePreparing} {
		for _, test := range []struct {
			name        string
			unavailable bool
		}{
			{"changed", false},
			{"temporarily unavailable", true},
		} {
			t.Run(string(phase)+"/"+test.name, func(t *testing.T) {
				observed := preparedObservation()
				observed.PreparationChangedReason = "DestinationChanged"
				observed.PreparationUnavailable = test.unavailable
				decision := assertDecision(t, State{Phase: phase}, observed, State{Phase: PhaseCanceling}, ActionWait)
				if decision.Reason != observed.PreparationChangedReason {
					t.Fatal("cancellation discarded the changed preparation diagnosis")
				}
			})
		}
	}
}

func TestPreparationIdentityChangeBlocksAfterDisruptionIntent(t *testing.T) {
	for _, phase := range []Phase{PhaseStartingDisruption, PhaseWaitingForUnpublish, PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseWaitingForCapacity} {
		for _, reason := range []string{"BindingChanged", "SourcePoolIdentityChanged", "PlacementChanged"} {
			t.Run(string(phase)+"/"+reason, func(t *testing.T) {
				observed := preparedObservation()
				observed.PreparationChangedReason = reason
				decision := assertDecision(t, State{Phase: phase}, observed, State{Phase: PhaseBlocked}, ActionMarkBlocked)
				if decision.Reason != reason {
					t.Fatalf("identity contradiction diagnosis lost: %+v", decision)
				}
			})
		}
	}
}

func TestTemporarySourceUnavailabilityKeepsDisruptedMove(t *testing.T) {
	for _, phase := range []Phase{PhaseStartingDisruption, PhaseWaitingForUnpublish, PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseWaitingForCapacity} {
		t.Run(string(phase), func(t *testing.T) {
			observed := preparedObservation()
			observed.SourceHealthy = false
			state := State{Phase: phase}
			assertDecision(t, state, observed, state, ActionWait)
			// A contradicted authority is different from a temporarily
			// unavailable source, even before execution was approved.
			observed.SourceAuthorityInvalid = true
			assertDecision(t, state, observed, State{Phase: PhaseBlocked}, ActionMarkBlocked)
		})
	}
}

func TestUnapprovedBlockedMoveDoesNotReleaseDisruptedWorkloadClaim(t *testing.T) {
	state := State{Phase: PhaseBlocked}
	observed := preparedObservation()
	observed.PreparationChangedReason = "BindingChanged"
	for range 3 {
		assertDecision(t, state, observed, state, ActionFinalizeSourceUsage)
	}
}

func TestInsufficientCapacityIsReprobedWithoutCancelingDisruptedMove(t *testing.T) {
	state := State{Phase: PhaseWaitingForCapacity}
	observed := preparedObservation()
	observed.CapacityBlocked = true
	for range 3 {
		assertDecision(t, state, observed, state, ActionEnsureCapacity)
	}
	// A fresh successful capacity action writes the execution authority and
	// consumes its preparation hold before a later observation starts copying.
	observed.CapacityBlocked, observed.PreparationReserved = false, false
	observed.CapacityApproved, observed.DestinationIntentRecorded = true, true
	assertDecision(t, state, observed, State{Phase: PhaseCopying}, ActionEnsureCopy)
}
