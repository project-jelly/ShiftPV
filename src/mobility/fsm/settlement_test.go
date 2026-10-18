package fsm

import "testing"

func TestWorkloadSettlementFencesExecutionAndSkipsServiceResumption(t *testing.T) {
	facts := Observation{CapacityApproved: true, DestinationIntentRecorded: true, WorkloadRetired: true}
	for _, phase := range []Phase{PhaseCopying, PhasePromoting, PhaseCommitting, PhaseReleasingDestination, PhaseWaitingForDestinationPublish, PhaseCleaningSource, PhaseBlocked} {
		assertDecision(t, State{Phase: phase}, facts, State{Phase: PhaseBlocked}, ActionRequestSettlement)
	}
	facts.WorkloadRetired, facts.SettlementOnly, facts.RecoveryRequested = false, true, true
	for _, destination := range []bool{false, true} {
		facts.RecoveryOwnerIsDestination = destination
		state := State{Phase: PhaseBlocked}
		for _, step := range []struct {
			phase  string
			action Action
		}{
			{RecoveryQuiescing, ActionBeginRecovery}, {RecoveryVerifying, ActionQuiesceRecovery},
			{RecoveryRetiring, ActionVerifyRecovery}, {RecoveryCompleting, ActionRetireRecovery},
			{RecoveryRecovered, ActionCompleteRecovery},
		} {
			decision := assertDecision(t, state, facts, State{Phase: PhaseBlocked, RecoveryPhase: step.phase}, step.action)
			state = decision.Next
		}
	}
}

func TestRetirementCannotReopenTerminalHistory(t *testing.T) {
	facts := Observation{WorkloadRetired: true, CapacityApproved: true, DestinationIntentRecorded: true}
	assertDecision(t, State{Phase: PhaseSucceeded}, facts, State{Phase: PhaseSucceeded}, ActionFinalizeHistory)
	state := State{Phase: PhaseBlocked, RecoveryPhase: RecoveryRecovered}
	assertDecision(t, state, facts, state, ActionFinalizeHistory)
}
