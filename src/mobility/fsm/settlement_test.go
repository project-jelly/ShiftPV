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

func TestRetiredUnapprovedWorkloadFencesCancellationBeforeRestoringBlockedSource(t *testing.T) {
	state := State{Phase: PhaseBlocked}
	facts := preparedObservation()
	facts.WorkloadRetired, facts.SourceBlocked = true, true
	for range 3 {
		decision := assertDecision(t, state, facts, State{Phase: PhaseCanceling}, ActionWait)
		if decision.Reason != "WorkloadRetired" {
			t.Fatalf("cancellation lost its retirement fence: %+v", decision)
		}
	}
	// Restore only after the Move's durable cancellation fence is observed.
	// A pending restore cannot advance cancellation or release its reservation.
	state.Phase = PhaseCanceling
	for range 3 {
		assertDecision(t, state, facts, state, ActionRestoreSource)
	}
	facts.SourceBlocked = false
	assertDecision(t, state, facts, state, ActionDeletePlacement)
	facts.PlacementAbsent = true
	assertDecision(t, state, facts, State{Phase: PhaseCancelled}, ActionReleasePreparation)
}

func TestBlockedSourceRestorationRequiresRetiredUnapprovedWorkload(t *testing.T) {
	for _, test := range []struct {
		name   string
		edit   func(*Observation)
		action Action
	}{
		{"live workload", func(o *Observation) { o.WorkloadRetired = false }, ActionFinalizeSourceUsage},
		{"execution approved", func(o *Observation) { o.CapacityApproved = true }, ActionRequestSettlement},
		{"execution intent recorded", func(o *Observation) { o.DestinationIntentRecorded = true }, ActionRequestSettlement},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := preparedObservation()
			facts.WorkloadRetired, facts.SourceBlocked = true, true
			test.edit(&facts)
			state := State{Phase: PhaseBlocked}
			assertDecision(t, state, facts, state, test.action)
		})
	}
}

func TestRetiredUnapprovedWorkloadCannotBypassSourceClaimEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Observation)
	}{
		{"contradicted source authority", func(o *Observation) { o.SourceAuthorityInvalid = true }},
		{"source plan missing", func(o *Observation) { o.SourcePlanRecorded = false }},
		{"source claim missing", func(o *Observation) { o.SourceClaimed = false }},
		{"preparation reservation missing", func(o *Observation) { o.PreparationReserved = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, sourceBlocked := range []bool{false, true} {
				facts := preparedObservation()
				facts.WorkloadRetired, facts.SourceBlocked = true, sourceBlocked
				test.edit(&facts)
				if decision, err := Decide(State{Phase: PhaseBlocked}, facts); err == nil {
					t.Fatalf("sourceBlocked=%t missing exact source evidence authorized settlement: %+v", sourceBlocked, decision)
				}
			}
		})
	}
}

func TestRetirementCannotRestoreSourceFromTerminalHistory(t *testing.T) {
	facts := Observation{WorkloadRetired: true, SourceBlocked: true}
	for _, state := range []State{{Phase: PhaseSucceeded}, {Phase: PhaseBlocked, RecoveryPhase: RecoveryRecovered}} {
		assertDecision(t, state, facts, state, ActionFinalizeHistory)
	}
}
