package fsm

import (
	"reflect"
	"testing"
)

func preparedObservation() Observation {
	return Observation{
		ProtectionPresent: true, SourcePlanRecorded: true, SourceClaimed: true,
		SourceHealthy: true, PreconditionsValid: true, PreparationReserved: true,
	}
}

func TestPreparedEntryPersistsIntentBeforeDisruption(t *testing.T) {
	tests := []struct {
		name   string
		phase  Phase
		edit   func(*Observation)
		next   Phase
		action Action
	}{
		{"protect", PhasePending, func(o *Observation) { o.ProtectionPresent = false }, PhasePending, ActionProtectMove},
		{"initialize", PhasePending, func(o *Observation) {
			o.SourcePlanRecorded = false
			o.SourceClaimed = false
			o.PreparationReserved = false
		}, PhasePreparing, ActionInitializePreparation},
		{"claim ready", PhasePreparing, func(o *Observation) { o.SourceClaimed = false; o.PreparationReserved = false }, PhasePreparing, ActionClaimSource},
		{"reserve", PhasePreparing, func(o *Observation) { o.PreparationReserved = false }, PhasePreparing, ActionReservePreparation},
		{"disruption intent only", PhasePreparing, func(*Observation) {}, PhaseStartingDisruption, ActionWait},
		{"disrupt", PhaseStartingDisruption, func(*Observation) {}, PhaseWaitingForUnpublish, ActionStartDisruption},
		{"resume moving", PhaseStartingDisruption, func(o *Observation) { o.SourceMoving = true }, PhaseWaitingForUnpublish, ActionStartDisruption},
		{"deferred without hold", PhasePreparing, func(o *Observation) { o.PreparationReserved = false; o.PreflightDeferred = true }, PhasePreparing, ActionWait},
		{"deferred placement release", PhasePreparing, func(o *Observation) {
			o.PreparationReserved = false
			o.PreflightDeferred = true
			o.PlacementExists = true
		}, PhasePreparing, ActionDeletePlacement},
		{"deferred held reservation", PhasePreparing, func(o *Observation) { o.PreflightDeferred = true }, PhaseCanceling, ActionWait},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := preparedObservation()
			test.edit(&observation)
			assertDecision(t, State{Phase: test.phase}, observation, State{Phase: test.next}, test.action)
		})
	}
	assertDecision(t, State{}, Observation{}, State{Phase: PhasePending}, ActionProtectMove)
}

func TestPreparedExecutionApprovalIsFreshlyObserved(t *testing.T) {
	observation := preparedObservation()
	observation.ReplacementExists, observation.ReplacementHeld, observation.DestinationScheduled = true, true, true
	assertDecision(t, State{Phase: PhaseWaitingForDestination}, observation, State{Phase: PhaseWaitingForCapacity}, ActionWait)
	assertDecision(t, State{Phase: PhaseWaitingForCapacity}, observation, State{Phase: PhaseWaitingForCapacity}, ActionEnsureCapacity)
	observation.CapacityApproved, observation.DestinationIntentRecorded, observation.PreparationReserved = true, true, false
	assertDecision(t, State{Phase: PhaseWaitingForCapacity}, observation, State{Phase: PhaseCopying}, ActionEnsureCopy)
}

func TestPreparedEmptyPhaseCanInitializeItsObservedPlan(t *testing.T) {
	observation := preparedObservation()
	observation.SourcePlanRecorded, observation.SourceClaimed, observation.PreparationReserved = false, false, false
	assertDecision(t, State{}, observation, State{Phase: PhasePreparing}, ActionInitializePreparation)
	assertDecision(t, State{}, Observation{}, State{Phase: PhasePending}, ActionProtectMove)
}

func TestSourceAuthorityLossBlocksAfterDisruptionIntent(t *testing.T) {
	for _, phase := range []Phase{PhaseWaitingForUnpublish, PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseWaitingForCapacity} {
		for _, evidence := range []string{"none", "approval", "disk intent"} {
			t.Run(string(phase)+"/"+evidence, func(t *testing.T) {
				observation := preparedObservation()
				observation.SourceAuthorityInvalid = true
				observation.UnsafeReason = "BindingChanged"
				observation.CapacityApproved = evidence == "approval"
				observation.DestinationIntentRecorded = evidence == "disk intent"
				decision := assertDecision(t, State{Phase: phase}, observation, State{Phase: PhaseBlocked}, ActionMarkBlocked)
				if decision.Reason != "BindingChanged" {
					t.Fatalf("reason=%q", decision.Reason)
				}
			})
		}
	}
	observation := preparedObservation()
	observation.PreparationChangedReason = "SourceNotCordoned"
	decision := assertDecision(t, State{Phase: PhasePreparing}, observation, State{Phase: PhaseCanceling}, ActionWait)
	if decision.Reason != observation.PreparationChangedReason {
		t.Fatal("fresh contradiction was discarded")
	}
	observation.PreparationChangedReason = ""
	observation.CapacityBlocked = true
	assertDecision(t, State{Phase: PhaseWaitingForCapacity}, observation, State{Phase: PhaseWaitingForCapacity}, ActionEnsureCapacity)
	observation.DestinationUnavailable = true
	assertDecision(t, State{Phase: PhaseWaitingForCapacity}, observation, State{Phase: PhaseWaitingForCapacity}, ActionWait)
}

func TestCancellationIntentDoesNotRequireLiveSourceClaimObservation(t *testing.T) {
	for _, phase := range []Phase{PhasePending, PhasePreparing} {
		t.Run(string(phase), func(t *testing.T) {
			// An effect-time invalidation fences the Move writer before another
			// Volume read. Missing live facts cannot authorize settlement.
			observation := Observation{PreparationReserved: true, PreparationChangedReason: "PlacementChanged"}
			decision := assertDecision(t, State{Phase: phase}, observation, State{Phase: PhaseCanceling}, ActionWait)
			observation.PreparationChangedReason = ""
			observation.PlacementAbsent = true
			if result, err := Decide(decision.Next, observation); err == nil {
				t.Fatalf("missing source claim authorized cancellation settlement: %+v", result)
			}
			if result, err := Decide(State{Phase: PhasePreparing}, observation); err == nil {
				t.Fatalf("normal preparation ignored a lost reserved source claim: %+v", result)
			}
		})
	}
}

func TestCancellationSettlesSourceThenPlacementThenHoldThenClaim(t *testing.T) {
	observation := Observation{SourcePlanRecorded: true}
	state := State{Phase: PhaseCanceling}
	assertDecision(t, state, observation, state, ActionClaimSource)
	observation.SourceClaimed, observation.SourceMoving = true, true
	observation.PreparationReserved = true
	assertDecision(t, state, observation, state, ActionRestoreSource)
	observation.SourceMoving = false
	assertDecision(t, state, observation, state, ActionDeletePlacement)
	observation.PlacementAbsent = true
	decision := assertDecision(t, state, observation, State{Phase: PhaseCancelled}, ActionReleasePreparation)
	// Pending effects do not consume Next. The same observation selects the
	// same action; only an accepted combined hold/phase CAS enters Cancelled.
	if again, _ := Decide(state, observation); !reflect.DeepEqual(again, decision) {
		t.Fatal("decision mutated input")
	}
	state, observation.PreparationReserved = decision.Next, false
	assertDecision(t, state, observation, state, ActionReleaseSource)
	observation.SourceClaimed = false
	assertDecision(t, state, observation, state, ActionFinalizeHistory)
}

func TestCancelledReentryStillSettlesDelayedWritesAndPlacement(t *testing.T) {
	state := State{Phase: PhaseCancelled}
	observation := Observation{SourcePlanRecorded: true, SourceClaimed: true, SourceMoving: true,
		SourceAuthorityInvalid: true, PreparationChangedReason: "MissingBinding"}
	assertDecision(t, state, observation, state, ActionRestoreSource)
	observation.SourceMoving = false
	assertDecision(t, state, observation, state, ActionDeletePlacement)
	observation.PlacementAbsent = true
	assertDecision(t, state, observation, state, ActionReleaseSource)
	observation.SourceClaimed = false
	assertDecision(t, state, observation, state, ActionFinalizeHistory)
	observation.PlacementAbsent = false
	assertDecision(t, state, observation, state, ActionDeletePlacement)
}

func TestTerminalAndRecoveryRoutingPrecedesOrdinaryGuards(t *testing.T) {
	observation := Observation{SourceAuthorityInvalid: true, DestinationBlocked: true,
		DestinationUnavailable: true, PreparationChangedReason: "MissingBinding", RecoveryRequested: true}
	assertDecision(t, State{Phase: PhaseSucceeded}, observation, State{Phase: PhaseSucceeded}, ActionFinalizeHistory)
	settled := State{Phase: PhaseBlocked, RecoveryPhase: RecoveryRecovered}
	assertDecision(t, settled, observation, settled, ActionFinalizeHistory)
	// Recovered clears approval, but retained disk intent must never route to
	// pre-disruption cancellation or create a second transaction.
	observation.DestinationIntentRecorded = true
	state := State{Phase: PhaseBlocked, RecoveryPhase: RecoveryQuiescing}
	assertDecision(t, state, observation, State{Phase: PhaseBlocked, RecoveryPhase: RecoveryVerifying}, ActionQuiesceRecovery)
	observation.RecoveryRequested = false
	assertDecision(t, state, observation, state, ActionFinalizeSourceUsage)
}

func TestRecoveryOwnerDeterminesCleanupPublicationOrder(t *testing.T) {
	for _, destination := range []bool{false, true} {
		name := "source"
		if destination {
			name = "destination"
		}
		t.Run(name, func(t *testing.T) {
			observation := Observation{CapacityApproved: true, DestinationIntentRecorded: true,
				RecoveryRequested: true, RecoveryOwnerIsDestination: destination}
			state := State{Phase: PhaseBlocked}
			stages := []struct {
				next   string
				action Action
			}{
				{RecoveryQuiescing, ActionBeginRecovery}, {RecoveryVerifying, ActionQuiesceRecovery},
				{RecoveryRetiring, ActionVerifyRecovery}, {RecoveryResuming, ActionRetireRecovery},
				{RecoveryCompleting, ActionResumeRecovery}, {RecoveryRecovered, ActionCompleteRecovery},
			}
			if destination {
				stages[2] = struct {
					next   string
					action Action
				}{RecoveryResuming, ActionVerifyRecovery}
				stages[3] = struct {
					next   string
					action Action
				}{RecoveryRetiring, ActionResumeRecovery}
				stages[4] = struct {
					next   string
					action Action
				}{RecoveryCompleting, ActionRetireRecovery}
			}
			for _, stage := range stages {
				decision := assertDecision(t, state, observation, State{Phase: PhaseBlocked, RecoveryPhase: stage.next}, stage.action)
				// Root applies Next only when the action is ready; a pending
				// action retains this exact recovery state across re-observation.
				assertDecision(t, state, observation, decision.Next, stage.action)
				state = decision.Next
			}
			observation.CapacityApproved = false
			assertDecision(t, state, observation, state, ActionFinalizeHistory)
		})
	}
}

func TestRecoveryUnlockObservationIsScopedToCompleting(t *testing.T) {
	observation := Observation{RecoveryRequested: true, DestinationIntentRecorded: true, RecoveryUnlockObserved: true}
	assertDecision(t, State{Phase: PhaseBlocked, RecoveryPhase: RecoveryCompleting}, observation,
		State{Phase: PhaseBlocked, RecoveryPhase: RecoveryRecovered}, ActionWait)
	assertDecision(t, State{Phase: PhaseBlocked, RecoveryPhase: RecoveryVerifying}, observation,
		State{Phase: PhaseBlocked, RecoveryPhase: RecoveryRetiring}, ActionVerifyRecovery)
}

func TestRetiredPhasesAreNotExecutable(t *testing.T) {
	for _, phase := range []Phase{"Locking", "Evicting"} {
		if decision, err := Decide(State{Phase: phase}, Observation{}); err == nil {
			t.Fatalf("retired phase executed: %+v", decision)
		}
	}
}

func TestLifecycleRejectsIncompatibleStateAxes(t *testing.T) {
	for _, test := range []struct {
		state       State
		observation Observation
	}{
		{State{Phase: PhaseStartingDisruption}, Observation{}},
		{State{Phase: PhasePending}, Observation{CapacityApproved: true}},
		{State{Phase: PhasePreparing}, Observation{CapacityApproved: true}},
		{State{Phase: PhaseCanceling}, Observation{DestinationIntentRecorded: true}},
		{State{Phase: PhaseCancelled}, Observation{PreparationReserved: true}},
		{State{Phase: PhasePreparing}, Observation{PreparationReserved: true, SourcePlanRecorded: true}},
		{State{Phase: PhaseCanceling}, Observation{PreparationReserved: true, SourcePlanRecorded: true}},
		{State{Phase: PhaseStartingDisruption}, Observation{}},
		{State{Phase: PhaseCopying, RecoveryPhase: RecoveryQuiescing}, Observation{}},
		{State{Phase: PhaseBlocked, RecoveryPhase: "Mystery"}, Observation{}},
	} {
		if decision, err := Decide(test.state, test.observation); err == nil {
			t.Errorf("invalid state=%+v observation=%+v returned %+v", test.state, test.observation, decision)
		}
	}
}

func assertDecision(t *testing.T, state State, observation Observation, next State, action Action) Decision {
	t.Helper()
	decision, err := Decide(state, observation)
	if err != nil || decision.Next != next || decision.Action != action {
		t.Fatalf("state=%+v decision=%+v error=%v want next=%+v action=%s", state, decision, err, next, action)
	}
	if state.Phase != "" {
		if err := ValidateTransition(state.Phase, decision.Next.Phase); err != nil {
			t.Fatal(err)
		}
	}
	return decision
}
