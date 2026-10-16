package fsm

import "testing"

func TestHappyPathIsClosed(t *testing.T) {
	state := State{Phase: PhasePending}
	observed := Observation{}
	steps := []struct {
		edit   func(*Observation)
		action Action
	}{
		{func(*Observation) {}, ActionProtectMove},
		{func(o *Observation) { o.ProtectionPresent = true; o.SourceHealthy = true; o.PreconditionsValid = true }, ActionInitializePreparation},
		{func(o *Observation) { o.SourcePlanRecorded = true }, ActionClaimSource},
		{func(o *Observation) { o.SourceClaimed = true }, ActionReservePreparation},
		{func(o *Observation) { o.PreparationReserved = true }, ActionWait},
		{func(*Observation) {}, ActionStartDisruption},
		{func(*Observation) {}, ActionWait},
		{func(o *Observation) { o.ReplacementExists = true; o.ReplacementHeld = true }, ActionEnsurePlacement},
		{func(o *Observation) { o.PlacementExists = true; o.DestinationScheduled = true }, ActionWait},
		{func(*Observation) {}, ActionEnsureCapacity},
		{func(o *Observation) {
			o.CapacityApproved = true
			o.DestinationIntentRecorded = true
			o.PreparationReserved = false
		}, ActionEnsureCopy},
		{func(o *Observation) { o.CopyComplete = true }, ActionEnsurePromotion},
		{func(o *Observation) { o.PromotionComplete = true }, ActionCommitOwner},
		{func(o *Observation) { o.OwnerCommitted = true }, ActionDeletePlacement},
		{func(o *Observation) { o.PlacementExists = false }, ActionReleasePlacement},
		{func(o *Observation) { o.ReplacementHeld = false; o.PublishedOnDestination = true }, ActionEnsureCleanup},
		{func(o *Observation) { o.CleanupComplete = true }, ActionConfirmCleanup},
		{func(o *Observation) { o.CompletionReady = true }, ActionMarkSucceeded},
	}
	for i, step := range steps {
		step.edit(&observed)
		decision, err := Decide(state, observed)
		if err != nil || decision.Action != step.action {
			t.Fatalf("step%d: decision=%+v err=%v want=%s", i, decision, err, step.action)
		}
		state = decision.Next
	}
	if state.Phase != PhaseSucceeded {
		t.Fatalf("final state=%+v", state)
	}
	assertDecision(t, state, Observation{}, state, ActionFinalizeHistory)
}

func TestCompletingUsesDurableCleanupEvidence(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseCleaningSource}, Observation{CleanupComplete: true})
	if err != nil || decision.Next.Phase != PhaseCompleting || decision.Action != ActionConfirmCleanup {
		t.Fatalf("cleanup must be journaled before effects: %+v, %v", decision, err)
	}
	decision, err = Decide(State{Phase: PhaseCompleting}, Observation{CompletionReady: true, DestinationUnavailable: true})
	if err != nil || decision.Next.Phase != PhaseSucceeded || decision.Action != ActionMarkSucceeded {
		t.Fatalf("metadata completion depended on unavailable disk work: %+v, %v", decision, err)
	}
	decision, err = Decide(State{Phase: PhaseCompleting}, Observation{})
	if err != nil || decision.Next.Phase != PhaseCompleting || decision.Action != ActionWait || decision.Reason != "CompletionAuthorityMismatch" {
		t.Fatalf("completion ignored authority: %+v, %v", decision, err)
	}
}

func TestEveryPhaseHasAClosedDecision(t *testing.T) {
	// Independent of allowedTransitions: preparation/cancellation constants must
	// not silently exist outside the executable graph again.
	phases := []Phase{
		PhasePending, PhasePreparing, PhaseStartingDisruption, PhaseCanceling, PhaseCancelled,
		PhaseWaitingForUnpublish, PhaseWaitingForReplacement,
		PhaseWaitingForDestination, PhaseWaitingForCapacity, PhaseCopying, PhasePromoting,
		PhaseCommitting, PhaseReleasingDestination, PhaseWaitingForDestinationPublish,
		PhaseCleaningSource, PhaseCompleting, PhaseSucceeded, PhaseBlocked,
	}
	if len(phases) != len(allowedTransitions) {
		t.Fatalf("declared phase list=%d graph=%d", len(phases), len(allowedTransitions))
	}
	for _, phase := range phases {
		observation := Observation{PreconditionsValid: true, SourceHealthy: true}
		switch phase {
		case PhasePreparing, PhaseStartingDisruption, PhaseCanceling, PhaseCancelled:
			observation.ProtectionPresent, observation.SourcePlanRecorded = true, true
			observation.SourceClaimed, observation.PreparationReserved = true, phase != PhaseCancelled
		}
		decision, err := Decide(State{Phase: phase}, observation)
		if err != nil {
			t.Errorf("phase %q: %v", phase, err)
			continue
		}
		if err := ValidateTransition(phase, decision.Next.Phase); err != nil {
			t.Errorf("phase %q returned open transition: %v", phase, err)
		}
		if decision.Action == "" {
			t.Errorf("phase %q returned no action", phase)
		}
	}
}

func TestTransitionGraphIsClosedAndEveryNonTerminalCanFinish(t *testing.T) {
	for phase, targets := range allowedTransitions {
		if len(targets) == 0 {
			t.Errorf("phase %q has no outgoing transition", phase)
			continue
		}
		for _, target := range targets {
			if !known(target) {
				t.Errorf("phase %q points to unknown phase %q", phase, target)
			}
		}
		if terminal(phase) {
			if len(targets) != 1 || targets[0] != phase {
				t.Errorf("terminal phase %q is not an isolated self-loop: %#v", phase, targets)
			}
			continue
		}
		if !canReachTerminal(phase) {
			t.Errorf("non-terminal phase %q cannot reach a terminal phase", phase)
		}
	}
}

func canReachTerminal(start Phase) bool {
	visited := map[Phase]bool{}
	queue := []Phase{start}
	for len(queue) > 0 {
		phase := queue[0]
		queue = queue[1:]
		if terminal(phase) {
			return true
		}
		if visited[phase] {
			continue
		}
		visited[phase] = true
		for _, target := range allowedTransitions[phase] {
			if !visited[target] {
				queue = append(queue, target)
			}
		}
	}
	return false
}

func TestSafetyFailuresBlockBeforeCommit(t *testing.T) {
	for _, phase := range []Phase{
		PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseCopying, PhasePromoting,
		PhaseCommitting,
	} {
		decision, err := Decide(State{Phase: phase}, Observation{CapacityApproved: true, DestinationIntentRecorded: true, UnsafeReason: "SourceUnavailable"})
		if err != nil {
			t.Fatal(err)
		}
		if decision.Next.Phase != PhaseBlocked || decision.Action != ActionMarkBlocked {
			t.Errorf("phase %q decision = %#v", phase, decision)
		}
	}
}

func TestCommittedOwnerSurvivesCommittingCrashWindow(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseCommitting}, Observation{
		OwnerCommitted: true,
		SourceHealthy:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Next.Phase != PhaseReleasingDestination || decision.Action != ActionDeletePlacement {
		t.Fatalf("committed crash-window decision = %#v", decision)
	}
}

func TestUnavailableDestinationCannotAdvanceTransaction(t *testing.T) {
	tests := []struct {
		phase       Phase
		observation Observation
	}{
		{PhaseCopying, Observation{SourceHealthy: true, DestinationUnavailable: true, CopyComplete: true}},
		{PhasePromoting, Observation{SourceHealthy: true, DestinationUnavailable: true, PromotionComplete: true}},
		{PhaseCommitting, Observation{SourceHealthy: true, DestinationUnavailable: true}},
		{PhaseReleasingDestination, Observation{DestinationUnavailable: true}},
		{PhaseWaitingForDestinationPublish, Observation{DestinationUnavailable: true, PublishedOnDestination: true}},
		{PhaseCleaningSource, Observation{DestinationUnavailable: true, CleanupComplete: true}},
	}
	for _, test := range tests {
		if test.phase == PhaseCopying || test.phase == PhasePromoting || test.phase == PhaseCommitting || test.phase == PhaseCleaningSource {
			test.observation.DestinationIntentRecorded = true
		}
		decision, err := Decide(State{Phase: test.phase}, test.observation)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Next.Phase != test.phase || decision.Action != ActionWait || decision.Reason != "DestinationUnavailable" {
			t.Errorf("phase %q decision = %#v", test.phase, decision)
		}
	}
}

func TestSelectedUnavailableDestinationWaitsBeforeCapacityProbe(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseWaitingForDestination}, Observation{
		SourceHealthy:          true,
		ReplacementExists:      true,
		ReplacementHeld:        true,
		DestinationScheduled:   true,
		DestinationUnavailable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Next.Phase != PhaseWaitingForCapacity || decision.Action != ActionWait || decision.Reason != "DestinationUnavailable" {
		t.Fatalf("selected unavailable-destination decision = %#v", decision)
	}
}

func TestCommittedOwnerWaitsToReleaseWhileDestinationIsUnavailable(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseCommitting}, Observation{OwnerCommitted: true, DestinationUnavailable: true})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Next.Phase != PhaseCommitting || decision.Action != ActionWait || decision.Reason != "DestinationUnavailable" {
		t.Fatalf("committed unavailable-destination decision = %#v", decision)
	}
}

func TestBindingLossBlocksAfterOwnerCommit(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseWaitingForDestinationPublish}, Observation{
		OwnerCommitted:            true,
		DestinationIntentRecorded: true,
		SourceAuthorityInvalid:    true,
		UnsafeReason:              "VolumeBindingMissing",
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Next.Phase != PhaseBlocked || decision.Reason != "VolumeBindingMissing" {
		t.Fatalf("post-commit binding-loss decision = %#v", decision)
	}
}

func TestActionFailuresAndDestinationFailureBlock(t *testing.T) {
	tests := []struct {
		phase       Phase
		observation Observation
		reason      string
	}{
		{PhaseWaitingForDestination, Observation{SourceHealthy: true, DestinationBlocked: true}, "DestinationUnavailable"},
		{PhaseCopying, Observation{SourceHealthy: true, DestinationBlocked: true, UnsafeReason: "PlacementReservationFailed"}, "PlacementReservationFailed"},
		{PhasePromoting, Observation{SourceHealthy: true, DestinationBlocked: true, UnsafeReason: "InvalidDestination"}, "InvalidDestination"},
		{PhaseCommitting, Observation{SourceHealthy: true, DestinationBlocked: true, UnsafeReason: "PlacementReservationConflict"}, "PlacementReservationConflict"},
		{PhaseCopying, Observation{SourceHealthy: true, CopyFailed: true}, "CopyFailed"},
		{PhasePromoting, Observation{SourceHealthy: true, PromotionFailed: true}, "PromotionFailed"},
		{PhaseCleaningSource, Observation{CleanupFailed: true}, "CleanupFailed"},
		{PhaseCopying, Observation{SourceHealthy: true, DestinationIntentRecorded: true, SourceAuthorityInvalid: true, UnsafeReason: "OwnerMismatch"}, "OwnerMismatch"},
	}
	for _, test := range tests {
		if test.phase == PhaseCopying || test.phase == PhasePromoting || test.phase == PhaseCommitting || test.phase == PhaseCleaningSource {
			test.observation.DestinationIntentRecorded = true
		}
		decision, err := Decide(State{Phase: test.phase}, test.observation)
		if err != nil {
			t.Fatal(err)
		}
		next := PhaseBlocked
		if test.phase == PhaseWaitingForDestination {
			next = PhaseCanceling
		}
		if decision.Next.Phase != next || decision.Reason != test.reason {
			t.Errorf("phase %q decision = %#v", test.phase, decision)
		}
	}
}

func TestWaitPathsStayInTheCurrentPhase(t *testing.T) {
	tests := []struct {
		phase       Phase
		observation Observation
		action      Action
	}{
		{PhasePending, Observation{ProtectionPresent: true, SourceHealthy: true, UnsafeReason: "UnsupportedConsumer"}, ActionWait},
		{PhaseWaitingForUnpublish, Observation{SourceHealthy: true, PublishedOnSource: true}, ActionWait},
		{PhaseWaitingForReplacement, Observation{SourceHealthy: true}, ActionWait},
		{PhaseWaitingForDestination, Observation{SourceHealthy: true}, ActionWait},
		{PhaseCopying, Observation{SourceHealthy: true, PlacementExists: true, DestinationScheduled: true}, ActionEnsureCopy},
		{PhasePromoting, Observation{SourceHealthy: true, PlacementExists: true, DestinationScheduled: true}, ActionEnsurePromotion},
		{PhaseCommitting, Observation{SourceHealthy: true, PlacementExists: true, DestinationScheduled: true}, ActionCommitOwner},
		{PhaseReleasingDestination, Observation{PlacementExists: true}, ActionDeletePlacement},
		{PhaseWaitingForDestinationPublish, Observation{}, ActionWait},
		{PhaseCleaningSource, Observation{}, ActionEnsureCleanup},
	}
	for _, test := range tests {
		if test.phase == PhaseCopying || test.phase == PhasePromoting || test.phase == PhaseCommitting || test.phase == PhaseCleaningSource {
			test.observation.DestinationIntentRecorded = true
		}
		decision, err := Decide(State{Phase: test.phase}, test.observation)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Next.Phase != test.phase || decision.Action != test.action {
			t.Errorf("phase %q decision = %#v", test.phase, decision)
		}
	}
}

func TestPlacementReservationIsRecreatedBeforeCommit(t *testing.T) {
	for _, phase := range []Phase{PhaseCopying, PhasePromoting, PhaseCommitting} {
		for _, observation := range []Observation{
			{SourceHealthy: true},
			{SourceHealthy: true, PlacementExists: true},
		} {
			decision, err := Decide(State{Phase: phase}, observation)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Next.Phase != phase || decision.Action != ActionEnsurePlacement {
				t.Errorf("phase %q reservation decision = %#v", phase, decision)
			}
		}
	}
}

func TestTransitionValidation(t *testing.T) {
	if err := ValidateTransition(PhasePending, PhaseCopying); err == nil {
		t.Fatal("illegal transition was accepted")
	}
	if err := ValidateTransition(PhaseSucceeded, PhasePending); err == nil {
		t.Fatal("terminal transition was accepted")
	}
	if _, err := Decide(State{Phase: Phase("Mystery")}, Observation{}); err == nil {
		t.Fatal("unknown phase was accepted")
	}
}
