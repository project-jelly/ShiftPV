package fsm

import "testing"

func TestApprovedConsumerAmbiguityWaitsAcrossExecutionPhases(t *testing.T) {
	for _, phase := range []Phase{PhaseWaitingForCapacity, PhaseCopying, PhasePromoting, PhaseCommitting} {
		t.Run(string(phase), func(t *testing.T) {
			observed := Observation{SourceHealthy: true, CapacityApproved: true, ConsumersAmbiguous: true,
				CopyComplete: true, PromotionComplete: true, CleanupComplete: true}
			decision, err := Decide(State{Phase: phase}, observed)
			if err != nil || decision.Next.Phase != phase || decision.Action != ActionWait || decision.Reason != "MultipleConsumers" {
				t.Fatalf("ambiguous consumers authorized an execution step: decision=%+v error=%v", decision, err)
			}
		})
	}
}

func TestConsumerAmbiguityDoesNotHideExecutionOutcomeOrCommittedCleanup(t *testing.T) {
	for _, test := range []struct {
		name        string
		phase, next Phase
		observation Observation
		action      Action
		reason      string
	}{
		{"copy failed", PhaseCopying, PhaseBlocked, Observation{CopyFailed: true}, ActionMarkBlocked, "CopyFailed"},
		{"promotion failed", PhasePromoting, PhaseBlocked, Observation{PromotionFailed: true}, ActionMarkBlocked, "PromotionFailed"},
		{"owner committed", PhaseCommitting, PhaseReleasingDestination, Observation{OwnerCommitted: true}, ActionDeletePlacement, ""},
		{"reservation removal", PhaseReleasingDestination, PhaseReleasingDestination, Observation{OwnerCommitted: true, PlacementExists: true}, ActionDeletePlacement, ""},
		{"publication missing", PhaseWaitingForDestinationPublish, PhaseWaitingForDestinationPublish, Observation{OwnerCommitted: true}, ActionWait, "MultipleConsumers"},
		{"destination published", PhaseWaitingForDestinationPublish, PhaseCleaningSource, Observation{OwnerCommitted: true, PublishedOnDestination: true}, ActionEnsureCleanup, ""},
		{"cleanup failed", PhaseCleaningSource, PhaseBlocked, Observation{OwnerCommitted: true, CleanupFailed: true}, ActionMarkBlocked, "CleanupFailed"},
		{"cleanup settled", PhaseCleaningSource, PhaseCompleting, Observation{OwnerCommitted: true, CleanupComplete: true}, ActionConfirmCleanup, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.observation
			o.SourceHealthy, o.CapacityApproved, o.ConsumersAmbiguous = true, true, true
			o.UnsafeReason = "MultipleConsumers"
			decision, err := Decide(State{Phase: test.phase}, o)
			if err != nil || decision.Next.Phase != test.next || decision.Action != test.action || decision.Reason != test.reason {
				t.Fatalf("consumer shape hid lifecycle evidence: decision=%+v error=%v", decision, err)
			}
		})
	}
}

func TestConsumerAmbiguityDoesNotOverrideDurableCompletion(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseCompleting}, Observation{CompletionReady: true, ConsumersAmbiguous: true})
	if err != nil || decision.Next.Phase != PhaseSucceeded || decision.Action != ActionMarkSucceeded {
		t.Fatalf("consumer shape blocked already-settled completion: decision=%+v error=%v", decision, err)
	}
}
