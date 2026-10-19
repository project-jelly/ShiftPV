package fsm

import "testing"

func TestApprovedConsumerAmbiguityWaitsAcrossExecutionPhases(t *testing.T) {
	for _, phase := range []Phase{PhaseWaitingForCapacity, PhaseCopying, PhasePromoting, PhaseCommitting,
		PhaseReleasingDestination, PhaseWaitingForDestinationPublish, PhaseCleaningSource} {
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

func TestConsumerAmbiguityDoesNotOverrideDurableCompletion(t *testing.T) {
	decision, err := Decide(State{Phase: PhaseCompleting}, Observation{CompletionReady: true, ConsumersAmbiguous: true})
	if err != nil || decision.Next.Phase != PhaseSucceeded || decision.Action != ActionMarkSucceeded {
		t.Fatalf("consumer shape blocked already-settled completion: decision=%+v error=%v", decision, err)
	}
}
