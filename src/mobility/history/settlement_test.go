package history

import "testing"

func TestRetiredWorkloadHistoryAndCompletionShareArtifactProof(t *testing.T) {
	move := preparedNoArtifactRecovery()
	move.Spec.Recovery = ""
	move.Status.SettlementRequested = true
	if err := ValidateSettled(move, nil, nil); err != nil {
		t.Fatalf("automatic settlement is not closed history: %v", err)
	}
	move.Status.RecoveryPhase = "Completing"
	if err := ValidateSettled(move, nil, nil); err == nil {
		t.Fatal("completion intent was mistaken for terminal history")
	}
	if err := ValidateSettlementCompletion(move, nil); err != nil {
		t.Fatalf("same recorded artifacts cannot complete retirement: %v", err)
	}
	move.Status.RollbackRequiredGeneration = 0
	if err := ValidateSettlementCompletion(move, nil); err == nil {
		t.Fatal("missing causal artifact proof completed retirement")
	}
}
