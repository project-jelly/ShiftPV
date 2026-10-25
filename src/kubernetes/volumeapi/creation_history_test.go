package volumeapi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func readyCreationHistory() State {
	return State{
		UID: "volume-uid", Phase: PhaseReady, OwnerNode: "worker-b", InitialNode: "worker-a",
		CurrentCopy:         &volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-b", PoolUID: "pool-b-uid", VolumeID: "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", VolumeUID: "volume-uid", CopyID: "promoted-copy", NodeName: "worker-b", Role: volume.RoleServing},
		CreationOperationID: "create-volume-uid",
		CreationExecutor:    &NodeExecutor{Namespace: "shiftpv-system", PodName: "original-creator", PodUID: "original-pod-uid", NodeName: "worker-a"},
		CreationReceipt:     &CreationReceipt{OperationID: "create-volume-uid", ExecutorUID: "original-pod-uid", ObservedAt: "2026-10-16T00:00:00Z", LocalReceiptDigest: strings.Repeat("a", 64)},
	}
}

func TestReadyCreationHistoryPreservesCreatorAfterMove(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		state := readyCreationHistory()
		if legacy {
			state.CreationExecutor, state.CreationReceipt, state.CreationOperationID = nil, nil, ""
		}
		before := *state.CurrentCopy
		if !ValidReadyCreationHistory(state) {
			t.Fatalf("settled history rejected: %+v", state)
		}
		if !reflect.DeepEqual(*state.CurrentCopy, before) {
			t.Fatal("history validation changed the current owner/copy")
		}
		if ValidCreationReceipt(state) {
			t.Fatal("historical receipt must not authorize current native creation")
		}
	}
}

func TestReadyCreationHistoryRejectsIncompleteOrContradictoryEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*State)
	}{
		{"pending", func(s *State) { s.Phase = PhasePending }},
		{"node creating", func(s *State) { s.Phase = PhaseNodeCreating }},
		{"deleting", func(s *State) { s.Deleting = true }},
		{"deletion intent", func(s *State) { s.DeletionOperationID = "delete-volume-uid" }},
		{"cleanup intent", func(s *State) { s.HasCleanup = true }},
		{"missing copy", func(s *State) { s.CurrentCopy = nil }},
		{"copy UID", func(s *State) { s.CurrentCopy.VolumeUID = "replacement-uid" }},
		{"copy role", func(s *State) { s.CurrentCopy.Role = volume.RoleIncoming }},
		{"current owner", func(s *State) { s.OwnerNode = "worker-c" }},
		{"executor missing", func(s *State) { s.CreationExecutor = nil }},
		{"receipt missing", func(s *State) { s.CreationReceipt = nil }},
		{"creator incomplete", func(s *State) { s.CreationExecutor.PodUID = "" }},
		{"receipt executor", func(s *State) { s.CreationReceipt.ExecutorUID = "other-pod" }},
		{"receipt operation", func(s *State) { s.CreationReceipt.OperationID = "create-other-uid" }},
		{"stored operation", func(s *State) { s.CreationOperationID = "create-other-uid" }},
		{"receipt digest", func(s *State) { s.CreationReceipt.LocalReceiptDigest = "bad" }},
		{"receipt timestamp", func(s *State) { s.CreationReceipt.ObservedAt = "bad" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := readyCreationHistory()
			tc.change(&state)
			if ValidReadyCreationHistory(state) {
				t.Fatalf("invalid history accepted: %+v", state)
			}
		})
	}
}
