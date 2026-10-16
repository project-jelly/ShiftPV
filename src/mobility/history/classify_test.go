package history

import (
	"context"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const historyVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"

func TestSucceededHistoryRequiresExactCompletedTransaction(t *testing.T) {
	for _, protocol := range []string{"", volumeapi.MoveProtocolPreparedV1} {
		for _, backend := range []string{"", "Node"} {
			t.Run(protocol+"/"+backend, func(t *testing.T) {
				move, journal := completedHistory(t, protocol, backend, "MoveSource", false)
				for _, current := range []*volumeapi.State{
					nil,
					{UID: "volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-b", CurrentCopy: move.Status.DestinationCopy},
					{UID: "replacement-volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-c", ActiveMove: "later-move"},
				} {
					if err := ValidateSettled(move, current, &journal); err != nil {
						t.Fatalf("closed history must survive later moves/Volume removal: %v", err)
					}
				}
			})
		}
	}
}

func TestHistoryRejectsContradictoryCompletion(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*volumeapi.Move, *volumeapi.State, **cleanupapi.Cleanup)
	}{
		{"active reference", func(m *volumeapi.Move, s *volumeapi.State, _ **cleanupapi.Cleanup) { s.ActiveMove = m.Name }},
		{"preparation hold", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.Preparation = &volumeapi.MovePreparation{ReservedBytes: 1}
		}},
		{"unknown protocol", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) { m.Spec.ProtocolVersion = "future" }},
		{"unfinished phase", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) { m.Status.Phase = "CleaningSource" }},
		{"no journal", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) { *c = nil }},
		{"phase projection only", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Status.Phase = cleanupapi.PhaseNeedsReview
		}},
		{"projection differs", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.CleanupPhase = cleanupapi.PhaseVerifying
		}},
		{"parent UID", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) { (*c).UID = "other-move" }},
		{"authority UID", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Spec.Authority.UID = "other-move"
		}},
		{"authority name", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Spec.Authority.Name = "other-move"
		}},
		{"operation", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Spec.OperationID = "other-operation"
		}},
		{"cleanup reason", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) { (*c).Spec.Reason = "MoveRollback" }},
		{"different source", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Spec.Target.CopyID = "other-copy"
		}},
		{"missing receipt", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) { (*c).Status.Receipt = nil }},
		{"incomplete absence", func(_ *volumeapi.Move, _ *volumeapi.State, c **cleanupapi.Cleanup) {
			(*c).Status.AbsenceProof.Complete = false
		}},
		{"source UID", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.SourceCopy.VolumeUID = "another-volume"
		}},
		{"destination UID", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.DestinationCopy.PoolUID = "another-pool"
		}},
		{"transaction operation", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.CopyOperationID = "another-operation"
		}},
		{"transaction copy", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) {
			m.Status.IncomingCopy.CopyID = "another-copy"
		}},
		{"prepared binding", func(m *volumeapi.Move, _ *volumeapi.State, _ **cleanupapi.Cleanup) { m.Status.ClaimUID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			move, journal := completedHistory(t, volumeapi.MoveProtocolPreparedV1, "Node", "MoveSource", false)
			current := volumeapi.State{UID: "volume-uid", Phase: volumeapi.PhaseReady}
			pointer := &journal
			test.edit(&move, &current, &pointer)
			if err := ValidateSettled(move, &current, pointer); err == nil {
				t.Fatal("contradictory history accepted")
			}
		})
	}
}

func TestRecoveredHistoryRequiresProofForItsExactOwner(t *testing.T) {
	for _, test := range []struct {
		reason   string
		incoming bool
	}{
		{"MoveSource", false}, {"MoveRollback", true}, {"MoveRollback", false},
	} {
		t.Run(test.reason+"/"+map[bool]string{true: "incoming", false: "serving"}[test.incoming], func(t *testing.T) {
			move, journal := completedHistory(t, "", "", test.reason, test.incoming)
			move.Spec.Recovery = "ResumeOwner"
			move.Status.Phase, move.Status.RecoveryPhase = "Blocked", "Recovered"
			move.Status.CapacityApproved, move.Status.CapacityReason = false, "RecoverySettled"
			move.Status.RecoveryOwner = move.Status.DestinationNode
			if test.reason == "MoveRollback" {
				move.Status.RecoveryOwner = move.Spec.SourceNode
				move.Status.RollbackRequiredGeneration = 3
			}
			if err := ValidateSettled(move, nil, &journal); err != nil {
				t.Fatal(err)
			}
			move.Status.CapacityApproved = true
			if err := ValidateSettled(move, nil, &journal); err == nil {
				t.Fatal("recovered approval still reserves capacity")
			}
			move.Status.CapacityApproved = false
			if err := ValidateSettled(move, nil, nil); err == nil {
				t.Fatal("effect-bearing recovery cannot rely on a phase or rollback generation alone")
			}
			if test.reason == "MoveRollback" {
				move.Status.RollbackRequiredGeneration = journal.Status.AbsenceProof.RequiredGeneration + 1
				if err := ValidateSettled(move, nil, &journal); err == nil {
					t.Fatal("cleanup proof predating rollback accepted")
				}
			}
		})
	}
}

func TestNoEffectRecoveryAndCancellation(t *testing.T) {
	move, journal := completedHistory(t, volumeapi.MoveProtocolPreparedV1, "Node", "MoveSource", false)
	preparedPlan := move.Status
	move.Spec.Recovery = "ResumeOwner"
	move.Status = volumeapi.MoveStatus{
		Phase: "Blocked", RecoveryPhase: "Recovered", RecoveryOwner: move.Spec.SourceNode,
		CapacityReason: "RecoverySettled", SourceCopy: move.Status.SourceCopy,
	}
	move.Spec.ProtocolVersion = "" // A legacy pre-approval failure has no prepared binding plan.
	if err := ValidateSettled(move, nil, nil); err != nil {
		t.Fatal(err)
	}
	move.Status.RollbackRequiredGeneration = 3
	if err := ValidateSettled(move, nil, nil); err == nil {
		t.Fatal("legacy partial approval absence cannot be reconstructed from a generation alone")
	}
	move.Spec.ProtocolVersion = volumeapi.MoveProtocolPreparedV1
	move.Status = preparedPlan
	move.Status.IncomingCopy, move.Status.DestinationCopy = nil, nil
	move.Status.CopyOperationID, move.Status.PromotionOperationID, move.Status.CleanupPhase = "", "", ""
	move.Status.Phase, move.Status.RecoveryPhase, move.Status.RecoveryOwner = "Blocked", "Recovered", move.Spec.SourceNode
	move.Status.CapacityApproved, move.Status.CapacityReason = false, "RecoverySettled"
	if err := ValidateSettled(move, nil, nil); err != nil {
		t.Fatalf("prepared no-effect recovery: %v", err)
	}
	move.Spec.ProtocolVersion = volumeapi.MoveProtocolPreparedV1
	move.Status = volumeapi.MoveStatus{Phase: "Cancelled"}
	before := move
	if err := ValidateSettled(move, nil, nil); err != nil || !reflect.DeepEqual(move, before) {
		t.Fatalf("initial-plan cancellation must remain read-only and valid: %v", err)
	}
	if err := ValidateSettled(move, nil, &journal); err == nil {
		t.Fatal("Cancelled with hidden cleanup accepted")
	}
	move.Status.CopyJobName = "copy-job"
	if err := ValidateSettled(move, nil, nil); err == nil {
		t.Fatal("Cancelled with execution intent accepted")
	}
	move.Status.CopyJobName = ""
	move.Status.RecoveryOwner = move.Spec.SourceNode
	if err := ValidateSettled(move, nil, nil); err == nil {
		t.Fatal("Cancelled with recovery authority accepted")
	}
	move.Status.RecoveryOwner = ""
	move.Status.SourceCopy = preparedPlan.SourceCopy
	if err := ValidateSettled(move, nil, nil); err == nil {
		t.Fatal("partial prepared plan accepted")
	}
}

// completedHistory uses the actual Registry decoder and Cleanup Store status
// writers, then drops lifecycle finalizers exactly as retained history does.
func completedHistory(t *testing.T, protocol, backend, reason string, rollbackIncoming bool) (volumeapi.Move, cleanupapi.Cleanup) {
	t.Helper()
	ctx := context.Background()
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-a-uid",
		VolumeID: historyVolumeID, VolumeUID: "volume-uid", NodeName: "worker-a", Role: volume.RoleServing, CopyID: "original-copy"}
	incoming, destination := volumeapi.MoveTransactionCopies("move-uid", historyVolumeID, volumeapi.MoveDestinationAnchor{
		InstallationID: source.InstallationID, PoolName: "pool-b", PoolUID: "pool-b-uid", NodeName: "worker-b", VolumeUID: source.VolumeUID,
	})
	copyMap := func(copy volume.CopyIdentity) map[string]any {
		result, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	parent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "resourceVersion": "1", "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"protocolVersion": protocol, "volumeID": historyVolumeID, "sourceNode": "worker-a"},
		"status": map[string]any{
			"phase": "Succeeded", "persistentVolumeName": "pv-a", "persistentVolumeUID": "pv-uid",
			"persistentVolumeClaimNamespace": "workloads", "persistentVolumeClaimName": "claim-a", "persistentVolumeClaimUID": "claim-uid",
			"consumerName": "consumer-a", "consumerUID": "consumer-uid", "destinationNode": "worker-b", "destinationPoolUID": "pool-b-uid",
			"sourceBytes": int64(10), "capacityApproved": true, "sourceCopy": copyMap(source), "incomingCopy": copyMap(incoming), "destinationCopy": copyMap(destination),
			"copyOperationID": volumeapi.MoveCopyOperationID("move-uid"), "promotionOperationID": volumeapi.MovePromotionOperationID("move-uid"),
		},
	}}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.VolumeResource: "ShiftPVVolumeList",
	}, parent)
	store := &cleanupapi.Store{Client: client}
	target, operation := source, volumeapi.MoveCleanupOperationID("move-uid")
	if reason == "MoveRollback" {
		target, operation = destination, volumeapi.MoveRollbackOperationID("move-uid")
		if rollbackIncoming {
			target = incoming
		}
	}
	cleanup, err := store.Ensure(ctx, cleanupapi.Spec{OperationID: operation, Target: target, Reason: reason,
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a", UID: "move-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	executor := &cleanupapi.Executor{JobName: "cleanup-job", JobUID: "job-uid", PodUID: "pod-uid", NodeName: target.NodeName}
	if backend == "Node" {
		executor = &cleanupapi.Executor{Kind: "Node", Namespace: "shiftpv-system", PodName: "node-pod", PodUID: "pod-uid", NodeName: target.NodeName}
	}
	status := cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}
	write := func() {
		if err := store.UpdateStatus(ctx, cleanup, status); err != nil {
			t.Fatal(err)
		}
		cleanup, err = store.Get(ctx, cleanup.Spec.Authority)
		if err != nil {
			t.Fatal(err)
		}
	}
	write()
	const now = "2026-10-16T00:00:00Z"
	status.Phase = cleanupapi.PhaseVerifying
	status.Receipt = &cleanupapi.Receipt{OperationID: operation, ExecutorUID: executor.ExecutionUID(), ObservedAt: now,
		Retired: true, Purged: true, LocalReceiptDigest: strings.Repeat("a", 64)}
	write()
	status.Phase = cleanupapi.PhaseConfirmingAbsence
	status.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "scan-request", PoolName: target.PoolName, PoolUID: target.PoolUID, RequiredGeneration: 4}
	write()
	status.Phase, status.SettledAt = cleanupapi.PhaseCompleted, now
	status.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "scan-request", PoolName: target.PoolName, PoolUID: target.PoolUID, RequiredGeneration: 4,
		ObservedGeneration: 5, Valid: true, Complete: true, Absent: true, ConfirmedAt: now}
	write()
	object, err := client.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetFinalizers(nil)
	if _, err := client.Resource(volumeapi.MoveResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	moves, err := (&volumeapi.Registry{Client: client}).ListMoves(ctx)
	if err != nil || len(moves) != 1 {
		t.Fatalf("moves=%v error=%v", moves, err)
	}
	cleanups, err := store.List(ctx)
	if err != nil || len(cleanups) != 1 {
		t.Fatalf("cleanups=%v error=%v", cleanups, err)
	}
	return moves[0], cleanups[0]
}
