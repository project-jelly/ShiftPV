package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

// This is historical no-disk source recovery, not an execution shortcut. The
// supported controller only reads this old record and observes owned helpers.
func seedLegacyRecoveredHistory(t *testing.T, f *preparationRegistryFixture, index int) volumeapi.Move {
	t.Helper()
	move, state := f.move(t, index), f.state(t, index)
	object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := runtime.DefaultUnstructuredConverter.ToUnstructured(state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
	_ = unstructured.SetNestedField(object.Object, "ResumeOwner", "spec", "recovery")
	object.Object["status"] = map[string]any{"phase": "Blocked", "sourceCopy": source, "recoveryOwner": "source", "recoveryPhase": "Recovered", "capacityReason": "RecoverySettled", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano)}
	if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
		t.Fatal(err)
	}
	return f.move(t, index)
}

func TestRecentLegacyHistoryAllowsNewPreparedDiscovery(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	old := seedLegacyRecoveredHistory(t, f, 0)
	if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil || len(moves) != 2 {
		t.Fatalf("retained history prevented discovery: %+v %v", moves, err)
	}
	for _, move := range moves {
		if move.UID == old.UID {
			if !reflect.DeepEqual(move.Status, old.Status) || move.Spec != old.Spec {
				t.Fatalf("history was executed: %+v", move)
			}
		} else if move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 || move.Status.Phase != "Preparing" || move.Status.CapacityApproved || move.Status.EvictionRequested {
			t.Fatalf("new admission did not create prepared intent only: %+v", move)
		}
	}
	assertPreparationOriginals(t, f)
}

func TestOwnerPlacementRequiresFreshSettledHistory(t *testing.T) {
	for _, fault := range []string{"none", "missing source proof", "live helper", "foreign Volume reference", "Volume list failure", "helper list failure"} {
		t.Run(fault, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 2)
			old := seedLegacyRecoveredHistory(t, f, 0)
			// The unrelated second volume is not referenced by this consumer.
			node, _ := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
			node.Spec.Unschedulable = false
			if _, err := f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			w := f.workloads[0]
			pod, _ := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
			pod.Spec.NodeName = ""
			pod.Status.Phase = corev1.PodPending
			pod.Labels = map[string]string{admission.ManagedLabel: "true"}
			pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}, {Name: "example.test/user"}}
			pod.Spec.NodeSelector = map[string]string{"example.test/zone": "home"}
			if _, err := f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "missing source proof":
				object, _ := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, old.Name, metav1.GetOptions{})
				unstructured.RemoveNestedField(object.Object, "status", "sourceCopy")
				if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
					t.Fatal(err)
				}
			case "live helper":
				helper := f.reconciler.placementPod(old, pod, namesFor(old.Name))
				if _, err := f.client.CoreV1().Pods("system").Create(f.ctx, helper, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "foreign Volume reference":
				object, _ := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, f.volumeIDs[1], metav1.GetOptions{})
				_ = unstructured.SetNestedField(object.Object, old.Name, "status", "activeMove")
				if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, object); err != nil {
					t.Fatal(err)
				}
			case "Volume list failure":
				f.dynamic.PrependReactor("list", "shiftpvvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("list unavailable") })
			case "helper list failure":
				f.client.PrependReactor("list", "jobs", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("list unavailable") })
			}
			before := f.move(t, 0)
			err := f.reconciler.reconcileOwnerPlacements(f.ctx)
			if (err != nil) != (fault != "none") {
				t.Fatalf("fault=%s error=%v", fault, err)
			}
			got, getErr := f.client.CoreV1().Pods(pod.Namespace).Get(f.ctx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				t.Fatal(getErr)
			}
			if fault != "none" {
				if !reflect.DeepEqual(pod, got) {
					t.Fatalf("unsettled history released consumer: %+v", got)
				}
			} else if got.UID != pod.UID || hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "source" || got.Spec.NodeSelector["example.test/zone"] != "home" || len(got.Spec.SchedulingGates) != 1 || got.Spec.SchedulingGates[0].Name != "example.test/user" {
				t.Fatalf("owner recovery changed identity/constraints: %+v", got)
			}
			if !reflect.DeepEqual(before, f.move(t, 0)) {
				t.Fatal("owner recovery rewrote historical record")
			}
		})
	}
}

type historyCountingRepository struct {
	Repository
	lists int
}

func (r *historyCountingRepository) ListVolumes(ctx context.Context) (map[string]volumeapi.State, error) {
	r.lists++
	return r.Repository.ListVolumes(ctx)
}

func TestSettledHistoryUsesOneCompleteHelperSnapshotForBatch(t *testing.T) {
	f := newPreparationRegistryFixture(t, 2)
	moves := []volumeapi.Move{seedLegacyRecoveredHistory(t, f, 0), seedLegacyRecoveredHistory(t, f, 1)}
	repo := &historyCountingRepository{Repository: f.registry}
	f.reconciler.Repository = repo
	f.client.ClearActions()
	if err := f.reconciler.validateSettledMoves(f.ctx, moves); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, a := range f.client.Actions() {
		if a.GetVerb() == "list" {
			counts[a.GetResource().Resource]++
		}
	}
	if repo.lists != 1 || counts["jobs"] != 1 || counts["pods"] != 1 {
		t.Fatalf("per-record repeated snapshots: volumes=%d helpers=%v", repo.lists, counts)
	}
	// An incomplete response is not equivalent to absence.
	f.client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: "next"}}, nil
	})
	if err := f.reconciler.validateSettledMoves(f.ctx, moves); err == nil {
		t.Fatal("partial helper snapshot accepted")
	}
}

func TestInvalidCompletedHistoryBlocksOnlyItsVolume(t *testing.T) {
	f := newPreparationRegistryFixture(t, 2)
	old := seedLegacyRecoveredHistory(t, f, 0)
	object, _ := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, old.Name, metav1.GetOptions{})
	// Coarse terminal metadata is deliberately insufficient. There is no full
	// destination transaction or receipt behind this claimed completion.
	_ = unstructured.SetNestedField(object.Object, "Succeeded", "status", "phase")
	object.Object["status"].(map[string]any)["cleanup"] = map[string]any{"phase": cleanupapi.PhaseCompleted}
	if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
		t.Fatal(err)
	}
	before := f.move(t, 0)
	err := f.reconciler.ReconcileAll(f.ctx)
	if err == nil || !strings.Contains(err.Error(), old.Name) {
		t.Fatalf("invalid history not reported: %v", err)
	}
	if !reflect.DeepEqual(before, f.move(t, 0)) || f.move(t, 1).Status.SourceCopy == nil {
		t.Fatal("bad history was rewritten or blocked unrelated Volume")
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil || len(moves) != 2 {
		t.Fatalf("invalid history admitted another Move: %+v %v", moves, err)
	}
}

func TestLegacyEntryPreservesUnsettledRecordsAndEffects(t *testing.T) {
	for _, fault := range []string{"active", "missing source", "NeedsReview", "live relabelled helper"} {
		t.Run(fault, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			move := seedLegacyRecoveredHistory(t, f, 0)
			switch fault {
			case "active", "missing source":
				object, _ := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
				if fault == "active" {
					_ = unstructured.SetNestedField(object.Object, "Pending", "status", "phase")
				} else {
					unstructured.RemoveNestedField(object.Object, "status", "sourceCopy")
				}
				if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
					t.Fatal(err)
				}
			case "NeedsReview":
				spec := cleanupapi.Spec{OperationID: volumeapi.MoveCleanupOperationID(move.UID), Target: *move.Status.SourceCopy, Reason: "MoveSource", Authority: moveCleanupAuthority(move)}
				journal, err := f.reconciler.Cleanups.Ensure(f.ctx, spec)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.reconciler.Cleanups.UpdateStatus(f.ctx, journal, cleanupapi.Status{Phase: cleanupapi.PhaseNeedsReview, Reason: "IdentityChanged", Message: "operator review required"}); err != nil {
					t.Fatal(err)
				}
			case "live relabelled helper":
				pod := f.reconciler.placementPod(move, f.workloads[0].Consumer, namesFor(move.Name))
				pod.Labels = nil
				if _, err := f.client.CoreV1().Pods("system").Create(f.ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			before, state := f.move(t, 0), f.state(t, 0)
			f.dynamic.ClearActions()
			f.client.ClearActions()
			for range 2 {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, before); err == nil {
					t.Fatal("unsettled legacy record accepted")
				}
			}
			if !reflect.DeepEqual(before, f.move(t, 0)) || !reflect.DeepEqual(state, f.state(t, 0)) {
				t.Fatal("legacy entry changed history or Volume")
			}
			for _, actions := range [][]ktesting.Action{f.dynamic.Actions(), f.client.Actions()} {
				for _, action := range actions {
					if action.GetVerb() != "get" && action.GetVerb() != "list" {
						t.Fatalf("legacy entry mutated API: %+v", action)
					}
				}
			}
		})
	}
}
