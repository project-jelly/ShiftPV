//go:build linux

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestApprovedCleanupIgnoresSiblingScanButRetainsPostEffectFence(t *testing.T) {
	client, node, api, _, state := fixture(t)
	ctx := context.Background()
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	installApprovedBacking(t, node, api)
	if err := node.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	// Approve deletion using a current scan before another operation advances
	// the shared Pool's scanEpoch. Mount/sysfs reads remain injected.
	object, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	for _, item := range conditions {
		item.(map[string]any)["observedGeneration"] = object.GetGeneration()
	}
	_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
	if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err = client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy,
		Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := bindNodeCleanup(ctx, node, cleanup); err != nil {
		t.Fatal(err)
	}
	api.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	if _, err := client.Volumes.RequestPoolScan(ctx, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	if err := node.execute(ctx, testID); err != nil {
		t.Fatalf("approved purge waited for sibling scan: %v", err)
	}
	if _, err := os.Stat(filepath.Join(node.HostRoot, "mnt/pool/volumes", testID)); !os.IsNotExist(err) {
		t.Fatalf("copy not purged: %v", err)
	}
	verified, err := node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	if err != nil || verified.Status.Phase != cleanupapi.PhaseVerifying || verified.Status.Receipt == nil {
		t.Fatalf("receipt missing: %+v %v", verified, err)
	}
	fenced, settled, err := node.Cleanups.ReconcileAbsence(ctx, verified)
	if err != nil || settled || fenced.Status.AbsenceProof == nil {
		t.Fatalf("missing post-effect fence: %+v %v", fenced, err)
	}
	if _, settled, err := node.Cleanups.ReconcileAbsence(ctx, fenced); err != nil || settled {
		t.Fatalf("pre-effect inventory released hold: %v", err)
	}
	current, err := client.Volumes.Get(ctx, testID)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, state.CurrentCopy.PoolUID)
	if err != nil || reserved != state.CapacityBytes || current.Phase != volumeapi.PhaseDeleting {
		t.Fatalf("hold changed before absence: reserved=%d state=%+v %v", reserved, current, err)
	}
	// Publish a simulated complete scan only after real purge and the new fence.
	object, err = api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
	_ = unstructured.SetNestedMap(object.Object, map[string]any{"valid": true, "observedAt": time.Now().UTC().Format(time.RFC3339Nano)}, "status", "inventory")
	if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if completed, settled, err := node.Cleanups.ReconcileAbsence(ctx, fenced); err != nil || !settled || completed.Status.Phase != cleanupapi.PhaseCompleted {
		t.Fatalf("fresh absence did not settle: %+v %v", completed, err)
	}
}
