package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestNodeMoveCleanupReceiptStillRequiresLaterAbsence(t *testing.T) {
	const volumeID = "shiftpv-0123456789abcdef0123456789abcdef"
	source, incoming, destination := testCopyIdentities(volumeID, "source", "destination")
	for _, test := range []struct {
		name, reason, operation string
		target                  volume.CopyIdentity
	}{
		{"source", "MoveSource", volumeapi.MoveCleanupOperationID("move-uid"), source},
		{"rollback incoming", "MoveRollback", volumeapi.MoveRollbackOperationID("move-uid"), incoming},
		{"rollback serving", "MoveRollback", volumeapi.MoveRollbackOperationID("move-uid"), destination},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestCleanupStore()
			client := store.Client.(*dynamicfake.FakeDynamicClient)
			// The API accepts a scan request, but the Node has not scanned it yet.
			client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
				update := action.(ktesting.UpdateAction)
				if update.GetSubresource() == "" {
					object := update.GetObject().(*unstructured.Unstructured)
					object.SetGeneration(object.GetGeneration() + 1)
					err := client.Tracker().Update(volumeapi.PoolResource, object, "")
					return true, object, err
				}
				return false, nil, nil
			})
			move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID}, Status: volumeapi.MoveStatus{SourceCopy: &source, DestinationCopy: &destination, CapacityApproved: true}}
			spec := cleanupapi.Spec{OperationID: test.operation, Target: test.target, Reason: test.reason, Authority: moveCleanupAuthority(move)}
			current, err := store.Ensure(ctx, spec)
			if err != nil {
				t.Fatal(err)
			}
			executor := &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: "system", PodName: "node-executor", PodUID: "node-pod-uid", NodeName: test.target.NodeName}
			if err := store.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
				t.Fatal(err)
			}
			current, err = store.Get(ctx, spec.Authority)
			if err != nil {
				t.Fatal(err)
			}
			receipt := &cleanupapi.Receipt{OperationID: spec.OperationID, ExecutorUID: executor.PodUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
			if err := store.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
				t.Fatal(err)
			}
			operator := &recordingCleanupOperator{}
			reconciler := &Reconciler{Cleanups: store, CleanupOperator: operator}
			for range 2 {
				if err := reconciler.ensureCleanupSpec(ctx, &move, spec); err != nil {
					t.Fatal(err)
				}
				current, err = store.Get(ctx, spec.Authority)
				if err != nil || current.Status.Phase != cleanupapi.PhaseConfirmingAbsence || current.Status.AbsenceProof == nil || current.Status.AbsenceProof.RequiredGeneration <= 1 || !move.Status.CapacityApproved {
					t.Fatalf("receipt bypassed fresh absence: cleanup=%+v move=%+v error=%v", current, move.Status, err)
				}
			}
			pool, err := client.Resource(volumeapi.PoolResource).Get(ctx, test.target.PoolName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
			if _, err := client.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ensureCleanupSpec(ctx, &move, spec); err != nil {
				t.Fatal(err)
			}
			current, err = store.Get(ctx, spec.Authority)
			if err != nil || current.Status.Phase != cleanupapi.PhaseCompleted || current.Status.AbsenceProof.ObservedGeneration < current.Status.AbsenceProof.RequiredGeneration || operator.reclaims != 0 || current.Spec.Target != test.target {
				t.Fatalf("cleanup did not settle exact Node receipt: cleanup=%+v reclaims=%d error=%v", current, operator.reclaims, err)
			}
		})
	}
}
