//go:build linux

package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	nodeobservation "github.com/project-jelly/ShiftPV/src/node/observation"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
)

// The API transport and publication lookup are injected; mount, ownership,
// Node execution, receipt persistence, inventory scan and settlement are real.
func TestNativeMountedPoolCreationCancellation(t *testing.T) {
	for _, boundary := range []string{"pending", "before_copy_intent", "before_stage_mkdir", "before_placement_marker", "before_final_rename", "before_api_receipt", "before_controller_completion"} {
		t.Run(boundary, func(t *testing.T) {
			client, node, api, kube, initial := fixture(t)
			ctx := context.Background()
			node.HostRoot = t.TempDir()
			root := filepath.Join(node.HostRoot, "mnt/pool")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := unix.Mount("tmpfs", root, "tmpfs", 0, "size=16m"); err != nil {
				if errors.Is(err, unix.EPERM) {
					t.Skip("mount capability unavailable")
				}
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = unix.Unmount(root, 0) })
			object, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_ = unstructured.SetNestedField(object.Object, volumeapi.PoolMountPolicyRequireMountPoint, "spec", "mountPolicy")
			if _, err := api.Resource(volumeapi.PoolResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			scanner := &nodeobservation.Scanner{HostRoot: node.HostRoot, TargetRoot: "/pods", Installation: client.Volumes, Publications: unpublishedCreation{}, Limit: 256}
			reconciler, err := readiness.NewReconciler(node.Identity.NodeName, client.Volumes, readiness.NewProbe(node.HostRoot), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			reconciler.Inventory = scanner.Scan
			reconcileCreationPool(t, ctx, reconciler, client, *initial.CurrentCopy)
			if boundary != "pending" {
				if err := client.Volumes.BindCreation(ctx, initial, node.Identity); err != nil {
					t.Fatal(err)
				}
				prepareCreationBoundary(t, ctx, node, root, *initial.CurrentCopy, boundary)
			}
			fenced, err := client.Volumes.BeginCreationCancellation(ctx, testID, initial.UID, *initial.CurrentCopy)
			if err != nil {
				t.Fatal(err)
			}
			journal, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: fenced.DeletionOperationID, Target: *fenced.CurrentCopy, Reason: "CreationCancelled", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: initial.UID}})
			if err != nil {
				t.Fatal(err)
			}
			if err := bindNodeCleanup(ctx, node, journal); err != nil {
				t.Fatal(err)
			}
			if err := client.Volumes.CompleteCreate(ctx, testID, initial.UID, *initial.CurrentCopy); !errors.Is(err, volumeapi.ErrStateConflict) {
				t.Fatalf("late completion: %v", err)
			}
			// Drop the API write after real purge, then replace the executor Pod.
			lost := false
			api.PrependReactor("update", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
				object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				phase, _, _ := unstructured.NestedString(object.Object, "status", "cleanup", "status", "phase")
				if phase == cleanupapi.PhaseVerifying && !lost {
					lost = true
					return true, nil, errors.New("cleanup receipt response lost")
				}
				return false, nil, nil
			})
			if err := node.execute(ctx, testID); err == nil {
				t.Fatal("receipt barrier did not run")
			}
			if !lost {
				t.Fatal("native cleanup did not reach receipt write")
			}
			assertCancelledCreationHold(t, client, initial)
			pod, err := kube.CoreV1().Pods(node.Identity.Namespace).Get(ctx, node.Identity.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.UID = types.UID("replacement-node-pod")
			if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			restarted := &Node{Identity: node.Identity, Pods: node.Pods, Volumes: node.Volumes, Cleanups: node.Cleanups, HostRoot: node.HostRoot}
			restarted.Identity.PodUID = string(pod.UID)
			journal, err = node.Cleanups.Get(ctx, journal.Spec.Authority)
			if err != nil {
				t.Fatal(err)
			}
			if err := bindNodeCleanup(ctx, restarted, journal); err != nil {
				t.Fatal(err)
			}
			request := &protocol.EffectRequest{VolumeName: testID, VolumeUid: initial.UID, ExecutorUid: restarted.Identity.PodUID, NodeName: restarted.Identity.NodeName, OperationId: fenced.DeletionOperationID}
			if err := restarted.Execute(ctx, request, protocol.Operation_RECLAIM); err != nil {
				t.Fatal(err)
			}
			journal, err = node.Cleanups.Get(ctx, journal.Spec.Authority)
			if err != nil || journal.Status.Receipt == nil || journal.Status.Receipt.ExecutorUID != restarted.Identity.PodUID {
				t.Fatalf("receipt=%+v err=%v", journal, err)
			}
			// Model real API generation behavior; the scanner below must observe
			// the new generation, not just a fresh timestamp at the old one.
			api.PrependReactor("update", "shiftpvpools", func(a ktesting.Action) (bool, runtime.Object, error) {
				if a.GetSubresource() == "" {
					object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
					object.SetGeneration(object.GetGeneration() + 1)
				}
				return false, nil, nil
			})
			journal, settled, err := node.Cleanups.ReconcileAbsence(ctx, journal)
			if err != nil || settled || journal.Status.AbsenceProof == nil {
				t.Fatalf("fence=%+v settled=%v err=%v", journal, settled, err)
			}
			if _, settled, err := node.Cleanups.ReconcileAbsence(ctx, journal); err != nil || settled {
				t.Fatalf("stale inventory settled: %v", err)
			}
			assertCancelledCreationHold(t, client, initial)
			pool := reconcileCreationPool(t, ctx, reconciler, client, *initial.CurrentCopy)
			if !pool.Status.Inventory.Valid || pool.Status.Inventory.Truncated || len(pool.Status.Inventory.Copies) != 0 {
				t.Fatalf("actual scan is not empty: %+v", pool.Status.Inventory)
			}
			journal, settled, err = node.Cleanups.ReconcileAbsence(ctx, journal)
			if err != nil || !settled || journal.Status.Phase != cleanupapi.PhaseCompleted {
				t.Fatalf("settlement=%+v settled=%v err=%v", journal, settled, err)
			}
			assertCancelledCreationHold(t, client, initial)
			if _, err := os.Stat(filepath.Join(root, "volumes", testID)); !os.IsNotExist(err) {
				t.Fatalf("serving copy remains: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".shiftpv/incoming", "create-"+initial.CurrentCopy.CopyID)); !os.IsNotExist(err) {
				t.Fatalf("creation stage remains: %v", err)
			}
		})
	}
}

func assertCancelledCreationHold(t *testing.T, client *Client, initial volumeapi.State) {
	t.Helper()
	current, err := client.Volumes.Get(context.Background(), testID)
	if err != nil || current.UID != initial.UID || current.CurrentCopy == nil || *current.CurrentCopy != *initial.CurrentCopy || !current.CancellingCreation() {
		t.Fatalf("cancellation identity=%+v err=%v", current, err)
	}
	reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, initial.CurrentCopy.PoolUID)
	if err != nil || reserved != initial.CapacityBytes {
		t.Fatalf("original reservation=%d err=%v", reserved, err)
	}
}
