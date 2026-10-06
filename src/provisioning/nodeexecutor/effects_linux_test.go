//go:build linux

package nodeexecutor

import (
	"context"
	"errors"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestNativeEffectsRecoverAfterLostReceipt(t *testing.T) {
	client, node, dynamic, _, state := fixture(t)
	ctx := context.Background()
	node.HostRoot = t.TempDir()
	root := filepath.Join(node.HostRoot, "mnt", "pool")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	var lost atomic.Bool
	dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if _, found, _ := unstructured.NestedMap(object.Object, "status", "creationReceipt"); found && !lost.Swap(true) {
			return true, nil, errors.New("lost receipt response")
		}
		return false, nil, nil
	})
	if err := node.execute(ctx, testID); err == nil {
		t.Fatal("expected receipt write failure")
	}
	if _, err := os.Stat(filepath.Join(root, "volumes", testID)); err != nil {
		t.Fatal("effect did not finish", err)
	}
	current, _ := client.Volumes.Get(ctx, testID)
	if current.CreationReceipt != nil || current.Phase != volumeapi.PhaseNodeCreating {
		t.Fatal("missing receipt permitted readiness")
	}
	// Fresh process object, same Pod UID and durable intent.
	restarted := &Node{Identity: node.Identity, Discovery: node.Discovery, Volumes: node.Volumes, Cleanups: node.Cleanups, HostRoot: node.HostRoot}
	if err := restarted.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateCopy(ctx, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	state, err := client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy, Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.bindCleanup(ctx, cleanup, node.Cleanups); err != nil {
		t.Fatal(err)
	}
	var cleanupLost atomic.Bool
	dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		phase, _, _ := unstructured.NestedString(object.Object, "status", "cleanup", "status", "phase")
		if phase == cleanupapi.PhaseVerifying && !cleanupLost.Swap(true) {
			return true, nil, errors.New("cleanup receipt response lost")
		}
		return false, nil, nil
	})
	if err := node.execute(ctx, testID); err == nil {
		t.Fatal("expected cleanup receipt write failure")
	}
	if _, err := os.Stat(filepath.Join(root, "volumes", testID)); !os.IsNotExist(err) {
		t.Fatalf("copy still exists: %v", err)
	}
	current, _ = client.Volumes.Get(ctx, testID)
	if current.Phase != volumeapi.PhaseDeleting {
		t.Fatal("Node released metadata/hold")
	}
	if err := restarted.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	verified, err := client.Reclaim(ctx, cleanup, node.Cleanups)
	if err != nil || verified.Status.Phase != cleanupapi.PhaseVerifying || verified.Status.Receipt == nil || verified.Status.Receipt.ExecutorUID != node.Identity.PodUID {
		t.Fatalf("cleanup=%+v err=%v", verified, err)
	}
	dynamic.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if action.GetSubresource() == "" {
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	fenced, settled, err := node.Cleanups.ReconcileAbsence(ctx, verified)
	if err != nil || settled || fenced.Status.Phase != cleanupapi.PhaseConfirmingAbsence {
		t.Fatalf("fence=%+v settled=%v err=%v", fenced, settled, err)
	}
	if _, settled, err := node.Cleanups.ReconcileAbsence(ctx, fenced); err != nil || settled {
		t.Fatalf("stale inventory released hold: %v", err)
	}
	pool, _ := dynamic.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
	_, _ = dynamic.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{})
	if completed, settled, err := node.Cleanups.ReconcileAbsence(ctx, fenced); err != nil || !settled || completed.Status.Phase != cleanupapi.PhaseCompleted {
		t.Fatalf("fresh absence=%+v settled=%v err=%v", completed, settled, err)
	}
}

func TestNativeEffectsRejectChangedPodAndPool(t *testing.T) {
	for _, change := range []string{"pod", "pool", "publication"} {
		t.Run(change, func(t *testing.T) {
			client, node, dynamic, kube, state := fixture(t)
			ctx := context.Background()
			node.HostRoot = t.TempDir()
			root := filepath.Join(node.HostRoot, "mnt", "pool")
			_ = os.MkdirAll(root, 0700)
			if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "pod":
				pod, _ := kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
				pod.UID = "other-uid"
				_, _ = kube.CoreV1().Pods("shiftpv").Update(ctx, pod, metav1.UpdateOptions{})
			case "pool":
				pool, _ := dynamic.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
				pool.SetUID("other-pool")
				_, _ = dynamic.Resource(volumeapi.PoolResource).Update(ctx, pool, metav1.UpdateOptions{})
			case "publication":
				object, _ := dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
				_ = unstructured.SetNestedStringSlice(object.Object, []string{"worker-a"}, "status", "publishedNodes")
				_, _ = dynamic.Resource(volumeapi.VolumeResource).UpdateStatus(ctx, object, metav1.UpdateOptions{})
			}
			if err := node.execute(ctx, testID); err == nil {
				t.Fatal("changed authority permitted effect")
			}
			if _, err := os.Stat(filepath.Join(root, "volumes", testID)); !os.IsNotExist(err) {
				t.Fatal("unauthorized directory was created")
			}
		})
	}
}

func TestNativeMountedPoolRejectsMountLossBeforeCreate(t *testing.T) {
	client, node, dynamic, _, state := fixture(t)
	ctx := context.Background()
	node.HostRoot = t.TempDir()
	root := filepath.Join(node.HostRoot, "mnt", "pool")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", root, "tmpfs", 0, "size=16m"); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("mount capability unavailable")
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(root, 0) })
	pool, err := client.Volumes.PoolForIdentity(ctx, "pool-a", "pool-uid", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
	result := readiness.NewProbe(node.HostRoot).Inspect(pool)
	if !result.Mounted.OK || result.MountIdentity == nil {
		t.Fatalf("mount observation=%+v", result)
	}
	object, _ := dynamic.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	_ = unstructured.SetNestedField(object.Object, pool.MountPolicy, "spec", "mountPolicy")
	encoded, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(result.MountIdentity)
	_ = unstructured.SetNestedMap(object.Object, encoded, "status", "mountIdentity")
	_, _ = dynamic.Resource(volumeapi.PoolResource).Update(ctx, object, metav1.UpdateOptions{})
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	if err := unix.Unmount(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := node.execute(ctx, testID); err == nil {
		t.Fatal("mount loss authorized create")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unmounted backing path was modified: entries=%v err=%v", entries, err)
	}
}
