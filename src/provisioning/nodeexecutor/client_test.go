package nodeexecutor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestCreationRequiresBoundReceiptAndPreservesIt(t *testing.T) {
	client, node, _, _, state := fixture(t)
	ctx := context.Background()
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	current, err := client.Volumes.Get(ctx, testID)
	if err != nil || current.CreationExecutor == nil || *current.CreationExecutor != node.Identity || current.Phase != volumeapi.PhaseNodeCreating {
		t.Fatalf("binding=%+v err=%v", current, err)
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("completion without receipt=%v", err)
	}
	receipt := creationReceipt(node.Identity)
	changed := receipt
	changed.ExecutorUID = "other-pod"
	if err := client.Volumes.RecordCreationReceipt(ctx, current, changed); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("foreign receipt=%v", err)
	}
	if err := client.Volumes.RecordCreationReceipt(ctx, current, receipt); err != nil {
		t.Fatal(err)
	}
	changed = receipt
	changed.LocalReceiptDigest = "bad"
	if err := client.Volumes.RecordCreationReceipt(ctx, current, changed); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("mutable receipt=%v", err)
	}
	if err := client.Volumes.BindCreation(ctx, current, node.Identity); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("post receipt rebind=%v", err)
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	ready, err := client.Volumes.Get(ctx, testID)
	if err != nil || !volumeapi.ValidCreationReceipt(ready) || ready.Phase != volumeapi.PhaseReady {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if err := client.FinalizeCreate(ctx, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
}
func TestCreationTimeoutKeepsBackendAndRebindsOnlyRetiredPod(t *testing.T) {
	client, node, _, kube, state := fixture(t)
	ctx := context.Background()
	if err := client.CreateCopy(ctx, *state.CurrentCopy); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait=%v", err)
	}
	pending, _ := client.Volumes.Get(ctx, testID)
	if pending.Phase != volumeapi.PhaseNodeCreating || pending.CreationExecutor == nil {
		t.Fatalf("lost binding=%+v", pending)
	}
	replacement := node.Identity
	replacement.PodUID = "new-pod-uid"
	pod, _ := kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
	pod.Name = "node-b"
	pod.UID = types.UID(replacement.PodUID)
	replacement.PodName = pod.Name
	_, _ = kube.CoreV1().Pods("shiftpv").Create(ctx, pod, metav1.CreateOptions{})
	if err := client.rebindCreation(ctx, pending); err != nil {
		t.Fatal(err)
	}
	still, _ := client.Volumes.Get(ctx, testID)
	if *still.CreationExecutor != node.Identity {
		t.Fatal("rebound live Pod")
	}
	_ = kube.CoreV1().Pods("shiftpv").Delete(ctx, "node-a", metav1.DeleteOptions{})
	if err := client.rebindCreation(ctx, pending); err != nil {
		t.Fatal(err)
	}
	rebound, _ := client.Volumes.Get(ctx, testID)
	if *rebound.CreationExecutor != replacement {
		t.Fatalf("rebind=%+v", rebound)
	}
	if err := client.Volumes.RecordCreationReceipt(ctx, pending, creationReceipt(node.Identity)); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("stale Pod receipt=%v", err)
	}

}
func TestDiscoveryRequiresExactDaemonSetAndCapability(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"old node", func(p *corev1.Pod) { delete(p.Annotations, capabilityAnnotation) }},
		{"owner", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "other-ds" }},
		{"namespace account", func(p *corev1.Pod) { p.Spec.ServiceAccountName = "other" }},
		{"not ready", func(p *corev1.Pod) { p.Status.Conditions = nil }},
		{"wrong node", func(p *corev1.Pod) { p.Spec.NodeName = "worker-b" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, _, _, kube, _ := fixture(t)
			ctx := context.Background()
			pod, _ := kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
			test.change(pod)
			_, _ = kube.CoreV1().Pods("shiftpv").Update(ctx, pod, metav1.UpdateOptions{})
			executor, err := client.Discovery.Find(ctx, "worker-a")
			if err != nil || executor != nil {
				t.Fatalf("discovery=%+v err=%v", executor, err)
			}
		})
	}
}
func TestCleanupTimeoutCannotFallBack(t *testing.T) {
	client, node, _, kube, state := fixture(t)
	ctx := context.Background()
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
	current, err := client.Reclaim(ctx, cleanup, node.Cleanups)
	if !errors.Is(err, context.DeadlineExceeded) || current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode {
		t.Fatalf("cleanup=%+v err=%v", current, err)
	}
	_, err = client.Reclaim(ctx, current, node.Cleanups)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry=%v", err)
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() == "create" {
			t.Fatalf("fallback create action: %v", action)
		}
	}
	if current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Receipt != nil {
		t.Fatal("unproven cleanup settled")
	}
}

func TestCleanupRebindingCannotChangeBackendOrReceipt(t *testing.T) {
	client, node, _, _, state := fixture(t)
	ctx := context.Background()
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
	current, _ := node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	legacy := current.Status
	legacy.Executor = &cleanupapi.Executor{JobName: "legacy", JobUID: "job-uid", PodUID: "legacy-pod", NodeName: node.Identity.NodeName}
	if err := node.Cleanups.UpdateStatus(ctx, current, legacy); !errors.Is(err, cleanupapi.ErrConflict) {
		t.Fatalf("backend switched: %v", err)
	}
	next := current.Status
	executor := *next.Executor
	executor.PodName = "node-b"
	executor.PodUID = "new-uid"
	next.Executor = &executor
	if err := node.Cleanups.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	stale := current.Status
	stale.Phase = cleanupapi.PhaseVerifying
	stale.Receipt = &cleanupapi.Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: node.Identity.PodUID, ObservedAt: creationReceipt(node.Identity).ObservedAt, Retired: true, Purged: true, LocalReceiptDigest: testDigest}
	if err := node.Cleanups.UpdateStatus(ctx, current, stale); !errors.Is(err, cleanupapi.ErrConflict) {
		t.Fatalf("stale receipt accepted: %v", err)
	}
	current, _ = node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	verified := current.Status
	verified.Phase = cleanupapi.PhaseVerifying
	receipt := *stale.Receipt
	receipt.ExecutorUID = executor.PodUID
	verified.Receipt = &receipt
	if err := node.Cleanups.UpdateStatus(ctx, current, verified); err != nil {
		t.Fatal(err)
	}
	current, _ = node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	next = current.Status
	changed := *next.Executor
	changed.PodUID = "another"
	next.Executor = &changed
	if err := node.Cleanups.UpdateStatus(ctx, current, next); !errors.Is(err, cleanupapi.ErrConflict) {
		t.Fatalf("post receipt rebind: %v", err)
	}
}

func TestLegacyAndUnsupportedNodeChooseHelperBeforeBinding(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			client, _, _, kube, state := fixture(t)
			ctx := context.Background()
			if existing {
				sum := sha256.Sum256([]byte(state.CreationOperationID + "\x00" + state.CurrentCopy.CopyID))
				_, _ = kube.CoreV1().Pods("shiftpv").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("shiftpv-create-%x", sum[:16]), Namespace: "shiftpv", UID: "helper-uid"}}, metav1.CreateOptions{})
			} else {
				pod, _ := kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
				delete(pod.Annotations, capabilityAnnotation)
				_, _ = kube.CoreV1().Pods("shiftpv").Update(ctx, pod, metav1.UpdateOptions{})
			}
			legacy, err := client.prepareCreation(ctx, state, *state.CurrentCopy)
			if err != nil || !legacy {
				t.Fatalf("helper routing=%v err=%v", legacy, err)
			}
			current, _ := client.Volumes.Get(ctx, testID)
			if current.CreationExecutor != nil || current.Phase != volumeapi.PhasePending {
				t.Fatal("helper path changed durable backend")
			}
		})
	}
}
