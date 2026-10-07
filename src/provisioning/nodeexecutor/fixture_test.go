package nodeexecutor

import (
	"context"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"testing"
	"time"
)

const testID = "shiftpv-0123456789abcdef0123456789abcdef"
const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func fixture(t *testing.T) (*Client, *Node, *dynamicfake.FakeDynamicClient, *kubefake.Clientset, volumeapi.State) {
	t.Helper()
	executor := volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-uid", NodeName: "worker-a"}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: testID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: executor.NodeName, Role: volume.RoleServing}
	parent := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": testID, "uid": "volume-uid", "finalizers": []any{volumeapi.VolumeProtectionFinalizer}}, "spec": map[string]any{"requestName": "pvc", "initialNode": "worker-a", "capacityBytes": int64(1024)}, "status": map[string]any{"phase": "Pending", "ownerNode": "worker-a", "creationOperationID": "create-volume-uid"}}}
	encoded, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	_ = unstructured.SetNestedMap(parent.Object, encoded, "status", "currentCopy")
	identity := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"}}}
	pool := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": "pool-a", "uid": "pool-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}}, "spec": map[string]any{"nodeName": "worker-a", "mountPath": "/mnt/pool", "capacity": map[string]any{"limit": "1Gi"}}, "status": map[string]any{"observedGeneration": int64(1), "inventory": map[string]any{"valid": true, "observedAt": time.Now().UTC().Format(time.RFC3339Nano)}, "conditions": []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(1), "lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano), "reason": "PoolReady"}}, "lastProbeTime": time.Now().UTC().Format(time.RFC3339Nano)}}}
	dynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.MoveResource: "ShiftPVMoveList"}, parent, pool, identity)
	controller := true
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shiftpv", Name: "shiftpv-node", UID: "ds-uid"}, Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "node"}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: executor.Namespace, Name: executor.PodName, UID: types.UID(executor.PodUID), Annotations: map[string]string{capabilityAnnotation: "v1"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: executor.NodeName, ServiceAccountName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	kube := kubefake.NewClientset(ds, pod)
	registry := &volumeapi.Registry{Client: dynamic}
	discovery := Discovery{Client: kube, Namespace: "shiftpv", DaemonSet: ds.Name}
	helper := &helperpod.Runner{Client: kube, Namespace: "shiftpv", Pools: registry, Image: "helper:test", Timeout: time.Second}
	client := &Client{Discovery: discovery, Volumes: registry, Fallback: helper, Timeout: 10 * time.Millisecond}
	node := &Node{Identity: executor, Discovery: discovery, Volumes: registry, Cleanups: &cleanupapi.Store{Client: dynamic}, HostRoot: "/host"}
	state, err := registry.Get(context.Background(), testID)
	if err != nil {
		t.Fatal(err)
	}
	return client, node, dynamic, kube, state
}
func creationReceipt(executor volumeapi.NodeExecutor) volumeapi.CreationReceipt {
	return volumeapi.CreationReceipt{OperationID: "create-volume-uid", ExecutorUID: executor.PodUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), LocalReceiptDigest: testDigest}
}
