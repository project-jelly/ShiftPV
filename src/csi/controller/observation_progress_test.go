package controller

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/provisioning"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"
)

type observationProgressInspector struct{}

func (observationProgressInspector) Inspect(volumeapi.Pool) readiness.Result {
	ok := readiness.Check{Known: true, OK: true, Reason: "OK"}
	return readiness.Result{Accessible: ok, Writable: ok, CapacityReadable: ok}
}
func (observationProgressInspector) InspectCapacityUnit(volumeapi.Pool) (*volumeapi.PoolCapacityUnit, readiness.Check) {
	return nil, readiness.Check{}
}

// Join both resident loops through the real Registry and informer. The only
// injected failure is a changing inventory; the test never drives Reconcile.
func TestObservationRecoveryConnectsScannerToWaitingCSIClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool", "uid": "pool-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "worker-a", "mountPath": "/pool", "poolGroup": "default", "capacity": map[string]any{"limit": "1Gi"}},
	}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
	}, pool)
	registry := &volumeapi.Registry{Client: api}
	kube := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}}, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test", Name: "claim", UID: types.UID("uid"), Annotations: map[string]string{selectedNodeAnnotation: "worker-a"},
	}})
	retries := provisioning.NewRetries(kube, registry)
	service := capacityService(kube, "1Gi", nil, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}})
	service.CapacityPools, service.RetryRequests = registry, retries
	c := clocktesting.NewFakeClock(time.Now())
	node, err := readiness.NewReconciler("worker-a", registry, observationProgressInspector{}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	node.Clock = c
	passes := make(chan struct{}, 4)
	scans := 0
	node.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		scans++
		if scans == 1 {
			return volumeapi.PoolInventory{Reason: volumeapi.InventoryPhysicalChanged, Message: volumeapi.InventoryPhysicalChanged, ObservedAt: metav1.Now()}
		}
		return volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.Now()}
	}
	node.ObserveAll = func(_ []readiness.Observation, err error) {
		if err != nil {
			t.Errorf("Pool observation: %v", err)
		}
		passes <- struct{}{}
	}
	nodeDone, retriesDone := make(chan struct{}), make(chan struct{})
	go func() { _ = node.Run(ctx); close(nodeDone) }()
	go func() { retries.Run(ctx, api); close(retriesDone) }()
	defer func() {
		cancel()
		for _, done := range []chan struct{}{nodeDone, retriesDone} {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("resident loop did not stop")
			}
		}
	}()
	select {
	case <-passes:
	case <-time.After(3 * time.Second):
		t.Fatal("initial scan missing")
	}
	request := validCreateRequest("worker-a")
	request.Parameters = map[string]string{PVCNamespaceKey: "test", PVCNameKey: "claim"}
	if _, err := service.CreateVolume(ctx, request); err == nil {
		t.Fatal("invalid inventory admitted a volume")
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 3*time.Second, true, func(context.Context) (bool, error) {
		return c.Waiters() == 2, nil
	}); err != nil {
		t.Fatal("missing automatic rescan timer", err)
	}
	c.Step(time.Second)
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		claim, err := kube.CoreV1().PersistentVolumeClaims("test").Get(ctx, "claim", metav1.GetOptions{})
		return err == nil && claim.Annotations["shiftpv.io/capacity-retry"] != "", err
	}); err != nil {
		t.Fatal("automatic observation recovery did not notify CSI retry", err)
	}
	if _, err := service.CreateVolume(ctx, request); err != nil {
		t.Fatalf("notified request did not pass fresh admission: %v", err)
	}
}
