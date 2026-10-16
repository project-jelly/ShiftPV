package controller

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// newTestReconciler builds the Reconciler configuration the mobility tests
// share. Anything beyond it is an explicit option so each test still reads as
// the exact configuration it exercises.
func newTestReconciler(client kubernetes.Interface, repository Repository, options ...func(*Reconciler)) *Reconciler {
	reconciler := &Reconciler{Client: client, Repository: repository, Namespace: "system", HelperImage: "helper", SourceUsage: fakeMoveCapacityProbe{}, PoolLocks: &poolcapacity.Locker{}, PlacementPriorityClassName: DefaultPlacementPriorityClassName}
	for _, option := range options {
		option(reconciler)
	}
	return reconciler
}

// withTestCleanups gives the reconciler its own cleanup store and the service
// account the helper jobs project.
func withTestCleanups() func(*Reconciler) {
	return func(r *Reconciler) {
		r.ServiceAccountName = "shiftpv-controller"
		r.Cleanups = newTestCleanupStore()
		r.CleanupOperator = receiptCleanupOperator{}
	}
}

// recordingCleanupOperator counts new executor requests without fabricating a
// receipt. Receipt-bearing fixture journals explicitly model a settled executor;
// actual live Job/Pod settlement is covered by cleanup_job_reentry_test.go.
type recordingCleanupOperator struct{ reclaims, observations int }

func (o *recordingCleanupOperator) ObserveCompletedCleanup(_ context.Context, current cleanupapi.Cleanup) (bool, error) {
	o.observations++
	err := cleanupapi.ValidateSettled(current)
	return err == nil, err
}

func (o *recordingCleanupOperator) AdvanceReclaim(_ context.Context, current cleanupapi.Cleanup, _ cleanupapi.Journal) (cleanupapi.Cleanup, bool, error) {
	if cleanupReceiptComplete(current, current.Spec) {
		o.observations++
		return current, true, nil
	}
	o.reclaims++
	return cleanupapi.Cleanup{}, false, fmt.Errorf("cleanup executor was requested")
}

// withRecordingCleanupOperator replaces the receipt-fabricating stub so a test
// can assert that no executor was requested at all.
func withRecordingCleanupOperator(operator *recordingCleanupOperator) func(*Reconciler) {
	return func(r *Reconciler) { r.CleanupOperator = operator }
}

// withCleanupsFrom shares one cleanup store across reconcilers, which is how a
// restart test keeps the persisted cleanup intents it already wrote.
func withCleanupsFrom(other *Reconciler) func(*Reconciler) {
	return func(r *Reconciler) {
		r.Cleanups = other.Cleanups
		r.CleanupOperator = other.CleanupOperator
	}
}

func assignJobUIDs(client *fake.Clientset) {
	for _, resource := range []string{"jobs", "pods", "services", "configmaps", "secrets"} {
		client.PrependReactor("create", resource, func(action ktesting.Action) (bool, runtime.Object, error) {
			object := action.(ktesting.CreateAction).GetObject().(metav1.Object)
			if object.GetUID() == "" {
				object.SetUID(types.UID(object.GetName() + "-uid"))
			}
			return false, nil, nil
		})
	}
}

func testCopyIdentities(volumeID, sourceNode, destinationNode string) (volume.CopyIdentity, volume.CopyIdentity, volume.CopyIdentity) {
	source := volume.CopyIdentity{
		InstallationID: "test-installation", PoolName: sourceNode + "-pool", PoolUID: sourceNode + "-pool-uid",
		VolumeID: volumeID, VolumeUID: "test-volume-uid", CopyID: "test-copy-" + sourceNode,
		NodeName: sourceNode, Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: destinationNode + "-pool", PoolUID: destinationNode + "-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "test-incoming-" + destinationNode,
		NodeName: destinationNode, Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.CopyID = "test-copy-" + destinationNode
	destination.Role = volume.RoleServing
	return source, incoming, destination
}

func newTestCleanupStore() *cleanupapi.Store {
	parent := func(name, uid string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
			"metadata": map[string]any{
				"name": name, "uid": uid, "resourceVersion": "1", "generation": int64(1),
				"finalizers": []any{volumeapi.MoveProtectionFinalizer},
			},
			"spec": map[string]any{},
		}}
	}
	pool := func(name, uid, node string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
			"metadata": map[string]any{
				"name": name, "uid": uid, "resourceVersion": "1", "generation": int64(1),
				"finalizers": []any{volumeapi.PoolProtectionFinalizer},
			},
			"spec": map[string]any{"nodeName": node, "scanEpoch": int64(0)},
			"status": map[string]any{
				"observedGeneration": int64(1),
				"inventory":          map[string]any{"valid": true, "truncated": false, "message": "", "copies": []any{}},
			},
		}}
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList",
		volumeapi.MoveResource:   "ShiftPVMoveList",
		volumeapi.PoolResource:   "ShiftPVPoolList",
	}, parent("move-test", "move-uid"), parent("move-cleanup", "move-cleanup-uid"),
		pool("source-pool", "source-pool-uid", "source"), pool("source", "source-pool-uid", "source"),
		pool("destination-pool", "destination-pool-uid", "destination"))
	client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		update := action.(ktesting.UpdateAction)
		if update.GetSubresource() == "" {
			object := update.GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
			_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
		}
		return false, nil, nil
	})
	return &cleanupapi.Store{Client: client}
}
