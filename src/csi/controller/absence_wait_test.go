package controller

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestDeleteVolumeWaitsForCausalProofWithinOneCall(t *testing.T) {
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-1023456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
	registry := &retryDeleteVolumeRegistry{state: volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName,
		CurrentCopy: &copy, CapacityBytes: 64 << 20}, exists: true, deleteCalls: 1}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds,
		cleanupParentVolume(copy.VolumeID, copy), cleanupPool(copy, true))
	// Model the Node completing its post-receipt scan before the next poll.
	api.PrependReactor("update", "shiftpvpools", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if action.GetSubresource() == "" {
			object.SetGeneration(object.GetGeneration() + 1)
			_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
			_ = unstructured.SetNestedSlice(object.Object, []any{}, "status", "inventory", "copies")
		}
		return false, nil, nil
	})
	operator := &verifyingCleanupOperator{}
	service := &Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: &cleanupapi.Store{Client: api}, CleanupOperator: operator, CleanupAbsenceWait: time.Second}
	if _, err := service.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: copy.VolumeID}); err != nil {
		t.Fatal(err)
	}
	if registry.exists || registry.finalizersRemoved != 1 || operator.calls != 1 {
		t.Fatalf("delete did not settle in one call: exists=%t finalizers=%d effects=%d", registry.exists, registry.finalizersRemoved, operator.calls)
	}
}
