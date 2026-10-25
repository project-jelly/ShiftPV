package helperauth

import (
	"context"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestCreationCancellationAuthorityRequiresOriginalCreationFence(t *testing.T) {
	ctx := context.Background()
	const id = "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: id, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "worker-a", Role: volume.RoleServing}
	for _, change := range []string{"none", "normal delete reason", "owner", "operation", "publication", "Ready", "initial node", "unbound receipt", "protection"} {
		t.Run(change, func(t *testing.T) {
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
				"metadata": map[string]any{"name": id, "uid": copy.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
				"spec":     map[string]any{"requestName": "pvc-original", "initialNode": copy.NodeName, "capacityBytes": int64(1024)},
			}}
			api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList"}, object)
			r := &volumeapi.Registry{Client: api}
			setVolumeStateFixture(t, api, id, volumeapi.State{Phase: volumeapi.PhasePending, OwnerNode: copy.NodeName, CurrentCopy: &copy, CreationOperationID: "create-" + copy.VolumeUID})
			state, err := r.BeginCreationCancellation(ctx, id, copy.VolumeUID, copy)
			if err != nil {
				t.Fatal(err)
			}
			cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: copy, Reason: "CreationCancelled", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: id, UID: copy.VolumeUID}}}
			switch change {
			case "normal delete reason":
				cleanup.Spec.Reason = "VolumeDelete"
			case "owner":
				state.OwnerNode = "worker-b"
			case "operation":
				state.CreationOperationID = "create-foreign"
			case "publication":
				state.PublishedNodes = []string{copy.NodeName}
			case "Ready":
				state.Phase = volumeapi.PhaseReady
			case "initial node":
				_ = unstructured.SetNestedField(object.Object, "worker-b", "spec", "initialNode")
				// State updates below preserve spec from the live tracker.
				if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
					t.Fatal(err)
				}
			case "unbound receipt":
				state.CreationReceipt = &volumeapi.CreationReceipt{}
			case "protection":
				object.SetFinalizers(nil)
				if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
					t.Fatal(err)
				}
			}
			setVolumeStateFixture(t, api, id, state)
			err = VerifyCleanupAuthority(ctx, r, cleanup)
			if change == "none" && err != nil {
				t.Fatal(err)
			}
			if change != "none" && err == nil {
				t.Fatalf("%s gained destructive authority", change)
			}
		})
	}
}
