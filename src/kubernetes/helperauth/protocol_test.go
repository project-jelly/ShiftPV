package helperauth

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	typedfake "k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Historical records remain decodable but cannot authorize helper execution.
// Only a fresh Move GET may happen before rejecting their protocol.
func TestRetiredMoveProtocolsCannotAuthorizeHelpers(t *testing.T) {
	for _, protocol := range []string{"", "future-v2"} {
		for _, action := range []string{"copy", "promote", "serve-source", "verify-owner", "MoveSource", "MoveRollback"} {
			t.Run(protocol+"/"+action, func(t *testing.T) {
				const id = "shiftpv-0123456789abcdef0123456789abcdef"
				source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", NodeName: "source", VolumeID: id, VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing}
				incoming, destination := volumeapi.MoveTransactionCopies("move-uid", id, volumeapi.MoveDestinationAnchor{InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid", NodeName: "destination", VolumeUID: source.VolumeUID})
				object := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
					"metadata": map[string]any{"name": "move-test", "uid": "move-uid", "resourceVersion": "1"},
					"spec":     map[string]any{"protocolVersion": protocol, "volumeID": id, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
				}}
				client := fake.NewSimpleDynamicClient(runtime.NewScheme(), object)
				status := volumeapi.MoveStatus{Phase: "Copying", SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination, CopyOperationID: "copy-move-uid", CopyJobName: "copy-job", PromotionJobName: "promote-job", PromotionOperationID: "promote-move-uid", DestinationNode: destination.NodeName, DestinationPoolUID: destination.PoolUID}
				options := MoveOptions{MoveName: "move-test", MoveUID: "move-uid", OperationID: status.CopyOperationID, Namespace: "system", PodName: "helper"}
				if action == "verify-owner" {
					status.Phase = "Blocked"
					status.RecoveryPhase = "Verifying"
					status.RecoveryOwner = source.NodeName
				}
				setMoveStatusFixture(t, client, options.MoveName, status)
				registry := &volumeapi.Registry{Client: client}
				typed := typedfake.NewSimpleClientset()
				client.ClearActions()
				var err error
				switch action {
				case "copy", "promote":
					if action == "promote" {
						options.OperationID = status.PromotionOperationID
					}
					err = MoveAuthority(typed, registry, options, action, incoming)(context.Background())
				case "serve-source":
					err = SourceAuthority(typed, registry, options, source)(context.Background())
				case "verify-owner":
					err = RecoveryAuthority(typed, registry, options, source)(context.Background())
				default:
					target, operation := source, volumeapi.MoveCleanupOperationID(options.MoveUID)
					if action == "MoveRollback" {
						target = incoming
						operation = volumeapi.MoveRollbackOperationID(options.MoveUID)
					}
					err = VerifyCleanupAuthority(context.Background(), registry, cleanupapi.Cleanup{Spec: cleanupapi.Spec{OperationID: operation, Target: target, Reason: action, Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: options.MoveName, UID: options.MoveUID}}})
				}
				if !errors.Is(err, volumeapi.ErrStateConflict) {
					t.Fatalf("retired protocol reached helper authority: %v", err)
				}
				if len(typed.Actions()) != 0 {
					t.Fatal("retired protocol queried an executor")
				}
				actions := client.Actions()
				if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource() != volumeapi.MoveResource {
					t.Fatalf("retired protocol reached other authority or mutation: %#v", actions)
				}
				after, getErr := client.Resource(volumeapi.MoveResource).Get(context.Background(), options.MoveName, metav1.GetOptions{})
				if getErr != nil || after.GetResourceVersion() != "1" {
					t.Fatalf("history changed: %v %v", after, getErr)
				}
			})
		}
	}
}
