package helperauth

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestRetiredWorkloadCleanupUsesExactUnpublishedDestinationAuthority(t *testing.T) {
	for _, scenario := range []string{"valid", "no intent", "before verification", "changed Move UID", "published", "pending scan", "old scan fence", "stale inventory", "wrong copy", "wrong cleanup target"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			const id = "shiftpv-0123456789abcdef0123456789abcdef"
			source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", NodeName: "source",
				VolumeID: id, VolumeUID: "volume-uid", CopyID: "initial-copy", Role: volume.RoleServing}
			incoming, destination := volumeapi.MoveTransactionCopies("move-uid", id, volumeapi.MoveDestinationAnchor{
				InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid", NodeName: "destination", VolumeUID: source.VolumeUID})
			move := volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Retiring", RecoveryOwner: "destination", SettlementRequested: true,
				PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid", ClaimNamespace: "workload", ClaimName: "claim", ClaimUID: "original-claim-uid",
				ConsumerName: "consumer", ConsumerUID: "consumer-uid", DestinationNode: "destination", DestinationPoolUID: destination.PoolUID,
				SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination, RollbackRequiredGeneration: 2,
				CopyOperationID: volumeapi.MoveCopyOperationID("move-uid"), PromotionOperationID: volumeapi.MovePromotionOperationID("move-uid")}
			moveObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": "move", "uid": "move-uid"},
				"spec": map[string]any{"protocolVersion": volumeapi.MoveProtocolPreparedV1, "volumeID": id, "sourceNode": "source"}}}
			volumeObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": id, "uid": source.VolumeUID},
				"spec": map[string]any{"volumeID": id}}}
			poolObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
				"metadata": map[string]any{"name": destination.PoolName, "uid": destination.PoolUID, "generation": int64(2), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
				"spec":     map[string]any{"nodeName": "destination", "mountPath": "/destination", "capacity": map[string]any{"limit": "1Gi"}}}}
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), moveObject, volumeObject, poolObject)
			registry := &volumeapi.Registry{Client: client}
			state := volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseBlocked, OwnerNode: "destination", ActiveMove: "move", CurrentCopy: &destination}
			now := metav1.Now()
			conditions := cleanupBackingConditions()
			for index := range conditions {
				conditions[index].ObservedGeneration = 2
			}
			poolStatus := volumeapi.PoolStatus{ObservedGeneration: 2, LastProbeTime: now, Conditions: conditions,
				Inventory: &volumeapi.PoolInventory{Valid: true, ObservedAt: now, Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true}}}}
			cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{OperationID: volumeapi.MoveCleanupOperationID("move-uid"), Target: source, Reason: "MoveSource",
				Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move", UID: "move-uid"}}}
			switch scenario {
			case "no intent":
				move.SettlementRequested = false
			case "before verification":
				move.RecoveryPhase = "Verifying"
			case "changed Move UID":
				cleanup.Spec.Authority.UID = "different-move-uid"
			case "published":
				state.PublishedNodes = []string{"destination"}
			case "old scan fence":
				move.RollbackRequiredGeneration = 3
			case "stale inventory":
				registry.Now = func() time.Time { return now.Time.Add(4 * time.Minute) }
			case "wrong copy":
				foreign := destination
				foreign.CopyID = "foreign-copy"
				poolStatus.Inventory.Copies[0].Identity = &foreign
			case "wrong cleanup target":
				cleanup.Spec.Target.CopyID = "foreign-source-copy"
			}
			setMoveStatusFixture(t, client, "move", move)
			setVolumeStateFixture(t, client, id, state)
			if err := registry.SetPoolStatus(ctx, destination.PoolName, destination.PoolUID, "destination", poolStatus); err != nil {
				t.Fatal(err)
			}
			if scenario == "pending scan" {
				object, err := client.Resource(volumeapi.PoolResource).Get(ctx, destination.PoolName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				object.SetGeneration(3)
				if err := client.Tracker().Update(volumeapi.PoolResource, object, ""); err != nil {
					t.Fatal(err)
				}
			}
			err := VerifyCleanupAuthority(ctx, registry, cleanup)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("cleanup authority for %s: %v", scenario, err)
			}
		})
	}
}
