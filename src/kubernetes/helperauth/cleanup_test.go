package helperauth

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestVolumeCleanupAuthorityRequiresDurableDeletionFence(t *testing.T) {
	const volumeID = "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &volumeapi.Registry{Client: dynamicClient}
	cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
		OperationID: "delete-" + copy.VolumeUID, Target: copy, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: volumeID, UID: copy.VolumeUID},
	}}
	setVolumeStateFixture(t, dynamicClient, volumeID, volumeapi.State{Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy})
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
		t.Fatal("Ready volume was accepted without a deletion fence")
	}
	if _, err := registry.BeginDelete(context.Background(), volumeID, copy.VolumeUID, copy); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err != nil {
		t.Fatalf("durably fenced deletion was rejected: %v", err)
	}
	state, err := registry.Get(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	state.PublishedNodes = []string{copy.NodeName}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
		t.Fatal("published volume crossed deletion authority")
	}
}

func TestUnknownOrphanCleanupHasNoExecutableAuthority(t *testing.T) {
	cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
		OperationID: "review-orphan", Reason: "OrphanReclaim",
		Target: volume.CopyIdentity{
			InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
			VolumeID: "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", VolumeUID: "volume-uid",
			CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
		},
		Authority: cleanupapi.Authority{Kind: "Namespace", Name: "kube-system", UID: "installation"},
	}}
	if err := cleanup.Spec.Validate(); err == nil {
		t.Fatal("orphan cleanup intent unexpectedly validated")
	}
	if err := VerifyCleanupAuthority(context.Background(), nil, cleanup); err == nil {
		t.Fatal("unknown orphan observation gained destructive authority")
	}
}

func TestRecoveryCleanupAuthorityRequiresExactTargetAndRetainedOwner(t *testing.T) {
	const (
		volumeID = "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		moveName = "move-recovery"
		moveUID  = "move-recovery-uid"
	)
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.Role = volume.RoleServing

	for name, test := range map[string]struct {
		reason, operationID, recoveryOwner string
		target, current                    volume.CopyIdentity
	}{
		"precommit rollback": {
			reason: "MoveRollback", operationID: "rollback-" + moveUID, recoveryOwner: source.NodeName,
			target: incoming, current: source,
		},
		"postcommit source cleanup": {
			reason: "MoveSource", operationID: "cleanup-" + moveUID, recoveryOwner: destination.NodeName,
			target: source, current: destination,
		},
	} {
		t.Run(name, func(t *testing.T) {
			moveObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
				"metadata": map[string]any{"name": moveName, "uid": moveUID},
				"spec":     map[string]any{"volumeID": volumeID, "sourceNode": source.NodeName, "recovery": "ResumeOwner", "protocolVersion": volumeapi.MoveProtocolPreparedV1},
			}}
			volumeObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
				"metadata": map[string]any{"name": volumeID, "uid": source.VolumeUID},
				"spec":     map[string]any{"volumeID": volumeID},
			}}
			poolObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
				"metadata": map[string]any{
					"name": destination.PoolName, "uid": destination.PoolUID, "generation": int64(1),
					"finalizers": []any{volumeapi.PoolProtectionFinalizer},
				},
				"spec": map[string]any{"nodeName": destination.NodeName, "mountPath": "/destination-pool", "capacity": map[string]any{"limit": "1Gi"}},
			}}
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
			}, moveObject, volumeObject, poolObject)
			registry := &volumeapi.Registry{Client: client}
			moveStatus := volumeapi.MoveStatus{
				Phase: "Blocked", RecoveryPhase: "Retiring", RecoveryOwner: test.recoveryOwner,
				DestinationNode: destination.NodeName, DestinationPoolUID: destination.PoolUID,
				SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			}
			setMoveStatusFixture(t, client, moveName, moveStatus)
			statePhase := volumeapi.PhaseBlocked
			if test.reason == "MoveSource" {
				statePhase = volumeapi.PhaseReady
			}
			state := volumeapi.State{
				UID: source.VolumeUID, Phase: statePhase, OwnerNode: test.current.NodeName,
				ActiveMove: moveName, CurrentCopy: &test.current, PublishedNodes: []string{test.current.NodeName},
			}
			setVolumeStateFixture(t, client, volumeID, state)
			if test.reason == "MoveSource" {
				now := metav1.Now()
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         cleanupBackingConditions(),
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
				OperationID: test.operationID, Target: test.target, Reason: test.reason,
				Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: moveName, UID: moveUID},
			}}
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err != nil {
				t.Fatalf("exact recovery cleanup rejected: %v", err)
			}
			if test.reason == "MoveSource" {
				state.PublishedNodes = nil
				setVolumeStateFixture(t, client, volumeID, state)
				if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
					t.Fatal("postcommit source cleanup was accepted before destination publish intent")
				}
				state.PublishedNodes = []string{test.current.NodeName}
				setVolumeStateFixture(t, client, volumeID, state)
				now := metav1.Now()
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         cleanupBackingConditions(),
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: false}},
					},
				}); err != nil {
					t.Fatal(err)
				}
				if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
					t.Fatal("postcommit source cleanup was accepted before scanner publication proof")
				}
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         cleanupBackingConditions(),
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			unknownReason := cleanup
			unknownReason.Spec.Reason = "MoveRelocate"
			if err := VerifyCleanupAuthority(context.Background(), registry, unknownReason); err == nil {
				t.Fatal("unimplemented move cleanup reason was accepted")
			}
			cleanup.Spec.OperationID = "replacement-operation"
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
				t.Fatal("replacement cleanup operation was accepted")
			}
			cleanup.Spec.OperationID = test.operationID
			state.PublishedNodes = append(state.PublishedNodes, test.target.NodeName)
			setVolumeStateFixture(t, client, volumeID, state)
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
				t.Fatal("published cleanup target was accepted")
			}
		})
	}
}

// TestMoveSourceCleanupAuthorityHonorsOperatorPoolReadinessBudget is the
// motivating case for forwarding --pool-readiness-stale-after to the cleanup
// helper. The destination Pool probe is older than the operator's tightened
// budget but still inside the compiled default, so a helper Registry left at
// zero accepts a publication proof the controller already treats as stale and
// would purge the retained source copy on it.
//
// This pins the consumer half of the contract and holds on its own: it builds
// the Registry directly, so it does not exercise the CLI plumbing. The other
// half — that the parsed flag actually reaches this Registry, and that the
// controller puts the flag on the cleanup Job — is pinned by
// TestHelperForwardsPoolReadinessBudgetToRegistry and
// TestCleanupJobForwardsPoolReadinessBudget.
func TestMoveSourceCleanupAuthorityHonorsOperatorPoolReadinessBudget(t *testing.T) {
	const (
		volumeID = "shiftpv-dddddddddddddddddddddddddddddddd"
		moveName = "move-stale-budget"
		moveUID  = "move-stale-budget-uid"
	)
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.Role = volume.RoleServing

	moveObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": moveName, "uid": moveUID, "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": volumeID, "sourceNode": source.NodeName, "recovery": "ResumeOwner", "protocolVersion": volumeapi.MoveProtocolPreparedV1},
	}}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": source.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	sourcePoolObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": source.PoolName, "uid": source.PoolUID, "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": source.NodeName, "mountPath": "/source-pool"},
	}}
	destinationPoolObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{
			"name": destination.PoolName, "uid": destination.PoolUID, "generation": int64(1),
			"finalizers": []any{volumeapi.PoolProtectionFinalizer},
		},
		"spec": map[string]any{"nodeName": destination.NodeName, "mountPath": "/destination-pool", "capacityPolicy": volumeapi.PoolCapacityPolicyFixedBlock, "capacity": map[string]any{"limit": "1Gi"}},
	}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "kube-system", "uid": source.InstallationID},
	}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
		volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, moveObject, volumeObject, sourcePoolObject, destinationPoolObject, clusterIdentity)

	// The probe and inventory are two minutes old: fresh under the compiled
	// three-minute default, stale under the one-minute budget the operator set.
	probeAt := metav1.NewTime(time.Now().Truncate(time.Second))
	observed := probeAt.Time.Add(2 * time.Minute)

	setup := &volumeapi.Registry{Client: client}
	moveStatus := volumeapi.MoveStatus{
		Phase: "Blocked", RecoveryPhase: "Retiring", RecoveryOwner: destination.NodeName,
		DestinationNode: destination.NodeName, DestinationPoolUID: destination.PoolUID,
		SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
	}
	setMoveStatusFixture(t, client, moveName, moveStatus)
	setVolumeStateFixture(t, client, volumeID, volumeapi.State{
		UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: destination.NodeName,
		ActiveMove: moveName, CurrentCopy: &destination, PublishedNodes: []string{destination.NodeName},
	})
	conditions := cleanupBackingConditions()
	conditions[0].Status, conditions[0].Reason = metav1.ConditionFalse, "CapacityPeerUnproven"
	conditions = append(conditions,
		metav1.Condition{Type: volumeapi.PoolConditionCapacityBackingVerified, Status: metav1.ConditionTrue, ObservedGeneration: 1},
		metav1.Condition{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: "CapacityPeerUnproven"})
	if err := setup.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
		ObservedGeneration: 1,
		LastProbeTime:      probeAt,
		Conditions:         conditions,
		CapacityUnit: &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4",
			Extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}},
		Inventory: &volumeapi.PoolInventory{
			ObservedAt: probeAt, Valid: true,
			Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	spec := cleanupapi.Spec{
		OperationID: "cleanup-" + moveUID, Target: source, Reason: "MoveSource",
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: moveName, UID: moveUID},
	}
	approved := cleanupapi.Cleanup{Spec: spec}

	registryAt := func(staleAfter time.Duration) *volumeapi.Registry {
		return &volumeapi.Registry{
			Client:                  client,
			PoolReadinessStaleAfter: staleAfter,
			Now:                     func() time.Time { return observed },
		}
	}
	if _, err := registryAt(0).ReadyPoolForIdentity(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName); err == nil {
		t.Fatal("peer failure admitted new destination allocation")
	}
	// The existing published copy remains proven even while peer uncertainty
	// prevents new allocation on its Pool.
	if err := VerifyCleanupAuthority(context.Background(), registryAt(0), approved); err != nil {
		t.Fatalf("publication proof inside the compiled default was rejected: %v", err)
	}
	if err := VerifyCleanupAuthority(context.Background(), registryAt(volumeapi.DefaultPoolReadinessStaleAfter), approved); err != nil {
		t.Fatalf("publication proof inside the explicit default budget was rejected: %v", err)
	}
	err := VerifyCleanupAuthority(context.Background(), registryAt(time.Minute), approved)
	if err == nil {
		t.Fatal("publication proof older than the operator budget kept destructive authority")
	}
	if !errors.Is(err, volumeapi.ErrPoolNotReady) {
		t.Fatalf("stale publication proof was rejected for the wrong reason: %v", err)
	}
	baseline, err := client.Resource(volumeapi.PoolResource).Get(context.Background(), destination.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*unstructured.Unstructured){
		"own backing failed": func(object *unstructured.Unstructured) {
			conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
			conditions[4].(map[string]any)["status"] = "False"
			_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
		},
		"outdated generation": func(object *unstructured.Unstructured) { object.SetGeneration(2) },
		"unprotected Pool":    func(object *unstructured.Unstructured) { object.SetFinalizers(nil) },
		"replaced Pool":       func(object *unstructured.Unstructured) { object.SetUID("replacement") },
		"invalid inventory": func(object *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(object.Object, false, "status", "inventory", "valid")
		},
		"truncated inventory": func(object *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(object.Object, true, "status", "inventory", "truncated")
		},
		"stale inventory": func(object *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(object.Object, observed.Add(-4*time.Minute).Format(time.RFC3339), "status", "inventory", "observedAt")
		},
		"different published copy": func(object *unstructured.Unstructured) {
			copies, _, _ := unstructured.NestedSlice(object.Object, "status", "inventory", "copies")
			copies[0].(map[string]any)["identity"].(map[string]any)["copyID"] = "different-copy"
			_ = unstructured.SetNestedSlice(object.Object, copies, "status", "inventory", "copies")
		},
	} {
		t.Run(name, func(t *testing.T) {
			object := baseline.DeepCopy()
			change(object)
			if _, err := client.Resource(volumeapi.PoolResource).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := VerifyCleanupAuthority(context.Background(), registryAt(0), approved); err == nil {
				t.Fatal("lost exact destination proof authorized source deletion")
			}
		})
	}
}

func cleanupBackingConditions() []metav1.Condition {
	conditions := []metav1.Condition{}
	for _, kind := range []string{volumeapi.PoolConditionReady, volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
		conditions = append(conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: 1})
	}
	return conditions
}
