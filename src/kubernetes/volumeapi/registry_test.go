package volumeapi

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestStateCASPreservesConcurrentNodePublication(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"})
	r := &Registry{Client: client}
	id := "shiftpv-44444444444444444444444444444444"
	seedReadyVolume(t, client, id, "source")
	object, err := client.Resource(VolumeResource).Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetUID("volume-uid")
	if _, err := client.Resource(VolumeResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	stale, _ := r.Get(ctx, id)
	setPublishedFixture(t, client, id, "source", true)
	stale.Phase, stale.ActiveMove = PhaseMoving, "move"
	if err := r.CompareAndSetState(ctx, id, PhaseReady, "", "source", stale); err != nil {
		t.Fatal(err)
	}
	live, _ := r.Get(ctx, id)
	if !reflect.DeepEqual(live.PublishedNodes, []string{"source"}) {
		t.Fatalf("publish lost: %+v", live)
	}
	stale.Phase, stale.OwnerNode = PhaseReady, "destination"
	if err := r.CompareAndSetState(ctx, id, PhaseMoving, "move", "source", stale); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("foreign publication permitted commit: %v", err)
	}
	setPublishedFixture(t, client, id, "source", false)
	if err := r.CompareAndSetState(ctx, id, PhaseMoving, "move", "source", stale); err != nil {
		t.Fatal(err)
	}
	live, _ = r.Get(ctx, id)
	if len(live.PublishedNodes) != 0 {
		t.Fatalf("unpublish lost: %+v", live)
	}
}

func TestStateCASRejectsReplacementVolumeUID(t *testing.T) {
	ctx := context.Background()
	const id = "shiftpv-55555555555555555555555555555555"
	original := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": id, "uid": "original-uid"},
		"spec":     map[string]any{"volumeID": id},
		"status":   map[string]any{"phase": PhaseReady, "ownerNode": "source", "activeMove": ""},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, original)
	registry := &Registry{Client: client}
	stale, err := registry.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	replacement := original.DeepCopy()
	replacement.SetUID("replacement-uid")
	replacement.SetResourceVersion("")
	if err := client.Resource(VolumeResource).Delete(ctx, id, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(VolumeResource).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	stale.Phase = PhaseMoving
	stale.ActiveMove = "stale-move"
	if err := registry.CompareAndSetState(ctx, id, PhaseReady, "", "source", stale); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Volume CAS error = %v", err)
	}
	current, err := registry.Get(ctx, id)
	if err != nil || current.UID != "replacement-uid" || current.Phase != PhaseReady || current.ActiveMove != "" {
		t.Fatalf("replacement Volume changed: state=%#v err=%v", current, err)
	}
}

func TestRecoveryFieldsRoundTrip(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "move", "uid": "uid"},
		"spec":     map[string]any{"volumeID": "volume", "sourceNode": "source", "recovery": "ResumeOwner"},
	}}
	status := MoveStatus{Phase: "Blocked", ConsumerUID: "consumer-uid", CandidateNodes: []string{}, LastTransitionTime: "2026-09-04T00:00:00Z", LastProgressTime: "2026-09-04T00:00:00Z", RecoveryPhase: "Quiescing", RecoveryOwner: "destination", RecoveryReason: "NodeNotReady", RecoveryMessage: "observe node"}
	setMoveStatus(object, status)
	move, err := moveFrom(object)
	if err != nil || move.Spec.Recovery != "ResumeOwner" || !reflect.DeepEqual(move.Status, status) {
		t.Fatalf("round trip: %+v %v", move, err)
	}
}

func TestStatusEncodersPreserveEmbeddedCleanupJournal(t *testing.T) {
	journal := map[string]any{
		"spec":   map[string]any{"operationID": "cleanup-operation"},
		"status": map[string]any{"phase": "ConfirmingAbsence"},
	}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"cleanup": journal}}}
	setState(volumeObject, State{Phase: PhaseDeleting, OwnerNode: "worker-a"})
	gotVolume, found, err := unstructured.NestedMap(volumeObject.Object, "status", "cleanup")
	if err != nil || !found || !reflect.DeepEqual(gotVolume, journal) {
		t.Fatalf("Volume cleanup journal changed: got=%#v found=%t err=%v", gotVolume, found, err)
	}

	moveObject := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{
		"cleanup": journal, "cleanupName": "legacy-cleanup", "cleanupJobName": "legacy-job",
	}}}
	setMoveStatus(moveObject, MoveStatus{Phase: "Completing"})
	gotMove, found, err := unstructured.NestedMap(moveObject.Object, "status", "cleanup")
	if err != nil || !found || !reflect.DeepEqual(gotMove, journal) {
		t.Fatalf("Move cleanup journal changed: got=%#v found=%t err=%v", gotMove, found, err)
	}
	decoded, err := moveStatusFrom(moveObject)
	if err != nil || decoded.CleanupPhase != "ConfirmingAbsence" {
		t.Fatalf("Move cleanup phase was not decoded: status=%#v err=%v", decoded, err)
	}
	for _, field := range []string{"cleanupName", "cleanupJobName"} {
		if _, found, err := unstructured.NestedString(moveObject.Object, "status", field); err != nil || found {
			t.Fatalf("legacy %s survived Move status encoding: found=%t err=%v", field, found, err)
		}
	}
}

func TestRegistryLifecycleAndPoolNodes(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList",
		PoolResource:   "ShiftPVPoolList",
		MoveResource:   "ShiftPVMoveList",
	}, pool("pool-b", "node-b"), pool("pool-a", "node-a"))
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	registry := &Registry{Client: client, Now: func() time.Time { return now }}
	ctx := context.Background()

	seedReadyVolume(t, client, "shiftpv-11111111111111111111111111111111", "node-a")
	state, err := registry.Get(ctx, "shiftpv-11111111111111111111111111111111")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if state.Phase != PhaseReady || state.OwnerNode != "node-a" {
		t.Fatalf("state = %#v", state)
	}
	nodes, err := registry.PoolNodes(ctx)
	if err != nil {
		t.Fatalf("PoolNodes: %v", err)
	}
	if !reflect.DeepEqual(nodes, []string{"node-a", "node-b"}) {
		t.Fatalf("pool nodes = %#v", nodes)
	}
	registered, err := registry.PoolForNode(ctx, "node-b")
	if err != nil || registered.Name != "pool-b" || registered.MountPath != "/mnt/shiftpv" || registered.CapacityLimit != "10Gi" {
		t.Fatalf("PoolForNode = %#v, %v", registered, err)
	}
}

func TestPoolGroupDefaultsAndRejectsInvalidValues(t *testing.T) {
	legacy := pool("pool-legacy", "node-a")
	decoded, err := poolFrom(legacy)
	if err != nil || decoded.PoolGroup != DefaultPoolGroup {
		t.Fatalf("legacy Pool group = %q, err = %v", decoded.PoolGroup, err)
	}
	grouped := pool("pool-grouped", "node-b")
	if err := unstructured.SetNestedField(grouped.Object, "fast", "spec", "poolGroup"); err != nil {
		t.Fatal(err)
	}
	decoded, err = poolFrom(grouped)
	if err != nil || decoded.PoolGroup != "fast" {
		t.Fatalf("grouped Pool group = %q, err = %v", decoded.PoolGroup, err)
	}
	if err := unstructured.SetNestedField(grouped.Object, "Invalid.Group", "spec", "poolGroup"); err != nil {
		t.Fatal(err)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, grouped)
	if ready, err := (&Registry{Client: client}).ReadyPools(context.Background()); err != nil || len(ready) != 0 {
		t.Fatalf("invalid Pool group accepted: %v", err)
	}
}

func TestPoolNodesForGroupRestrictsAccessibleTopology(t *testing.T) {
	defaultPool := pool("pool-default", "node-a")
	fastPool := pool("pool-fast", "node-b")
	if err := unstructured.SetNestedField(fastPool.Object, "fast", "spec", "poolGroup"); err != nil {
		t.Fatal(err)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		PoolResource: "ShiftPVPoolList",
	}, defaultPool, fastPool)
	registry := &Registry{Client: client}
	for group, want := range map[string]string{DefaultPoolGroup: "node-a", "fast": "node-b"} {
		nodes, err := registry.PoolNodesForGroup(context.Background(), group)
		if err != nil || !reflect.DeepEqual(nodes, []string{want}) {
			t.Fatalf("group %q nodes=%v want=%q err=%v", group, nodes, want, err)
		}
	}
}

func TestPoolTopologySurvivesLimitErrorsAndRecovery(t *testing.T) {
	for _, limit := range []string{"not-a-quantity", "0", "1500m", "9223372036854775808"} {
		t.Run(limit, func(t *testing.T) {
			ctx := context.Background()
			a, b := pool("a", "node-a"), pool("b", "node-b")
			a.SetUID("a-uid")
			b.SetUID("b-uid")
			if err := unstructured.SetNestedField(b.Object, true, "status", "registrationApproved"); err != nil {
				t.Fatal(err)
			}
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, a, b)
			registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
			for i, value := range []string{limit, "10Gi"} {
				object, err := client.Resource(PoolResource).Get(ctx, "b", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				object.SetGeneration(int64(i + 2))
				if err := unstructured.SetNestedField(object.Object, value, "spec", "capacity", "limit"); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Resource(PoolResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				nodes, err := registry.PoolNodesForGroup(ctx, DefaultPoolGroup)
				if err != nil || !reflect.DeepEqual(nodes, []string{"node-a", "node-b"}) {
					t.Fatalf("limit %q shrank topology: nodes=%v err=%v", value, nodes, err)
				}
				ready, err := registry.ReadyPools(ctx)
				if err != nil || len(ready) != 1 || ready[0].UID != "a-uid" {
					t.Fatalf("invalid limit or old observation admitted placement: ready=%v err=%v", ready, err)
				}
			}
			object, err := client.Resource(PoolResource).Get(ctx, "b", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := poolFrom(object)
			if err != nil {
				t.Fatal(err)
			}
			recovered.Status.ObservedGeneration = recovered.Generation
			for i := range recovered.Status.Conditions {
				recovered.Status.Conditions[i].ObservedGeneration = recovered.Generation
			}
			if err := registry.SetPoolStatus(ctx, "b", "b-uid", "node-b", recovered.Status); err != nil {
				t.Fatal(err)
			}
			ready, err := registry.ReadyPools(ctx)
			if err != nil || len(ready) != 2 {
				t.Fatalf("fresh recovered Pool stayed unavailable: ready=%v err=%v", ready, err)
			}
		})
	}
}

func TestPoolTopologyRejectsInvalidRegistrationShape(t *testing.T) {
	for _, tc := range []struct {
		field, value string
	}{
		{"nodeName", ""},
		{"poolGroup", "Invalid.Group"},
		{"mountPath", "/"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			a, b := pool("a", "node-a"), pool("b", "node-b")
			if err := unstructured.SetNestedField(b.Object, true, "status", "registrationApproved"); err != nil {
				t.Fatal(err)
			}
			if err := unstructured.SetNestedField(b.Object, tc.value, "spec", tc.field); err != nil {
				t.Fatal(err)
			}
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, a, b)
			registry := &Registry{Client: client}
			nodes, err := registry.PoolNodes(context.Background())
			if err != nil || !reflect.DeepEqual(nodes, []string{"node-a"}) {
				t.Fatalf("invalid registration added a node: nodes=%v err=%v", nodes, err)
			}
		})
	}
}

func TestRegistryReturnsObjectIncarnationIdentity(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	volumeID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	registeredVolume := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": "volume-uid"},
		"spec":     map[string]any{"volumeID": volumeID},
		"status":   map[string]any{"phase": PhaseReady, "ownerNode": "node-a", "publishedNodes": []any{}},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool, registeredVolume)
	registry := &Registry{Client: client}
	installationID, err := registry.InstallationID(ctx)
	if err != nil || installationID != "installation-uid" {
		t.Fatalf("installationID=%q err=%v", installationID, err)
	}
	registered, err := registry.PoolForNode(ctx, "node-a")
	if err != nil || registered.UID != "pool-uid" {
		t.Fatalf("pool=%#v err=%v", registered, err)
	}
	state, err := registry.Get(ctx, volumeID)
	if err != nil || state.UID != "volume-uid" {
		t.Fatalf("volume=%#v err=%v", state, err)
	}
}

func TestBeginCreatePersistsIdentityBeforeReady(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool)
	client.PrependReactor("create", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		object.SetUID("volume-uid")
		return false, nil, nil
	})
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	volumeID := "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	state, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != PhasePending || state.UID != "volume-uid" || state.CreationOperationID != "create-volume-uid" || state.CurrentCopy == nil {
		t.Fatalf("pending state=%#v", state)
	}
	if state.CurrentCopy.InstallationID != "installation-uid" || state.CurrentCopy.PoolUID != "pool-uid" || state.CurrentCopy.VolumeUID != "volume-uid" || state.CurrentCopy.Role != volume.RoleServing {
		t.Fatalf("copy identity=%#v", state.CurrentCopy)
	}
	second, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64)
	if err != nil || !reflect.DeepEqual(second, state) {
		t.Fatalf("idempotent begin=%#v err=%v", second, err)
	}
	if err := registry.CompleteCreate(ctx, volumeID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	ready, err := registry.Get(ctx, volumeID)
	if err != nil || ready.Phase != PhaseReady || ready.CreationOperationID != state.CreationOperationID {
		t.Fatalf("ready=%#v err=%v", ready, err)
	}
	if err := registry.CompleteCreate(ctx, volumeID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
}

func TestBeginCreateInPoolAnchorsSelectedPoolAcrossRetries(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	identity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	poolA, poolB := pool("pool-a", "node-a"), pool("pool-b", "node-a")
	poolA.SetUID("pool-a-uid")
	poolB.SetUID("pool-b-uid")
	enableFixedCapacity(t, poolA, "8:1", 0)
	enableFixedCapacity(t, poolB, "8:2", 1024)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, identity, poolA, poolB)
	client.PrependReactor("create", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).SetUID("volume-uid")
		return false, nil, nil
	})
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	id := "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	state, err := registry.BeginCreateInPool(ctx, id, "pvc", "node-a", 64, "pool-b", "pool-b-uid")
	if err != nil || state.CurrentCopy == nil || state.CurrentCopy.PoolUID != "pool-b-uid" {
		t.Fatalf("selected Pool was not anchored: state=%#v err=%v", state, err)
	}
	if _, err := registry.BeginCreateInPool(ctx, id, "pvc", "node-a", 64, "pool-a", "pool-a-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("retry changed Pool identity: %v", err)
	}
	if _, err := registry.BeginCreateInPool(ctx, id, "pvc", "node-a", 64, "pool-b", "pool-b-uid"); err != nil {
		t.Fatalf("retry in anchored Pool failed: %v", err)
	}
}

func TestPoolServingCopyConflict(t *testing.T) {
	serving := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", VolumeUID: "old-volume-uid",
		CopyID: "old-copy", NodeName: "node-a", Role: volume.RoleServing,
	}
	pool := Pool{Status: PoolStatus{Inventory: &PoolInventory{Valid: true, Copies: []CopyObservation{
		{Identity: &serving, Present: true},
	}}}}
	if !PoolHasServingVolume(pool, serving.VolumeID) {
		t.Fatal("existing serving copy was not detected")
	}
	if PoolHasConflictingServingVolume(pool, serving.VolumeID, &serving) {
		t.Fatal("the exact copy owned by the current transaction was rejected")
	}

	foreign := serving
	foreign.CopyID = "foreign-copy"
	pool.Status.Inventory.Copies = append(pool.Status.Inventory.Copies, CopyObservation{Identity: &foreign, Present: true})
	if !PoolHasConflictingServingVolume(pool, serving.VolumeID, &serving) {
		t.Fatal("a foreign serving copy was hidden by the allowed identity")
	}

	retired := serving
	retired.Role = volume.RoleRetired
	pool.Status.Inventory.Copies = []CopyObservation{
		{Identity: &serving, Present: false},
		{Identity: &retired, Present: true},
	}
	if PoolHasServingVolume(pool, serving.VolumeID) {
		t.Fatal("absent or retired copies were treated as serving conflicts")
	}
}

func TestPoolHasPublishedCopyRequiresExactScannerProof(t *testing.T) {
	copy := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", VolumeUID: "volume-uid",
		CopyID: "copy-a", NodeName: "node-a", Role: volume.RoleServing,
	}
	pool := Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, Status: PoolStatus{Inventory: &PoolInventory{
		Valid: true, Copies: []CopyObservation{{Identity: &copy, Present: true, Published: true}},
	}}}
	if !PoolHasPublishedCopy(pool, &copy) {
		t.Fatal("exact published serving copy was not accepted")
	}
	for name, mutate := range map[string]func(*Pool){
		"wrong pool name": func(pool *Pool) { pool.Name = "other-pool" },
		"wrong pool uid":  func(pool *Pool) { pool.UID = "other-uid" },
		"wrong node":      func(pool *Pool) { pool.NodeName = "other-node" },
		"not present":     func(pool *Pool) { pool.Status.Inventory.Copies[0].Present = false },
		"not published":   func(pool *Pool) { pool.Status.Inventory.Copies[0].Published = false },
		"problem":         func(pool *Pool) { pool.Status.Inventory.Copies[0].Problem = "MountMissing" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := pool
			changed.Status.Inventory = &PoolInventory{Valid: true, Copies: append([]CopyObservation(nil), pool.Status.Inventory.Copies...)}
			mutate(&changed)
			if PoolHasPublishedCopy(changed, &copy) {
				t.Fatalf("accepted %s as publication proof", name)
			}
		})
	}
}

func TestBeginCreateRejectsExistingServingCopyBeforeIntent(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	const volumeID = "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	serving := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "old-volume-uid", CopyID: "old-copy",
		NodeName: "node-a", Role: volume.RoleServing,
	}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&PoolStatus{
		ObservedGeneration: 1,
		LastProbeTime:      metav1.NewTime(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)),
		Conditions: []metav1.Condition{{
			Type: PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1,
			LastTransitionTime: metav1.NewTime(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)), Reason: "PoolReady",
		}},
		Inventory: &PoolInventory{
			ObservedAt: metav1.NewTime(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)),
			Valid:      true, Copies: []CopyObservation{{Identity: &serving, Present: true}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registeredPool.Object["status"] = status
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool)
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 1, 0, time.UTC) }}

	if _, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64); !errors.Is(err, ErrPoolCopyConflict) {
		t.Fatalf("existing serving copy error = %v", err)
	}
	if _, err := client.Resource(VolumeResource).Get(ctx, volumeID, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("conflicting copy created a ShiftPVVolume: %v", err)
	}

	placeholder := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": "new-volume-uid"},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	if _, err := client.Resource(VolumeResource).Create(ctx, placeholder, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64); !errors.Is(err, ErrPoolCopyConflict) {
		t.Fatalf("uninitialized intent bypassed serving copy conflict: %v", err)
	}
	current, err := client.Resource(VolumeResource).Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil || current.GetUID() != "new-volume-uid" {
		t.Fatalf("uninitialized intent was replaced: object=%#v err=%v", current, err)
	}
	state, err := stateFrom(current)
	if err != nil || state.Phase != "" || state.CurrentCopy != nil {
		t.Fatalf("conflicting intent received creation authority: state=%#v err=%v", state, err)
	}
}

func TestBeginCreateRejectsReplacementVolumeBeforeStatusWrite(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	const volumeID = "shiftpv-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	original := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": "original-uid"},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	replacement := original.DeepCopy()
	replacement.SetUID("replacement-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool, replacement)
	getCalls := 0
	client.PrependReactor("get", "shiftpvvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls == 1 {
			return true, original.DeepCopy(), nil
		}
		return false, nil, nil
	})
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	if _, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Volume creation error = %v", err)
	}
	current, err := registry.Get(ctx, volumeID)
	if err != nil || current.UID != "replacement-uid" || current.Phase != "" || current.CurrentCopy != nil {
		t.Fatalf("replacement Volume received stale creation state: state=%#v err=%v", current, err)
	}
}

func TestBeginCreateResumesExactPendingIdentityWithInvalidInventory(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool)
	client.PrependReactor("create", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		object.SetUID("volume-uid")
		return false, nil, nil
	})
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	volumeID := "shiftpv-cccccccccccccccccccccccccccccccc"
	pending, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64)
	if err != nil {
		t.Fatal(err)
	}

	currentPool, err := client.Resource(PoolResource).Get(ctx, registeredPool.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(currentPool.Object, false, "status", "inventory", "valid"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(PoolResource).Update(ctx, currentPool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ReadyPoolForNode(ctx, "node-a"); !errors.Is(err, ErrPoolNotReady) {
		t.Fatalf("invalid inventory remained eligible: %v", err)
	}

	resumed, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64)
	if err != nil || !reflect.DeepEqual(resumed, pending) {
		t.Fatalf("exact pending creation did not resume: state=%#v err=%v", resumed, err)
	}
	if _, err := registry.BeginCreate(ctx, "shiftpv-dddddddddddddddddddddddddddddddd", "pvc-new", "node-a", 64); !errors.Is(err, ErrPoolNotReady) {
		t.Fatalf("new creation bypassed invalid inventory: %v", err)
	}
}

func TestBeginCreateDoesNotResumeAfterPoolProtectionIsLost(t *testing.T) {
	ctx := context.Background()
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": installationNamespace, "uid": "installation-uid"},
	}}
	registeredPool := pool("pool-a", "node-a")
	registeredPool.SetUID("pool-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, clusterIdentity, registeredPool)
	client.PrependReactor("create", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		object.SetUID("volume-uid")
		return false, nil, nil
	})
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	volumeID := "shiftpv-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	if _, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64); err != nil {
		t.Fatal(err)
	}
	currentPool, err := client.Resource(PoolResource).Get(ctx, registeredPool.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	currentPool.SetFinalizers(nil)
	if _, err := client.Resource(PoolResource).Update(ctx, currentPool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.BeginCreate(ctx, volumeID, "pvc", "node-a", 64); !errors.Is(err, ErrPoolNotReady) {
		t.Fatalf("pending creation resumed without Pool protection: %v", err)
	}
}

func TestBeginDeleteFencesPublicationAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-cccccccccccccccccccccccccccccccc"
	copy := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	setState(object, State{UID: copy.VolumeUID, Phase: PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy})
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &Registry{Client: client}

	fenced, err := registry.BeginDelete(ctx, volumeID, copy.VolumeUID, copy)
	if err != nil || fenced.Phase != PhaseDeleting || fenced.DeletionOperationID != "delete-"+copy.VolumeUID {
		t.Fatalf("fenced=%#v err=%v", fenced, err)
	}
	if _, err := registry.BeginDelete(ctx, volumeID, copy.VolumeUID, copy); err != nil {
		t.Fatalf("idempotent deletion fence: %v", err)
	}
	if err := registry.BeginPublish(ctx, volumeID, copy.NodeName, copy); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("publication crossed deletion fence: %v", err)
	}
}

func TestBeginPublishRecordsIntentIdempotently(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	copy := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	setState(object, State{UID: copy.VolumeUID, Phase: PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy})
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &Registry{Client: client}

	for range 2 {
		if err := registry.BeginPublish(ctx, volumeID, copy.NodeName, copy); err != nil {
			t.Fatal(err)
		}
	}
	state, err := registry.Get(ctx, volumeID)
	if err != nil || !reflect.DeepEqual(state.PublishedNodes, []string{copy.NodeName}) {
		t.Fatalf("publish intent=%#v err=%v", state.PublishedNodes, err)
	}
}

func TestBeginDeleteRejectsPublishedOrChangedIdentity(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-dddddddddddddddddddddddddddddddd"
	copy := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	setState(object, State{UID: copy.VolumeUID, Phase: PhaseReady, OwnerNode: copy.NodeName, PublishedNodes: []string{copy.NodeName}, CurrentCopy: &copy})
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &Registry{Client: client}
	if _, err := registry.BeginDelete(ctx, volumeID, copy.VolumeUID, copy); !errors.Is(err, ErrStateConflict) || !errors.Is(err, ErrVolumePublished) {
		t.Fatalf("published copy was fenced for deletion: %v", err)
	}
	if err := registry.ReconcilePublished(ctx, volumeID, copy.NodeName, copy, false); err != nil {
		t.Fatal(err)
	}
	replacement := copy
	replacement.CopyID = "replacement"
	if _, err := registry.BeginDelete(ctx, volumeID, copy.VolumeUID, replacement); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("changed copy identity was fenced for deletion: %v", err)
	}
}

func TestReconcilePublishedRequiresExactLiveCopy(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	copy := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	setState(object, State{UID: copy.VolumeUID, Phase: PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy})
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &Registry{Client: client}
	if err := registry.ReconcilePublished(ctx, volumeID, copy.NodeName, copy, true); err != nil {
		t.Fatal(err)
	}
	state, err := registry.Get(ctx, volumeID)
	if err != nil || !reflect.DeepEqual(state.PublishedNodes, []string{copy.NodeName}) {
		t.Fatalf("published state=%#v err=%v", state, err)
	}
	replacement := copy
	replacement.CopyID = "replacement-copy"
	if err := registry.ReconcilePublished(ctx, volumeID, copy.NodeName, replacement, false); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement copy cleared publication: %v", err)
	}
	state, err = registry.Get(ctx, volumeID)
	if err != nil || !reflect.DeepEqual(state.PublishedNodes, []string{copy.NodeName}) {
		t.Fatalf("publication changed after rejected identity: state=%#v err=%v", state, err)
	}
	if err := registry.ReconcilePublished(ctx, volumeID, copy.NodeName, copy, false); err != nil {
		t.Fatal(err)
	}
}

func TestCreationOperationIDRejectsUnboundedUID(t *testing.T) {
	if got, err := CreationOperationID("volume-uid"); err != nil || got != "create-volume-uid" {
		t.Fatalf("operationID=%q err=%v", got, err)
	}
	if _, err := CreationOperationID(strings.Repeat("x", 128)); err == nil {
		t.Fatal("unbounded creation operation identity was accepted")
	}
}

func TestRegistryReadyPoolsRejectsMissingStaleAndOutdatedStatus(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	ready := pool("ready", "node-ready")
	stale := pool("stale", "node-stale")
	_ = unstructured.SetNestedField(stale.Object, now.Add(-4*time.Minute).Format(time.RFC3339), "status", "lastProbeTime")
	outdated := pool("outdated", "node-outdated")
	_ = unstructured.SetNestedField(outdated.Object, int64(0), "status", "observedGeneration")
	conditionOutdated := pool("condition-outdated", "node-condition-outdated")
	conditions, _, _ := unstructured.NestedSlice(conditionOutdated.Object, "status", "conditions")
	conditions[0].(map[string]any)["observedGeneration"] = int64(0)
	_ = unstructured.SetNestedSlice(conditionOutdated.Object, conditions, "status", "conditions")
	pending := pool("pending", "node-pending")
	delete(pending.Object, "status")
	missingInventory := pool("missing-inventory", "node-missing-inventory")
	unstructured.RemoveNestedField(missingInventory.Object, "status", "inventory")
	invalidInventory := pool("invalid-inventory", "node-invalid-inventory")
	_ = unstructured.SetNestedMap(invalidInventory.Object, map[string]any{
		"observedAt": now.Format(time.RFC3339), "valid": false,
	}, "status", "inventory")
	staleInventory := pool("stale-inventory", "node-stale-inventory")
	_ = unstructured.SetNestedMap(staleInventory.Object, map[string]any{
		"observedAt": now.Add(-4 * time.Minute).Format(time.RFC3339), "valid": true,
	}, "status", "inventory")
	truncated := pool("truncated", "node-truncated")
	_ = unstructured.SetNestedMap(truncated.Object, map[string]any{
		"observedAt": now.Format(time.RFC3339), "valid": true, "truncated": true,
	}, "status", "inventory")
	deleting := pool("deleting", "node-deleting")
	deletionTime := metav1.NewTime(now.Add(-time.Second))
	deleting.SetDeletionTimestamp(&deletionTime)
	deleting.SetFinalizers([]string{PoolProtectionFinalizer})
	unprotected := pool("unprotected", "node-unprotected")
	unprotected.SetFinalizers(nil)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		PoolResource: "ShiftPVPoolList",
	}, ready, stale, outdated, conditionOutdated, pending, missingInventory, invalidInventory, staleInventory, truncated, deleting, unprotected)
	registry := &Registry{Client: client, Now: func() time.Time { return now }}

	pools, err := registry.ReadyPools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pools) != 1 || pools[0].Name != "ready" {
		t.Fatalf("ready pools = %#v", pools)
	}
	nodes, err := registry.PoolNodes(context.Background())
	if err != nil || len(nodes) != 11 || !slices.Contains(nodes, "node-deleting") {
		t.Fatalf("registered topology nodes = %#v err=%v", nodes, err)
	}
	if _, err := registry.ReadyPoolForNode(context.Background(), "node-stale"); !errors.Is(err, ErrPoolNotReady) {
		t.Fatalf("stale pool error = %v", err)
	}
	if _, err := registry.ReadyPoolForNode(context.Background(), "node-truncated"); !errors.Is(err, ErrPoolNotReady) || !strings.Contains(err.Error(), "InventoryTruncated") {
		t.Fatalf("truncated pool error = %v", err)
	}
	if _, err := registry.ReadyPoolForNode(context.Background(), "node-deleting"); !errors.Is(err, ErrPoolNotReady) || !strings.Contains(err.Error(), "PoolDeregistering") {
		t.Fatalf("deleting pool error = %v", err)
	}
	if _, err := registry.ReadyPoolForNode(context.Background(), "node-unprotected"); !errors.Is(err, ErrPoolNotReady) || !strings.Contains(err.Error(), "PoolProtectionMissing") {
		t.Fatalf("unprotected pool error = %v", err)
	}
	for node, reason := range map[string]string{
		"node-missing-inventory": "InventoryMissing",
		"node-invalid-inventory": "InventoryInvalid",
		"node-stale-inventory":   "InventoryStale",
	} {
		if _, err := registry.ReadyPoolForNode(context.Background(), node); !errors.Is(err, ErrPoolNotReady) || !strings.Contains(err.Error(), reason) {
			t.Fatalf("%s pool error = %v", reason, err)
		}
	}
}

func TestRegistryMaintainsExactPoolProtectionFinalizer(t *testing.T) {
	object := pool("pool-a", "node-a")
	object.SetUID("pool-a-uid")
	object.SetFinalizers(nil)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		PoolResource: "ShiftPVPoolList",
	}, object)
	registry := &Registry{Client: client}
	ctx := context.Background()

	if err := registry.EnsurePoolFinalizer(ctx, "pool-a", "pool-a-uid"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnsurePoolFinalizer(ctx, "pool-a", "pool-a-uid"); err != nil {
		t.Fatalf("idempotent ensure: %v", err)
	}
	current, err := client.Resource(PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(current.GetFinalizers(), []string{PoolProtectionFinalizer}) {
		t.Fatalf("finalizers=%v err=%v", current.GetFinalizers(), err)
	}
	if err := registry.ApprovePoolIdentityRelease(ctx, "pool-a", "pool-a-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("active Pool identity release was approved: %v", err)
	}
	deletionTime := metav1.Now()
	current.SetDeletionTimestamp(&deletionTime)
	if _, err := client.Resource(PoolResource).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.ApprovePoolIdentityRelease(ctx, "pool-a", "replacement-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Pool identity release was approved: %v", err)
	}
	if err := registry.ApprovePoolIdentityRelease(ctx, "pool-a", "pool-a-uid"); err != nil {
		t.Fatal(err)
	}
	current, err = client.Resource(PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil || current.GetAnnotations()[PoolIdentityReleaseAnnotation] != "pool-a-uid" {
		t.Fatalf("release approval=%v err=%v", current.GetAnnotations(), err)
	}
	if err := registry.RemovePoolFinalizer(ctx, "pool-a", "replacement-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Pool removed finalizer: %v", err)
	}
	if err := registry.RemovePoolFinalizer(ctx, "pool-a", "pool-a-uid"); err != nil {
		t.Fatal(err)
	}
	current, err = client.Resource(PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil || len(current.GetFinalizers()) != 0 {
		t.Fatalf("finalizers=%v err=%v", current.GetFinalizers(), err)
	}
}

func TestRegistryRemovesVolumeAndMoveProtectionFinalizers(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	volumeObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": "volume-uid", "finalizers": []any{VolumeProtectionFinalizer, "example.io/keep"}},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	moveObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "finalizers": []any{MoveProtectionFinalizer, "example.io/keep"}},
		"spec":     map[string]any{"volumeID": volumeID, "sourceNode": "node-a"},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", MoveResource: "ShiftPVMoveList",
	}, volumeObject, moveObject)
	registry := &Registry{Client: client}

	if err := registry.RemoveVolumeFinalizer(ctx, volumeID, "volume-uid"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RemoveMoveFinalizer(ctx, "move-a", "move-uid"); err != nil {
		t.Fatal(err)
	}
	for resource, name := range map[schema.GroupVersionResource]string{VolumeResource: volumeID, MoveResource: "move-a"} {
		object, err := client.Resource(resource).Get(ctx, name, metav1.GetOptions{})
		if err != nil || !reflect.DeepEqual(object.GetFinalizers(), []string{"example.io/keep"}) {
			t.Fatalf("%s finalizers=%v err=%v", name, object.GetFinalizers(), err)
		}
	}
	if err := registry.RemoveMoveFinalizer(ctx, "move-a", "move-uid"); err != nil {
		t.Fatalf("idempotent finalizer removal: %v", err)
	}
	if err := client.Resource(MoveResource).Delete(ctx, "move-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RemoveMoveFinalizer(ctx, "move-a", "move-uid"); err != nil {
		t.Fatalf("missing Move finalizer removal: %v", err)
	}
}

// TestRegistryReassertsMoveProtectionOnlyOnALiveMove pins the boundary that
// makes re-asserting protection for an unfinished cleanup journal safe: a Move
// that is already deleting can never gain a finalizer back, and a Move that is
// gone is reported as missing rather than silently accepted.
func TestRegistryReassertsMoveProtectionOnlyOnALiveMove(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	move := func(name string, metadata map[string]any) *unstructured.Unstructured {
		metadata["name"], metadata["uid"] = name, name+"-uid"
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
			"metadata": metadata, "spec": map[string]any{"volumeID": volumeID, "sourceNode": "node-a"},
		}}
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{MoveResource: "ShiftPVMoveList"},
		move("live", map[string]any{"finalizers": []any{"example.io/keep"}}),
		move("deleting", map[string]any{"deletionTimestamp": "2026-09-13T01:00:00Z"}))
	registry := &Registry{Client: client}

	if err := registry.AddMoveFinalizer(ctx, "live", "live-uid"); err != nil {
		t.Fatal(err)
	}
	object, err := client.Resource(MoveResource).Get(ctx, "live", metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(object.GetFinalizers(), []string{"example.io/keep", MoveProtectionFinalizer}) {
		t.Fatalf("live finalizers=%v err=%v", object.GetFinalizers(), err)
	}
	if err := registry.AddMoveFinalizer(ctx, "live", "live-uid"); err != nil {
		t.Fatalf("idempotent protection re-assertion: %v", err)
	}

	err = registry.AddMoveFinalizer(ctx, "deleting", "deleting-uid")
	if !errors.Is(err, ErrStateConflict) || !strings.Contains(err.Error(), "already deleting") {
		t.Fatalf("protected an already deleting Move: %v", err)
	}
	if err := registry.AddMoveFinalizer(ctx, "absent", "absent-uid"); !apierrors.IsNotFound(errors.Unwrap(err)) {
		t.Fatalf("protecting a missing Move was not reported: %v", err)
	}
	if err := registry.AddMoveFinalizer(ctx, "live", "replaced-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("protected a reincarnated Move: %v", err)
	}
}

func TestMoveCapacityHoldPredicates(t *testing.T) {
	state := State{OwnerNode: "source", ActiveMove: "move-a"}
	move := Move{
		Name:   "move-a",
		Status: MoveStatus{DestinationNode: "destination", CapacityApproved: true},
	}
	if !MoveReservesDestination(move, state, "destination") || MoveReservesDestination(move, state, "source") {
		t.Fatalf("unexpected destination hold: move=%#v state=%#v", move, state)
	}
	move.Status = MoveStatus{Phase: "Succeeded", CleanupPhase: "Completed"}
	if !MoveCleanupSettled(move) {
		t.Fatal("Succeeded Move with completed cleanup was not settled")
	}
	move.Status = MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityReason: "RecoverySettled"}
	if !MoveCleanupSettled(move) {
		t.Fatal("recovered Move with released capacity was not settled")
	}
	move.Status.CapacityApproved = true
	if MoveCleanupSettled(move) {
		t.Fatal("recovered Move with a capacity hold was settled")
	}
}

func TestTerminatingPoolSeparatesPlacementFromExactCleanup(t *testing.T) {
	now := time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC)
	deletionTime := metav1.NewTime(now.Add(-time.Second))
	pool := Pool{
		Name: "pool", UID: "pool-uid", Generation: 2, DeletionTimestamp: &deletionTime,
		NodeName: "node", MountPath: "/mnt/pool", PoolGroup: DefaultPoolGroup, CapacityLimit: "1Gi",
		Status: PoolStatus{
			ObservedGeneration: 2, LastProbeTime: metav1.NewTime(now),
			Conditions: []metav1.Condition{
				{Type: PoolConditionReady, Status: metav1.ConditionFalse, Reason: "PoolDeregistering", ObservedGeneration: 2},
				{Type: PoolConditionAccessible, Status: metav1.ConditionTrue, Reason: "DirectoryAccessible", ObservedGeneration: 2},
				{Type: PoolConditionWritable, Status: metav1.ConditionTrue, Reason: "DirectoryWritable", ObservedGeneration: 2},
				{Type: PoolConditionCapacityReadable, Status: metav1.ConditionTrue, Reason: "CapacityReadable", ObservedGeneration: 2},
			},
		},
	}
	if ready, reason := pool.ReadyAt(now, time.Minute); ready || reason != "PoolDeregistering" {
		t.Fatalf("placement readiness = %v, %s", ready, reason)
	}
	if ready, reason := pool.CleanupReadyAt(now, time.Minute); !ready || reason != "PoolCleanupReady" {
		t.Fatalf("cleanup readiness = %v, %s", ready, reason)
	}
	pool.Status.Conditions[1].Status = metav1.ConditionFalse
	pool.Status.Conditions[1].Reason = "PathMissing"
	if ready, reason := pool.CleanupReadyAt(now, time.Minute); ready || reason != "PathMissing" {
		t.Fatalf("failed cleanup readiness = %v, %s", ready, reason)
	}
}

func TestInvalidLimitCleanupRequiresFreshPhysicalProof(t *testing.T) {
	now := time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		change func(*Pool)
		ready  bool
	}{
		{"current backing", func(p *Pool) {}, true},
		{"outdated generation", func(p *Pool) { p.Status.ObservedGeneration-- }, false},
		{"outdated condition", func(p *Pool) { p.Status.Conditions[1].ObservedGeneration-- }, false},
		{"stale probe", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(-2 * time.Minute)) }, false},
		{"future probe", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(time.Minute)) }, false},
		{"failed directory", func(p *Pool) { p.Status.Conditions[1].Status = metav1.ConditionFalse }, false},
		{"unknown directory", func(p *Pool) { p.Status.Conditions[1].Status = metav1.ConditionUnknown }, false},
		{"invalid path", func(p *Pool) { p.MountPath = "/" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := Pool{
				Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool", PoolGroup: DefaultPoolGroup,
				Generation: 2, CapacityLimit: "not-a-quantity",
				Status: PoolStatus{
					RegistrationApproved: true, ObservedGeneration: 2, LastProbeTime: metav1.NewTime(now),
					Conditions: []metav1.Condition{
						{Type: PoolConditionReady, Status: metav1.ConditionFalse, Reason: "PoolCapacityLimitInvalid", ObservedGeneration: 2},
						{Type: PoolConditionAccessible, Status: metav1.ConditionTrue, Reason: "DirectoryAccessible", ObservedGeneration: 2},
						{Type: PoolConditionWritable, Status: metav1.ConditionTrue, Reason: "DirectoryWritable", ObservedGeneration: 2},
						{Type: PoolConditionCapacityReadable, Status: metav1.ConditionTrue, Reason: "CapacityReadable", ObservedGeneration: 2},
					},
				},
			}
			tc.change(&pool)
			if ready, reason := pool.CleanupReadyAt(now, time.Minute); ready != tc.ready {
				t.Fatalf("cleanup ready=%v reason=%s want=%v", ready, reason, tc.ready)
			}
			if ready, _ := poolReady(pool, now, time.Minute); ready {
				t.Fatal("invalid placement settings admitted new work")
			}
		})
	}
}

func TestPoolReadyForActiveMoveRepairAtAdmitsOnlyExactCrashWindow(t *testing.T) {
	now := time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC)
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "move-move-uid-incoming", NodeName: "destination", Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.CopyID, destination.Role = "move-move-uid-serving", volume.RoleServing
	state := State{UID: source.VolumeUID, Phase: PhaseMoving, OwnerNode: source.NodeName, ActiveMove: "move-test", CurrentCopy: &source}
	move := Move{
		Name: "move-test", UID: "move-uid", Spec: MoveSpec{VolumeID: volumeID, SourceNode: source.NodeName},
		Status: MoveStatus{
			Phase: "Copying", DestinationNode: incoming.NodeName, DestinationPoolUID: incoming.PoolUID,
			SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			CopyOperationID: "copy-move-uid", PromotionOperationID: "promote-move-uid",
		},
	}
	readyStatus := PoolStatus{
		ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		Conditions: []metav1.Condition{{Type: PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
	}
	pool := Pool{Name: incoming.PoolName, UID: incoming.PoolUID, NodeName: incoming.NodeName, MountPath: "/pool", Generation: 1, Status: readyStatus}
	pool.Status.Inventory = &PoolInventory{
		ObservedAt: metav1.NewTime(now), Message: "CopyObservationProblem",
		Copies: []CopyObservation{{Marker: "path:.shiftpv/incoming/" + incoming.CopyID, Present: true, Problem: "UnrecordedPath"}},
	}
	if !PoolReadyForActiveMoveRepairAt(pool, move, state, now, DefaultPoolReadinessStaleAfter) {
		t.Fatal("exact incoming crash window was not admitted for repair")
	}

	promoting := move
	promoting.Status.Phase = "Promoting"
	promotingPool := pool
	promotingPool.Status.Inventory = &PoolInventory{
		ObservedAt: metav1.NewTime(now), Message: "CopyObservationProblem",
		Copies: []CopyObservation{
			{Marker: "placement-" + incoming.CopyID, Identity: &incoming, Present: false},
			{Marker: "path:volumes/" + volumeID, Present: true, Problem: "UnrecordedPath"},
		},
	}
	if !PoolReadyForActiveMoveRepairAt(promotingPool, promoting, state, now, DefaultPoolReadinessStaleAfter) {
		t.Fatal("exact promotion crash window was not admitted for repair")
	}

	tests := map[string]func(*Pool, *Move, *State){
		"invalid backing path": func(pool *Pool, _ *Move, _ *State) { pool.MountPath = "/." },
		"invalid mount policy": func(pool *Pool, _ *Move, _ *State) { pool.MountPolicy = "unsupported" },
		"unrelated path": func(pool *Pool, _ *Move, _ *State) {
			pool.Status.Inventory.Copies[0].Marker = "path:.shiftpv/incoming/foreign"
		},
		"extra problem": func(pool *Pool, _ *Move, _ *State) {
			pool.Status.Inventory.Copies = append(pool.Status.Inventory.Copies, CopyObservation{Marker: "path:volumes/foreign", Present: true, Problem: "UnrecordedPath"})
		},
		"unexpected type": func(pool *Pool, _ *Move, _ *State) {
			pool.Status.Inventory.Copies[0].Problem = "UnexpectedPathType"
		},
		"stale inventory": func(pool *Pool, _ *Move, _ *State) {
			pool.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(-4 * time.Minute))
		},
		"truncated inventory": func(pool *Pool, _ *Move, _ *State) { pool.Status.Inventory.Truncated = true },
		"pool replacement":    func(pool *Pool, _ *Move, _ *State) { pool.UID = "replacement-pool-uid" },
		"inactive move":       func(_ *Pool, _ *Move, state *State) { state.ActiveMove = "other-move" },
		"changed operation":   func(_ *Pool, move *Move, _ *State) { move.Status.CopyOperationID = "copy-other" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidatePool, candidateMove, candidateState := pool, move, state
			inventory := *pool.Status.Inventory
			inventory.Copies = append([]CopyObservation(nil), pool.Status.Inventory.Copies...)
			candidatePool.Status.Inventory = &inventory
			mutate(&candidatePool, &candidateMove, &candidateState)
			if PoolReadyForActiveMoveRepairAt(candidatePool, candidateMove, candidateState, now, DefaultPoolReadinessStaleAfter) {
				t.Fatal("unsafe inventory was admitted for repair")
			}
		})
	}
}

func TestRegistrySetPoolStatusUsesPoolIdentity(t *testing.T) {
	object := pool("pool-a", "node-a")
	object.SetUID("pool-a-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		PoolResource: "ShiftPVPoolList",
	}, object)
	registry := &Registry{Client: client}
	status := PoolStatus{ObservedGeneration: 1, LastProbeTime: metav1.NewTime(time.Now()), Conditions: []metav1.Condition{{
		Type: PoolConditionReady, Status: metav1.ConditionFalse, Reason: "ReadOnly", Message: "read-only",
	}}}
	if err := registry.SetPoolStatus(context.Background(), "pool-a", "pool-a-uid", "node-b", status); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("foreign node update error = %v", err)
	}
	if err := registry.SetPoolStatus(context.Background(), "pool-a", "replacement-uid", "node-a", status); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Pool update error = %v", err)
	}
	if err := registry.SetPoolStatus(context.Background(), "pool-a", "pool-a-uid", "node-a", status); err != nil {
		t.Fatal(err)
	}
	updated, err := registry.PoolForNode(context.Background(), "node-a")
	if err != nil || updated.Status.Conditions[0].Reason != "ReadOnly" {
		t.Fatalf("updated pool = %#v err=%v", updated, err)
	}
}

func TestRegistryPoolForNodeRejectsMissingAndDuplicateRegistration(t *testing.T) {
	active := pool("pool-a", "node-a")
	retiring := pool("pool-a-duplicate", "node-a")
	active.SetUID("pool-a-uid")
	retiring.SetUID("pool-a-duplicate-uid")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", MoveResource: "ShiftPVMoveList",
	}, active, retiring)
	registry := &Registry{Client: client}
	registrations, err := registry.ListPoolRegistrations(context.Background())
	if err != nil || len(registrations) != 2 {
		t.Fatalf("raw lifecycle registrations=%v err=%v", registrations, err)
	}
	if _, err := registry.PoolForNode(context.Background(), "node-b"); err == nil {
		t.Fatal("missing node registration was accepted")
	}
	if _, err := registry.PoolForNode(context.Background(), "node-a"); err == nil {
		t.Fatal("duplicate node registration was accepted")
	}
	deletedAt := metav1.Now()
	retiring.SetDeletionTimestamp(&deletedAt)
	if _, err := client.Resource(PoolResource).Update(context.Background(), retiring, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	lifecyclePool, err := registry.PoolForNodeLifecycle(context.Background(), "node-a")
	if err != nil || lifecyclePool.Name != retiring.GetName() {
		t.Fatalf("deleting duplicate lifecycle Pool=%#v err=%v", lifecyclePool, err)
	}
	exactPool, err := registry.PoolForIdentity(context.Background(), retiring.GetName(), string(retiring.GetUID()), "node-a")
	if err != nil || exactPool.Name != retiring.GetName() {
		t.Fatalf("exact duplicate lifecycle Pool=%#v err=%v", exactPool, err)
	}
	if _, err := registry.PoolForIdentity(context.Background(), retiring.GetName(), "replacement-uid", "node-a"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Pool identity was accepted: %v", err)
	}
}

func TestRegistryRejectsMissingConfigurationAndEmptyPools(t *testing.T) {
	if _, err := (&Registry{}).Get(context.Background(), "volume"); err == nil {
		t.Fatal("expected missing client error")
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList",
		PoolResource:   "ShiftPVPoolList",
		MoveResource:   "ShiftPVMoveList",
	})
	registry := &Registry{Client: client}
	ctx := context.Background()
	if _, err := registry.PoolNodes(ctx); err == nil {
		t.Fatal("expected empty pool error")
	}
}

func TestRegistryCompareAndSetAndMoveStatus(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-33333333333333333333333333333333"
	moveObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-test", "uid": "move-uid"},
		"spec":     map[string]any{"volumeID": volumeID, "sourceNode": "node-a"},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", MoveResource: "ShiftPVMoveList",
	}, pool("pool-a", "node-a"), moveObject)
	registry := &Registry{Client: client}
	seedReadyVolume(t, client, volumeID, "node-a")
	setVolumeUID(t, client, volumeID, "volume-uid")
	current, err := registry.Get(ctx, volumeID)
	if err != nil {
		t.Fatal(err)
	}
	next := State{UID: current.UID, Phase: PhaseMoving, OwnerNode: "node-a", ActiveMove: "move-test"}
	if err := registry.CompareAndSetState(ctx, volumeID, PhaseReady, "", "node-a", next); err != nil {
		t.Fatal(err)
	}
	if err := registry.CompareAndSetState(ctx, volumeID, PhaseReady, "", "node-a", next); err == nil {
		t.Fatal("stale state precondition was accepted")
	}
	status := MoveStatus{Phase: "Copying", DestinationNode: "node-b", DestinationPoolUID: "pool-b-uid", ReplacementUID: "replacement-uid", CandidateNodes: []string{"node-b"}, EvictionRequested: true, CopyJobName: "copy"}
	if err := registry.SetMoveStatus(ctx, "move-test", "replacement-uid", status); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("replacement Move status error = %v", err)
	}
	if err := registry.SetMoveStatus(ctx, "move-test", "move-uid", status); err != nil {
		t.Fatal(err)
	}
	move, err := registry.GetMove(ctx, "move-test")
	if err != nil {
		t.Fatal(err)
	}
	if move.Status.Phase != "Copying" || move.Status.DestinationNode != "node-b" || move.Status.DestinationPoolUID != "pool-b-uid" || move.Status.ReplacementUID != "replacement-uid" || !move.Status.EvictionRequested {
		t.Fatalf("move = %#v", move)
	}
	moves, err := registry.ListMoves(ctx)
	if err != nil || len(moves) != 1 {
		t.Fatalf("moves=%#v err=%v", moves, err)
	}
	volumes, err := registry.ListVolumes(ctx)
	if err != nil || volumes[volumeID].ActiveMove != "move-test" {
		t.Fatalf("volumes=%#v err=%v", volumes, err)
	}
	pools, err := registry.Pools(ctx)
	if err != nil || pools[0].MountPath != "/mnt/shiftpv" {
		t.Fatalf("pools=%#v err=%v", pools, err)
	}
}

// TestRegistryDeleteFencesOnExactUID covers the UID precondition that keeps a
// stale delete from removing a recreated ShiftPVVolume.
func TestRegistryDeleteFencesOnExactUID(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-33333333333333333333333333333333"
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList",
	})
	registry := &Registry{Client: client}
	seedReadyVolume(t, client, volumeID, "node-a")
	setVolumeUID(t, client, volumeID, "volume-uid")
	client.PrependReactor("delete", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != "volume-uid" {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, volumeID, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	if err := registry.Delete(ctx, volumeID, "wrong-uid"); err == nil {
		t.Fatal("replacement volume was deleted with the wrong UID")
	}
	if err := registry.Delete(ctx, volumeID, "volume-uid"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Delete(ctx, volumeID, "volume-uid"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryCreateMove(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", MoveResource: "ShiftPVMoveList",
	})
	client.PrependReactor("create", "shiftpvmoves", func(action k8stesting.Action) (bool, runtime.Object, error) {
		created := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		if created.GetGenerateName() != "move-volume-" {
			t.Fatalf("generateName = %q", created.GetGenerateName())
		}
		created.SetName("move-volume-abcde")
		return true, created, nil
	})
	registry := &Registry{Client: client}
	move, err := registry.CreateMove(context.Background(), "move-volume-", MoveSpec{VolumeID: "volume", SourceNode: "source"})
	if err != nil {
		t.Fatal(err)
	}
	if move.Name != "move-volume-abcde" || move.Spec.VolumeID != "volume" || move.Spec.SourceNode != "source" {
		t.Fatalf("move = %#v", move)
	}
	if !slices.Contains(move.Finalizers, MoveProtectionFinalizer) {
		t.Fatalf("move finalizers = %v", move.Finalizers)
	}
}

func TestRegistryDeleteMoveUsesUIDPreconditionAndIsIdempotent(t *testing.T) {
	move := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVMove",
		"metadata":   map[string]any{"name": "move-test", "uid": "move-uid"},
		"spec":       map[string]any{"volumeID": "volume", "sourceNode": "source"},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeResource: "ShiftPVVolumeList", PoolResource: "ShiftPVPoolList", MoveResource: "ShiftPVMoveList",
	}, move)
	client.PrependReactor("delete", "shiftpvmoves", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deletion := action.(k8stesting.DeleteAction)
		options := deletion.GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil {
			t.Fatal("Move deletion omitted the UID precondition")
		}
		if *options.Preconditions.UID == "wrong-uid" {
			return true, nil, apierrors.NewConflict(MoveResource.GroupResource(), deletion.GetName(), errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	registry := &Registry{Client: client}

	if err := registry.DeleteMove(context.Background(), "move-test", "wrong-uid"); !apierrors.IsConflict(err) {
		t.Fatalf("wrong UID deletion error = %v, want conflict", err)
	}
	if _, err := client.Resource(MoveResource).Get(context.Background(), "move-test", metav1.GetOptions{}); err != nil {
		t.Fatalf("wrong UID deleted the Move: %v", err)
	}
	if err := registry.DeleteMove(context.Background(), "move-test", "move-uid"); err != nil {
		t.Fatal(err)
	}
	if err := registry.DeleteMove(context.Background(), "move-test", "move-uid"); err != nil {
		t.Fatalf("idempotent delete failed: %v", err)
	}
	if err := registry.DeleteMove(context.Background(), "", ""); err == nil {
		t.Fatal("empty delete identity was accepted")
	}
}

func pool(name, nodeName string) *unstructured.Unstructured {
	probeTime := "2026-09-07T00:00:00Z"
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVPool",
		"metadata": map[string]any{
			"name": name, "generation": int64(1),
			"finalizers": []any{PoolProtectionFinalizer},
		},
		"spec": map[string]any{
			"nodeName": nodeName, "mountPath": "/mnt/shiftpv",
			"capacity": map[string]any{"limit": "10Gi"},
		},
		"status": map[string]any{
			"observedGeneration": int64(1), "lastProbeTime": probeTime,
			"inventory": map[string]any{
				"observedAt": probeTime, "valid": true,
			},
			"conditions": []any{map[string]any{
				"type": PoolConditionReady, "status": "True", "observedGeneration": int64(1),
				"lastTransitionTime": probeTime, "reason": "PoolReady", "message": "ready",
			}},
		},
	}}
}

func setVolumeUID(t *testing.T, client *dynamicfake.FakeDynamicClient, volumeID, uid string) {
	t.Helper()
	object, err := client.Resource(VolumeResource).Get(context.Background(), volumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetUID(types.UID(uid))
	if _, err := client.Resource(VolumeResource).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func seedReadyVolume(t *testing.T, client *dynamicfake.FakeDynamicClient, volumeID, ownerNode string) {
	t.Helper()
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVVolume",
		"metadata": map[string]any{
			"name": volumeID, "finalizers": []any{VolumeProtectionFinalizer},
		},
		"spec": map[string]any{
			"volumeID": volumeID, "requestName": volumeID,
			"initialNode": ownerNode, "capacityBytes": int64(1),
		},
	}}
	setState(object, State{Phase: PhaseReady, OwnerNode: ownerNode})
	if _, err := client.Resource(VolumeResource).Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed ready ShiftPVVolume: %v", err)
	}
}

func setPublishedFixture(t *testing.T, client *dynamicfake.FakeDynamicClient, volumeID, nodeName string, published bool) {
	t.Helper()
	resource := client.Resource(VolumeResource)
	object, err := resource.Get(context.Background(), volumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ShiftPVVolume fixture: %v", err)
	}
	state, err := stateFrom(object)
	if err != nil {
		t.Fatalf("decode ShiftPVVolume fixture: %v", err)
	}
	state.PublishedNodes = slices.DeleteFunc(state.PublishedNodes, func(node string) bool { return node == nodeName })
	if published {
		state.PublishedNodes = append(state.PublishedNodes, nodeName)
	}
	setState(object, state)
	if _, err := resource.UpdateStatus(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update ShiftPVVolume fixture: %v", err)
	}
}
