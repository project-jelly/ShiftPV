//go:build linux || darwin

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type moveCleanupFixture struct {
	node    *Node
	api     *dynamicfake.FakeDynamicClient
	kube    *kubefake.Clientset
	cleanup cleanupapi.Cleanup
	path    string
}

func newMoveCleanupFixture(t *testing.T, reason, role string) moveCleanupFixture {
	t.Helper()
	_, node, api, kube, state := fixture(t)
	ctx := context.Background()
	local := *state.CurrentCopy
	remote := local
	remote.PoolName, remote.PoolUID, remote.NodeName, remote.CopyID = "pool-b", "pool-b-uid", "worker-b", "remote-copy"
	source, destination := local, remote
	phase, recovery, recoveryOwner := "CleaningSource", "", ""
	volumePhase, owner, published := volumeapi.PhaseReady, remote.NodeName, []string{remote.NodeName}
	if reason == "MoveRollback" {
		source, destination = remote, local
		phase, recovery, recoveryOwner = "Blocked", "Retiring", remote.NodeName
		volumePhase, owner, published = volumeapi.PhaseBlocked, remote.NodeName, nil
	}
	incoming := destination
	incoming.Role = volume.RoleIncoming
	incoming.CopyID, destination.CopyID = volumeapi.MoveCopyIDs("move-uid")
	target := source
	operation := volumeapi.MoveCleanupOperationID("move-uid")
	if reason == "MoveRollback" {
		target, operation = destination, volumeapi.MoveRollbackOperationID("move-uid")
		if role == volume.RoleIncoming {
			target = incoming
		}
	}
	move := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "resourceVersion": "17", "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"protocolVersion": volumeapi.MoveProtocolPreparedV1, "volumeID": testID, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
		"status": map[string]any{
			"persistentVolumeName": "pv-a", "persistentVolumeUID": "pv-uid",
			"persistentVolumeClaimNamespace": "workload", "persistentVolumeClaimName": "claim-a", "persistentVolumeClaimUID": "claim-uid",
			"consumerName": "consumer-a", "consumerUID": "consumer-uid", "placementPodUID": "placement-uid",
			"copyOperationID": volumeapi.MoveCopyOperationID("move-uid"), "promotionOperationID": volumeapi.MovePromotionOperationID("move-uid"),
			"phase": phase, "destinationNode": destination.NodeName, "destinationPoolUID": destination.PoolUID,
			"capacityApproved": true, "recoveryPhase": recovery, "recoveryOwner": recoveryOwner},
	}}
	for key, copy := range map[string]volume.CopyIdentity{"sourceCopy": source, "incomingCopy": incoming, "destinationCopy": destination} {
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedMap(move.Object, encoded, "status", key)
	}
	if _, err := api.Resource(volumeapi.MoveResource).Create(ctx, move, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	plan, err := node.Volumes.GetMove(ctx, move.GetName())
	if err != nil {
		t.Fatal(err)
	}
	if err := volumeapi.ValidatePreparedMovePlan(plan); err != nil {
		t.Fatalf("invalid prepared Node fixture: %v", err)
	}
	if err := volumeapi.ValidateMoveTransactionIdentities(plan, volumeapi.MoveDestinationAnchor{
		InstallationID: destination.InstallationID, PoolName: destination.PoolName, PoolUID: destination.PoolUID,
		NodeName: destination.NodeName, VolumeUID: destination.VolumeUID,
	}); err != nil {
		t.Fatalf("invalid prepared transaction fixture: %v", err)
	}
	parent, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current := source
	if reason == "MoveSource" {
		current = destination
	}
	encoded, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&current)
	_ = unstructured.SetNestedMap(parent.Object, encoded, "status", "currentCopy")
	_ = unstructured.SetNestedField(parent.Object, volumePhase, "status", "phase")
	_ = unstructured.SetNestedField(parent.Object, owner, "status", "ownerNode")
	_ = unstructured.SetNestedField(parent.Object, move.GetName(), "status", "activeMove")
	_ = unstructured.SetNestedStringSlice(parent.Object, published, "status", "publishedNodes")
	if _, err := api.Resource(volumeapi.VolumeResource).UpdateStatus(ctx, parent, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pool, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pool.SetName(remote.PoolName)
	pool.SetUID("pool-b-uid")
	_ = unstructured.SetNestedField(pool.Object, remote.NodeName, "spec", "nodeName")
	_ = unstructured.SetNestedSlice(pool.Object, []any{map[string]any{"marker": current.CopyID, "identity": encoded, "present": true, "published": reason == "MoveSource"}}, "status", "inventory", "copies")
	if _, err := api.Resource(volumeapi.PoolResource).Create(ctx, pool, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	node.HostRoot = t.TempDir()
	root := filepath.Join(node.HostRoot, "mnt/pool")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "volumes", testID)
	authority := func(context.Context) error { return nil }
	if target.Role == volume.RoleIncoming {
		path = filepath.Join(root, ".shiftpv/incoming", target.CopyID)
		err = ownership.PopulateIncoming(ctx, root, target, "copy-move-uid", authority, func(_ context.Context, path string) error {
			return os.WriteFile(filepath.Join(path, "payload"), []byte("exact move payload"), 0600)
		})
	} else {
		err = ownership.PrepareServing(ctx, root, target, authority)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "payload"), []byte("exact move payload"), 0600); err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: operation, Target: target, Reason: reason,
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.GetName(), UID: string(move.GetUID())}})
	if err != nil {
		t.Fatal(err)
	}
	if err := bindNodeCleanup(ctx, node, cleanup); err != nil {
		t.Fatal(err)
	}
	cleanup, err = node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return moveCleanupFixture{node: node, api: api, kube: kube, cleanup: cleanup, path: path}
}

func (f moveCleanupFixture) request() *protocol.EffectRequest {
	return &protocol.EffectRequest{ExecutorUid: f.node.Identity.PodUID, NodeName: f.node.Identity.NodeName,
		VolumeName: testID, VolumeUid: f.cleanup.Spec.Target.VolumeUID, OperationId: f.cleanup.Spec.OperationID}
}

func (f moveCleanupFixture) mutate(t *testing.T, resource schema.GroupVersionResource, name string, change func(*unstructured.Unstructured)) {
	t.Helper()
	object, err := f.api.Resource(resource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change(object)
	if err := f.api.Tracker().Update(resource, object, ""); err != nil {
		t.Fatal(err)
	}
}

func (f moveCleanupFixture) requirePayload(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.path, "payload"))
	if err != nil || string(data) != "exact move payload" {
		t.Fatalf("unapproved cleanup changed payload: %q %v", data, err)
	}
}

func initialWatchObjects(stream *watch.RaceFreeFakeWatcher, object *unstructured.Unstructured) {
	initial := object.DeepCopy()
	initial.SetResourceVersion("1")
	stream.Add(initial)
	bookmark := &unstructured.Unstructured{}
	bookmark.SetResourceVersion("1")
	bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	stream.Action(watch.Bookmark, bookmark)
}
