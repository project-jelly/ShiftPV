//go:build linux

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": testID, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
		"status": map[string]any{"phase": phase, "destinationNode": destination.NodeName, "destinationPoolUID": destination.PoolUID,
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

func TestNodeMoveCleanupExecutesExactSourceAndRollback(t *testing.T) {
	for _, tc := range []struct{ reason, role string }{{"MoveSource", volume.RoleServing}, {"MoveRollback", volume.RoleIncoming}, {"MoveRollback", volume.RoleServing}} {
		t.Run(tc.reason+tc.role, func(t *testing.T) {
			f := newMoveCleanupFixture(t, tc.reason, tc.role)
			if err := f.node.execute(context.Background(), testID); err != nil {
				t.Fatal(err)
			}
			current, err := f.node.Cleanups.Get(context.Background(), f.cleanup.Spec.Authority)
			if err != nil || current.Status.Phase != cleanupapi.PhaseVerifying || current.Status.Receipt == nil || current.Status.Receipt.ExecutorUID != f.node.Identity.PodUID {
				t.Fatalf("missing native Move receipt: %+v %v", current, err)
			}
			if _, err := os.Stat(f.path); !os.IsNotExist(err) {
				t.Fatalf("exact target remains: %v", err)
			}
			if current.Status.AbsenceProof != nil {
				t.Fatal("Node supplied controller absence proof")
			}
			state, err := f.node.Volumes.Get(context.Background(), testID)
			if err != nil || state.ActiveMove != "move-a" {
				t.Fatalf("Node released Move authority: %+v %v", state, err)
			}
		})
	}
}
