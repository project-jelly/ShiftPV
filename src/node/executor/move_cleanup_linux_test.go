//go:build linux

package executor

import (
	"context"
	"os"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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

func TestNodeMoveCleanupSettlesRetiredWorkload(t *testing.T) {
	for _, scenario := range []string{"valid", "destination published after admission", "intent lost after admission"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
			f.mutate(t, volumeapi.MoveResource, "move-a", func(object *unstructured.Unstructured) {
				unstructured.RemoveNestedField(object.Object, "spec", "recovery")
				_ = unstructured.SetNestedField(object.Object, "Blocked", "status", "phase")
				_ = unstructured.SetNestedField(object.Object, "Retiring", "status", "recoveryPhase")
				_ = unstructured.SetNestedField(object.Object, "worker-b", "status", "recoveryOwner")
				_ = unstructured.SetNestedField(object.Object, true, "status", "settlementRequested")
				_ = unstructured.SetNestedField(object.Object, int64(1), "status", "rollbackRequiredGeneration")
			})
			f.mutate(t, volumeapi.VolumeResource, testID, func(object *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(object.Object, volumeapi.PhaseBlocked, "status", "phase")
				_ = unstructured.SetNestedStringSlice(object.Object, nil, "status", "publishedNodes")
			})
			f.mutate(t, volumeapi.PoolResource, "pool-b", func(object *unstructured.Unstructured) {
				copies, _, _ := unstructured.NestedSlice(object.Object, "status", "inventory", "copies")
				copies[0].(map[string]any)["published"] = false
				_ = unstructured.SetNestedSlice(object.Object, copies, "status", "inventory", "copies")
			})
			before, err := f.node.Volumes.Get(ctx, testID)
			if err != nil {
				t.Fatal(err)
			}
			f.node.moveCleanupReady.Store(true)
			if _, done, err := f.node.cleanupRPC(ctx, before, f.request()); err != nil || done {
				t.Fatalf("exact retirement RPC was not admitted: done=%v err=%v", done, err)
			}
			if scenario == "destination published after admission" {
				f.mutate(t, volumeapi.VolumeResource, testID, func(object *unstructured.Unstructured) {
					_ = unstructured.SetNestedStringSlice(object.Object, []string{"worker-b"}, "status", "publishedNodes")
				})
			}
			if scenario == "intent lost after admission" {
				f.mutate(t, volumeapi.MoveResource, "move-a", func(object *unstructured.Unstructured) {
					unstructured.RemoveNestedField(object.Object, "status", "settlementRequested")
				})
			}
			err = f.node.execute(ctx, testID)
			if scenario != "valid" {
				if err == nil {
					t.Fatal("changed retirement authority reached filesystem purge")
				}
				f.requirePayload(t)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
			if err != nil || current.Status.Phase != cleanupapi.PhaseVerifying || current.Status.Receipt == nil || current.Status.AbsenceProof != nil {
				t.Fatalf("missing effect receipt or fabricated absence: %+v %v", current.Status, err)
			}
			if _, err := os.Stat(f.path); !os.IsNotExist(err) {
				t.Fatalf("exact retired source was not purged: %v", err)
			}
			after, err := f.node.Volumes.Get(ctx, testID)
			if err != nil || after.UID != before.UID || after.OwnerNode != before.OwnerNode || *after.CurrentCopy != *before.CurrentCopy || after.ActiveMove != before.ActiveMove {
				t.Fatalf("Node changed committed owner or settled controller authority: %+v %v", after, err)
			}
		})
	}
}
