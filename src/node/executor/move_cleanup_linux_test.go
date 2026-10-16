//go:build linux

package executor

import (
	"context"
	"os"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/volume"
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
