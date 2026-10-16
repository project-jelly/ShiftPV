package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestRecoveryRollbackWithUnrelatedPublishedVolume(t *testing.T) {
	for _, artifact := range []string{"absent", volume.RoleIncoming, volume.RoleServing} {
		for _, unrelatedFirst := range []bool{false, true} {
			name := artifact + "/unrelated-last"
			if unrelatedFirst {
				name = artifact + "/unrelated-first"
			}
			t.Run(name, func(t *testing.T) {
				r, repo, incoming, destination := rollbackRecoveryFixture(t, nil)
				unrelated := destination
				unrelated.VolumeID = "shiftpv-fedcba9876543210fedcba9876543210"
				unrelated.VolumeUID = "unrelated-volume-uid"
				unrelated.CopyID = "unrelated-serving-copy"
				if err := unrelated.Validate(); err != nil {
					t.Fatal(err)
				}
				observation := volumeapi.CopyObservation{Identity: &unrelated, Present: true, Published: true}
				copies := []volumeapi.CopyObservation{observation}
				target := incoming
				if artifact == volume.RoleServing {
					target = destination
				}
				if artifact != "absent" {
					current := volumeapi.CopyObservation{Identity: &target, Present: true}
					if unrelatedFirst {
						copies = append(copies, current)
					} else {
						copies = append([]volumeapi.CopyObservation{current}, copies...)
					}
				}
				repo.pools[1].Status.Inventory.Copies = copies
				unrelatedState := volumeapi.State{UID: unrelated.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: unrelated.NodeName, CurrentCopy: &unrelated, PublishedNodes: []string{unrelated.NodeName}, CapacityBytes: 32 << 20}
				repo.volumes[unrelated.VolumeID] = unrelatedState
				move := repo.moves[0]
				if err := r.reconcilePersistedMove(context.Background(), move); err != nil {
					t.Fatalf("recovery rejected unrelated publication: %v", err)
				}
				if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
					t.Fatal("rollback did not settle its capacity hold")
				}
				journal, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move))
				if artifact == "absent" {
					if !errors.Is(err, cleanupapi.ErrNoJournal) {
						t.Fatalf("absent transaction created cleanup: %+v, %v", journal, err)
					}
				} else if err != nil || journal.Spec.Target != target || journal.Status.Phase != "Completed" {
					t.Fatalf("cleanup must target only the transaction copy: %+v, %v", journal, err)
				}
				if !reflect.DeepEqual(repo.volumes[unrelated.VolumeID], unrelatedState) {
					t.Fatal("unrelated volume state changed")
				}
				state := repo.volumes[move.Spec.VolumeID]
				if state.OwnerNode != move.Spec.SourceNode || state.ActiveMove != move.Name || state.Phase != volumeapi.PhaseBlocked {
					t.Fatalf("source authority or recovery guard changed: %+v", state)
				}
				if err := r.reconcilePersistedMove(context.Background(), repo.moves[0]); err != nil {
					t.Fatal(err)
				}
				if repo.moves[0].Status.RecoveryPhase != recoveryResuming {
					t.Fatal("settled rollback did not advance after read-back")
				}
			})
		}
	}
}
