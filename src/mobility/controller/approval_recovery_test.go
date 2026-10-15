package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func clearDestinationEffectIntent(move *volumeapi.Move) {
	move.Status.IncomingCopy, move.Status.DestinationCopy = nil, nil
	move.Status.CopyJobName, move.Status.PromotionJobName = "", ""
	move.Status.CopyOperationID, move.Status.PromotionOperationID = "", ""
}

func approvedBeforeCopyFixture(t *testing.T) (*Reconciler, *memoryRepository, *fake.Clientset) {
	t.Helper()
	r, repo, client := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Status.Phase, move.Status.Reason = string(fsm.PhaseWaitingForCapacity), ""
	state := repo.volumes[move.Spec.VolumeID]
	state.Phase = volumeapi.PhaseMoving
	repo.volumes[move.Spec.VolumeID], repo.moves[0] = state, move
	now := time.Now().UTC()
	pool := &repo.pools[1]
	pool.CapacityLimit, pool.Generation = "32Mi", 1
	pool.Status = volumeapi.PoolStatus{
		ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		Inventory:  &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true},
	}
	r.Now = func() time.Time { return now }
	r.PoolLocks = &poolcapacity.Locker{}
	r.CapacityProbe = fakeMoveCapacityProbe{usage: 1 << 20, stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if !repo.moves[0].Status.CapacityApproved || noDestinationEffectIntent(repo.moves[0]) {
		t.Fatalf("approval did not persist its execution plan: %+v", repo.moves[0].Status)
	}
	return r, repo, client
}

func TestApprovedMoveRecoversBeforeCopyStarts(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "complete approval"
		if legacy {
			name = "legacy approval without execution intent"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, repo, client := approvedBeforeCopyFixture(t)
			if legacy {
				clearDestinationEffectIntent(&repo.moves[0])
			}
			move := repo.moves[0]
			source, err := client.CoreV1().Nodes().Get(ctx, "source", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			unready := source.DeepCopy()
			unready.Status.Conditions = nil
			if _, err := client.CoreV1().Nodes().UpdateStatus(ctx, unready, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := r.reconcileMove(ctx, move); err != nil {
				t.Fatal(err)
			}
			if repo.moves[0].Status.Phase != "Blocked" || repo.moves[0].Status.Reason != "SourceUnavailable" {
				t.Fatalf("source interruption did not block the approved move: %+v", repo.moves[0].Status)
			}
			if _, err := client.CoreV1().Nodes().UpdateStatus(ctx, source, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for cycle := 0; cycle < 8 && repo.moves[0].Status.RollbackRequiredGeneration == 0; cycle++ {
				if err := r.ReconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				finishRecoveryJobs(t, client, batchv1.JobComplete)
			}
			if repo.moves[0].Status.RollbackRequiredGeneration != 2 || !repo.moves[0].Status.CapacityApproved {
				t.Fatalf("recovery skipped its post-Retiring fence: %+v", repo.moves[0].Status)
			}
			assertApprovalReservesDestination(t, r, repo)
			restarted := newTestReconciler(client, repo, withCleanupsFrom(r), func(next *Reconciler) {
				next.Now, next.ServiceAccountName = r.Now, r.ServiceAccountName
			})
			if err := restarted.ReconcileAll(ctx); err == nil || !repo.moves[0].Status.CapacityApproved {
				t.Fatal("restart released the hold using a pre-Retiring inventory")
			}
			observeRollbackGeneration(repo)
			for cycle := 0; cycle < 8; cycle++ {
				if err := restarted.ReconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				state := repo.volumes[move.Spec.VolumeID]
				if state.OwnerNode != "source" || *state.CurrentCopy != *move.Status.SourceCopy {
					t.Fatal("recovery changed the authoritative copy")
				}
				if state.Phase == volumeapi.PhaseReady {
					state.PublishedNodes = []string{"source"}
					repo.volumes[move.Spec.VolumeID] = state
				}
			}
			if repo.moves[0].Status.RecoveryPhase != recoveryRecovered || repo.volumes[move.Spec.VolumeID].ActiveMove != "" {
				t.Fatalf("approved-but-unstarted move did not recover: %+v", repo.moves[0].Status)
			}
			reserved, err := poolcapacity.ReservedBytesForPool(repo.volumes, repo.moves, repo.pools[1].UID)
			if err != nil || reserved != 0 {
				t.Fatalf("settled move retained capacity: %d %v", reserved, err)
			}
			if legacy && !noDestinationEffectIntent(repo.moves[0]) {
				t.Fatal("legacy absence proof fabricated execution authority")
			}
			if _, err := r.Cleanups.Get(ctx, moveCleanupAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
				t.Fatalf("no-effect recovery fabricated a purge receipt: %v", err)
			}
		})
	}
}

func assertApprovalReservesDestination(t *testing.T, r *Reconciler, repo *memoryRepository) {
	t.Helper()
	id := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	other := volumeapi.Move{Name: "competing-move", UID: "competing-uid", Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: "source"}}
	repo.volumes[id] = volumeapi.State{Phase: volumeapi.PhaseMoving, ActiveMove: other.Name, OwnerNode: "source", CapacityBytes: 32 << 20}
	repo.moves = append(repo.moves, other)
	defer func() { delete(repo.volumes, id); repo.moves = repo.moves[:1] }()
	if err := r.ensureCapacity(context.Background(), &other, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if other.Status.CapacityApproved || other.Status.CapacityReason != "DestinationReservationLimit" {
		t.Fatalf("unsettled approval allowed oversubscription: %+v", other.Status)
	}
}

func TestLegacyUnstartedApprovalCannotAuthorizeArtifactCleanup(t *testing.T) {
	for _, scenario := range []string{"incoming artifact", "serving artifact", "replaced pool", "changed source"} {
		t.Run(scenario, func(t *testing.T) {
			r, repo, incoming, destination := rollbackRecoveryFixture(t, nil)
			clearDestinationEffectIntent(&repo.moves[0])
			repo.moves[0].Status.RollbackRequiredGeneration = 0
			move := repo.moves[0]
			state, _ := repo.Get(context.Background(), move.Spec.VolumeID)
			if _, err := r.settleRecoveryArtifacts(context.Background(), &move, state); err != nil {
				t.Fatal(err)
			}
			observeRollbackGeneration(repo)
			switch scenario {
			case "incoming artifact":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &incoming, Present: true}}
			case "serving artifact":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &destination, Present: true}}
			case "replaced pool":
				repo.pools[1].UID = "replacement-uid"
			case "changed source":
				copy := *state.CurrentCopy
				copy.CopyID = "another-copy"
				state.CurrentCopy = &copy
			}
			move = repo.moves[0]
			if _, err := r.settleRecoveryArtifacts(context.Background(), &move, state); err == nil {
				t.Fatal("contradictory evidence settled an unstarted approval")
			}
			if !repo.moves[0].Status.CapacityApproved || !noDestinationEffectIntent(repo.moves[0]) {
				t.Fatal("legacy contradiction released capacity or invented execution intent")
			}
			if _, err := r.Cleanups.Get(context.Background(), moveCleanupAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
				t.Fatalf("legacy contradiction authorized destructive cleanup: %v", err)
			}
		})
	}
}
