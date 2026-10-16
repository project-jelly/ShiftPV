package controller

import (
	"context"
	"errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
		Conditions: testBackingConditions(1),
		Inventory:  &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true},
	}
	r.Now = func() time.Time { return now }
	r.PoolLocks = &poolcapacity.Locker{}
	r.CapacityProbe = fakeMoveCapacityProbe{usage: 1 << 20, stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	r.SourceUsage = r.CapacityProbe.(SourceUsage)
	clearDestinationEffectIntent(&move)
	move.Status.CapacityApproved = false
	prepareCapacityMove(repo, &move, repo.pools[1])
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if !repo.moves[0].Status.CapacityApproved || noDestinationEffectIntent(repo.moves[0]) {
		t.Fatalf("approval did not persist its execution plan: %+v", repo.moves[0].Status)
	}
	return r, repo, client
}

func TestApprovedMoveRecoversBeforeCopyStarts(t *testing.T) {
	t.Run("complete approval", func(t *testing.T) {
		ctx := context.Background()
		r, repo, client := approvedBeforeCopyFixture(t)
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
		if _, err := r.Cleanups.Get(ctx, moveCleanupAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
			t.Fatalf("no-effect recovery fabricated a purge receipt: %v", err)
		}
		// A completed no-artifact rollback is closed history. Its real causal
		// absence path above must not require a fabricated cleanup receipt before
		// this Volume can admit its next prepared transaction.
		if err := restarted.validateSettledMoves(ctx, repo.moves); err != nil {
			t.Fatalf("completed rollback blocks subsequent admission: %v", err)
		}
		oldConsumer, err := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := client.CoreV1().Pods(oldConsumer.Namespace).Delete(ctx, oldConsumer.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &oldConsumer.UID}}); err != nil {
			t.Fatal(err)
		}
		held := oldConsumer.DeepCopy()
		held.Name, held.UID, held.ResourceVersion = "next-consumer", "next-consumer-uid", ""
		held.Spec.NodeName, held.Status.Phase = "", corev1.PodPending
		held.Labels = map[string]string{admission.ManagedLabel: "true"}
		held.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}, {Name: "example.test/user"}}
		if _, err := client.CoreV1().Pods(held.Namespace).Create(ctx, held, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := restarted.reconcileOwnerPlacements(ctx); err != nil {
			t.Fatal(err)
		}
		released, err := client.CoreV1().Pods(held.Namespace).Get(ctx, held.Name, metav1.GetOptions{})
		if err != nil || released.UID != held.UID || hasPlacementHold(released) || released.Spec.NodeSelector[corev1.LabelHostname] != "source" || len(released.Spec.SchedulingGates) != 1 {
			t.Fatalf("closed rollback history blocked owner recovery: %+v %v", released, err)
		}
		// The user gate owner releases its gate; the scheduler then assigns this Pod.
		released.Spec.NodeName, released.Status.Phase = "source", corev1.PodRunning
		released.Spec.SchedulingGates = nil
		if _, err := client.CoreV1().Pods(released.Namespace).Update(ctx, released, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		source.Spec.Unschedulable = true
		if _, err := client.CoreV1().Nodes().Update(ctx, source, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := restarted.discoverMoves(ctx); err != nil {
			t.Fatal(err)
		}
		if len(repo.moves) != 2 || repo.moves[1].Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 {
			t.Fatalf("retained recovery history blocked next Move: %+v", repo.moves)
		}

	})
}

func assertApprovalReservesDestination(t *testing.T, _ *Reconciler, repo *memoryRepository) {
	t.Helper()
	reserved, err := poolcapacity.ReservedBytesForPool(repo.volumes, repo.moves, repo.pools[1].UID)
	if err != nil || reserved != 32<<20 || poolcapacity.FitsReservation(1, reserved, 32<<20) {
		t.Fatalf("unsettled approval did not retain exact destination reservation: %d %v", reserved, err)
	}
}

func TestIncompleteApprovalRetainsAuthorityWithoutInventingRollback(t *testing.T) {
	for _, protocol := range []string{"", volumeapi.MoveProtocolPreparedV1} {
		t.Run(protocol, func(t *testing.T) {
			f, move := preparedRecoveryStatusFixture(t)
			object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if protocol == "" {
				unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
			}
			for _, field := range []string{"incomingCopy", "destinationCopy", "copyJobName", "promotionJobName", "copyOperationID", "promotionOperationID"} {
				unstructured.RemoveNestedField(object.Object, "status", field)
			}
			_ = unstructured.SetNestedField(object.Object, recoveryRetiring, "status", "recoveryPhase")
			if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
				t.Fatal(err)
			}
			move = f.move(t, 0)
			before := f.state(t, 0)
			pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err == nil {
				t.Fatal("incomplete approval was not reported")
			}
			after := f.move(t, 0)
			latestPool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(pool, latestPool) || !reflect.DeepEqual(before, f.state(t, 0)) ||
				!after.Status.CapacityApproved || !noDestinationEffectIntent(after) || after.Status.RollbackRequiredGeneration != 0 {
				t.Fatalf("incomplete approval inferred rollback or released authority: move=%+v err=%v", after, err)
			}
			if _, err := f.reconciler.Cleanups.Get(f.ctx, moveCleanupAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
				t.Fatal(err)
			}
			jobs, err := f.client.BatchV1().Jobs("system").List(f.ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 0 {
				t.Fatalf("unexpected effects: %+v %v", jobs, err)
			}
		})
	}
}
