package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestWorkloadRetirementRequiresAbsenceRatherThanAPIFailure(t *testing.T) {
	for _, scenario := range []string{"timeout", "forbidden", "unknown", "in-use deletion"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, client := recoveryFixture(t, "source")
			before := repo.volumes[repo.moves[0].Spec.VolumeID]
			if scenario == "in-use deletion" {
				claim, _ := client.CoreV1().PersistentVolumeClaims("workload").Get(ctx, "claim", metav1.GetOptions{})
				now := metav1.Now()
				claim.DeletionTimestamp = &now
				if _, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				failure := errors.New("unavailable observation")
				if scenario == "timeout" {
					failure = apierrors.NewTimeoutError("timeout", 1)
				}
				if scenario == "forbidden" {
					failure = apierrors.NewForbidden(volumeapi.VolumeResource.GroupResource(), "claim", errors.New("denied"))
				}
				client.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, failure
				})
			}
			if err := r.ReconcileAll(ctx); err == nil {
				t.Fatal("unknown or still-protected binding was accepted")
			}
			if repo.moves[0].Status.SettlementRequested || !repo.moves[0].Status.CapacityApproved || !reflect.DeepEqual(before, repo.volumes[repo.moves[0].Spec.VolumeID]) {
				t.Fatal("unknown lifetime granted settlement or changed authority/accounting")
			}
			jobs, _ := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
			if len(jobs.Items) != 0 {
				t.Fatal("unknown lifetime started an executor")
			}
		})
	}
}

func TestWorkloadSettlementAndOwnerCommitCompeteOnExactVolumeCAS(t *testing.T) {
	for _, scenario := range []string{"commit wins", "settlement wins", "copy changed"} {
		t.Run(scenario, func(t *testing.T) {
			f, move := preparationAtCapacity(t)
			if err := f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
				t.Fatal(err)
			}
			move = f.move(t, 0)
			claim := f.workloads[0].Claim
			consumer := f.workloads[0].Consumer
			if err := f.client.CoreV1().Pods(consumer.Namespace).Delete(f.ctx, consumer.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			if err := f.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Delete(f.ctx, claim.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			observed, err := f.reconciler.observeLifecycle(f.ctx, move)
			if err != nil || !observed.FSM.WorkloadRetired {
				t.Fatalf("retirement not observed: %+v %v", observed.FSM, err)
			}
			source := f.state(t, 0)
			committed := source
			committed.Phase, committed.OwnerNode, committed.CurrentCopy = volumeapi.PhaseReady, "destination", move.Status.DestinationCopy
			if scenario == "settlement wins" {
				if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
					t.Fatal(err)
				}
				if err := f.registry.CompareAndSetState(f.ctx, move.Spec.VolumeID, source.Phase, move.Name, source.OwnerNode, committed); !errors.Is(err, volumeapi.ErrStateConflict) {
					t.Fatalf("stale owner commit crossed settlement fence: %v", err)
				}
				if live := f.state(t, 0); live.OwnerNode != "source" || live.Phase != volumeapi.PhaseBlocked || *live.CurrentCopy != *source.CurrentCopy {
					t.Fatalf("settlement lost source authority: %+v", live)
				}
				return
			}
			if scenario == "copy changed" {
				copy := *source.CurrentCopy
				copy.CopyID = "different-copy"
				committed = source
				committed.CurrentCopy = &copy
			}
			if err := f.registry.CompareAndSetState(f.ctx, move.Spec.VolumeID, source.Phase, move.Name, source.OwnerNode, committed); err != nil {
				t.Fatal(err)
			}
			if err := f.reconciler.requestWorkloadSettlement(f.ctx, &move, observed); !errors.Is(err, volumeapi.ErrStateConflict) {
				t.Fatalf("stale retirement replaced a winning authority: %v", err)
			}
			live := f.state(t, 0)
			if live.OwnerNode != committed.OwnerNode || *live.CurrentCopy != *committed.CurrentCopy || f.move(t, 0).Status.SettlementRequested {
				t.Fatal("losing settlement changed the winner or persisted stale intent")
			}
			if scenario == "commit wins" {
				if err := f.reconciler.reconcileMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				if live = f.state(t, 0); live.OwnerNode != "destination" || live.Phase != volumeapi.PhaseBlocked || !f.move(t, 0).Status.SettlementRequested {
					t.Fatalf("fresh retirement did not follow committed destination: %+v", live)
				}
			}
		})
	}
}

func TestWorkloadSettlementReobservesAcceptedWritesAfterResponseLoss(t *testing.T) {
	for _, scenario := range []string{"Volume fence", "Move intent", "final unlock"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, client := recoveryFixture(t, "source")
			move := repo.moves[0]
			move.Spec.Recovery, move.Status.Phase = "", "Copying"
			state := repo.volumes[move.Spec.VolumeID]
			state.Phase = volumeapi.PhaseMoving
			repo.moves[0], repo.volumes[move.Spec.VolumeID] = move, state
			if err := client.CoreV1().Pods("workload").Delete(ctx, "consumer", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := client.CoreV1().PersistentVolumeClaims("workload").Delete(ctx, "claim", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			lost := &lostResponseRepository{memoryRepository: repo}
			if scenario == "Volume fence" {
				lost.stateCASResponses = 1
			}
			if scenario == "Move intent" {
				lost.statusResponses = 1
			}
			injected, sawFailure := scenario != "final unlock", false
			for cycle := 0; cycle < 32; cycle++ {
				if !injected && repo.moves[0].Status.RecoveryPhase == recoveryCompleting {
					lost.stateCASResponses, injected = 1, true
				}
				restarted := newTestReconciler(client, lost, withCleanupsFrom(r), func(next *Reconciler) { next.ServiceAccountName = r.ServiceAccountName })
				if err := restarted.ReconcileAll(ctx); err != nil {
					sawFailure = true
				}
				finishRecoveryJobs(t, client, batchv1.JobComplete)
				pool := &repo.pools[1]
				pool.Status.ObservedGeneration = pool.Generation
				pool.Status.Conditions = testBackingConditions(pool.Generation)
				pool.Status.LastProbeTime = metav1.Now()
				pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.Now()}
			}
			live := repo.volumes[move.Spec.VolumeID]
			if !injected || !sawFailure || live.OwnerNode != state.OwnerNode || *live.CurrentCopy != *state.CurrentCopy || live.ActiveMove != "" || !volumeapi.MoveCleanupSettled(repo.moves[0]) {
				t.Fatalf("response loss did not settle exact authority: injected=%v failed=%v state=%+v move=%+v", injected, sawFailure, live, repo.moves[0].Status)
			}
		})
	}
}

func TestSettlementCompletionSurvivesCSIReclaimAfterUnlock(t *testing.T) {
	for _, scenario := range []string{"ready", "deleting", "gone", "API unknown", "new Volume UID", "missing artifact proof"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, client := recoveryFixture(t, "source")
			move := repo.moves[0]
			move.Spec.Recovery = ""
			move.Status.SettlementRequested, move.Status.CapacityApproved = true, false
			move.Status.CapacityReason, move.Status.RecoveryOwner, move.Status.RecoveryPhase = recoveryCapacitySettled, "source", recoveryCompleting
			move.Status.RollbackRequiredGeneration = 2
			state := repo.volumes[move.Spec.VolumeID]
			state.Phase, state.ActiveMove = volumeapi.PhaseReady, ""
			if scenario == "deleting" {
				state.Phase, state.DeletionOperationID, state.HasCleanup = volumeapi.PhaseDeleting, "delete-"+state.UID, true
			}
			if scenario == "new Volume UID" {
				state.UID = "new-volume-uid"
			}
			if scenario == "missing artifact proof" {
				move.Status.RollbackRequiredGeneration = 0
			}
			repo.moves[0], repo.volumes[move.Spec.VolumeID] = move, state
			if scenario == "gone" {
				delete(repo.volumes, move.Spec.VolumeID)
				r.Repository = &completionReadRepository{memoryRepository: repo, getError: apierrors.NewNotFound(volumeapi.VolumeResource.GroupResource(), move.Spec.VolumeID)}
			}
			if scenario == "API unknown" {
				r.Repository = &completionReadRepository{memoryRepository: repo, getError: errors.New("API unavailable")}
			}
			client.ClearActions()
			err := r.reconcileMove(ctx, move)
			valid := scenario == "ready" || scenario == "deleting" || scenario == "gone"
			if (err == nil) != valid {
				t.Fatalf("completion for %s: %v", scenario, err)
			}
			if valid && repo.moves[0].Status.RecoveryPhase != recoveryRecovered {
				t.Fatalf("accepted deletion stranded metadata: %+v", repo.moves[0].Status)
			}
			if !valid && repo.moves[0].Status.RecoveryPhase == recoveryRecovered {
				t.Fatal("unknown authority or missing artifacts were marked completed")
			}
			if scenario != "gone" && !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
				t.Fatal("metadata completion altered the remaining/new Volume")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" {
					t.Fatalf("metadata completion recreated an effect: %+v", action)
				}
			}
		})
	}
}
