package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// A historical Completed journal does not make a surviving executor harmless.
// Exercise the ordinary FSM observation and completion paths, not only Reclaim.
func TestCompletedLegacyCleanupKeepsMoveAuthorityWhileReceiptPodRemains(t *testing.T) {
	for _, phase := range []string{"CleaningSource", "Completing"} {
		for _, missingJob := range []bool{false, true} {
			t.Run(phase+"/"+completedJobCase(missingJob), func(t *testing.T) {
				ctx := context.Background()
				r, repo, kube := cleanupMoveFixture()
				repo.journalParents = r.Cleanups.Client
				repo.moves[0].Status.Phase, repo.moves[0].Status.CapacityApproved = phase, true
				repo.moves[0].Finalizers = []string{volumeapi.MoveProtectionFinalizer}
				move := repo.moves[0]
				spec, err := moveCleanupSpec(move)
				if err != nil {
					t.Fatal(err)
				}
				journal, pod := completedLegacyWithLivePod(t, r, kube, spec, missingJob)
				before, err := repo.GetMove(ctx, move.Name)
				if err != nil {
					t.Fatal(err)
				}
				state := repo.volumes[move.Spec.VolumeID]
				pool, _ := cleanupPoolObject(t, r.Cleanups, spec.Target.PoolName)
				kube.ClearActions()
				if err := r.reconcilePersistedMove(ctx, before); !errors.Is(err, nodeexecutor.ErrLegacyExecution) {
					t.Fatalf("live completed executor did not retain Move authority: %v", err)
				}
				after, err := repo.GetMove(ctx, move.Name)
				if err != nil || !sameMoveAuthority(before.Status, after.Status) || after.UID != before.UID || after.Spec != before.Spec ||
					!reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) || !slices.Contains(after.Finalizers, volumeapi.MoveProtectionFinalizer) {
					t.Fatalf("live completed executor changed phase, hold, authority or protection: before=%+v after=%+v err=%v", before, after, err)
				}
				assertCompletedObservationReadOnly(t, r, kube, move, journal, pool.Object)
				deleteCompletedReceiptPod(t, kube, pod)
				if err := r.reconcilePersistedMove(ctx, after); err != nil {
					t.Fatal(err)
				}
				if phase == "CleaningSource" {
					if repo.moves[0].Status.Phase != "Completing" || repo.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
						t.Fatalf("cleanup observation skipped completion boundary: move=%+v state=%+v", repo.moves[0], repo.volumes[move.Spec.VolumeID])
					}
					if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
						t.Fatal(err)
					}
				}
				if repo.moves[0].Status.Phase != "Succeeded" || repo.volumes[move.Spec.VolumeID].ActiveMove != "" {
					t.Fatalf("quiescent completed history did not release authority: move=%+v state=%+v", repo.moves[0], repo.volumes[move.Spec.VolumeID])
				}
				if got := listedJournal(t, r, move); !reflect.DeepEqual(got, journal) {
					t.Fatalf("candidate rewrote completed proof: before=%+v after=%+v", journal, got)
				}
			})
		}
	}
}

func TestCompletedLegacyCleanupKeepsTerminalHistoryWhileReceiptPodRemains(t *testing.T) {
	for _, phase := range []string{"Succeeded", "Blocked"} {
		for _, protected := range []bool{false, true} {
			for _, missingJob := range []bool{false, true} {
				name := phase + "/unprotected/" + completedJobCase(missingJob)
				if protected {
					name = phase + "/protected/" + completedJobCase(missingJob)
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					r, repo, _, move, incoming := settledTerminalMoveFixture(t, phase, recoveryRecovered, recoveryCapacitySettled)
					if phase == "Succeeded" {
						move.Status.RecoveryPhase, move.Status.CapacityReason = "", ""
					}
					move.Status.LastTransitionTime = r.now().Add(-DefaultMoveJournalRetention - time.Hour).Format(time.RFC3339Nano)
					move.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
					repo.moves[0] = move
					spec := rollbackCleanupSpec(move, incoming)
					if phase == "Succeeded" {
						var err error
						spec, err = moveCleanupSpec(move)
						if err != nil {
							t.Fatal(err)
						}
					}
					kube := r.Client.(*fake.Clientset)
					assignJobUIDs(kube)
					_, pod := completedLegacyWithLivePod(t, r, kube, spec, missingJob)
					if !protected {
						stripMoveProtection(t, repo, move)
					}
					before, err := repo.GetMove(ctx, move.Name)
					if err != nil {
						t.Fatal(err)
					}
					journal := listedJournal(t, r, move)
					state := repo.volumes[move.Spec.VolumeID]
					pool, _ := cleanupPoolObject(t, r.Cleanups, spec.Target.PoolName)
					kube.ClearActions()
					if err := r.reconcilePersistedMove(ctx, before); !errors.Is(err, nodeexecutor.ErrLegacyExecution) {
						t.Fatalf("live completed executor did not retain terminal history: %v", err)
					}
					after, err := repo.GetMove(ctx, move.Name)
					if err != nil || !sameMoveAuthority(before.Status, after.Status) || after.UID != before.UID || after.Spec != before.Spec ||
						!reflect.DeepEqual(after.Finalizers, before.Finalizers) || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
						t.Fatalf("live completed executor released finalizer/history/authority: before=%+v after=%+v err=%v", before, after, err)
					}
					assertCompletedObservationReadOnly(t, r, kube, move, journal, pool.Object)
					deleteCompletedReceiptPod(t, kube, pod)
					if err := r.reconcilePersistedMove(ctx, after); err != nil {
						t.Fatal(err)
					}
					if protected {
						if len(repo.moves) != 1 || slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) {
							t.Fatal("settled history did not remove finalizer before retention GC")
						}
						if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
							t.Fatal(err)
						}
					}
					if len(repo.moves) != 0 || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
						t.Fatalf("quiescent expired history did not GC without changing Volume: moves=%+v state=%+v", repo.moves, repo.volumes[move.Spec.VolumeID])
					}
				})
			}
		}
	}
}

func TestCompletedLegacyRollbackKeepsHoldWhileReceiptPodRemains(t *testing.T) {
	for _, missingJob := range []bool{false, true} {
		t.Run(completedJobCase(missingJob), func(t *testing.T) {
			ctx := context.Background()
			r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
			kube := r.Client.(*fake.Clientset)
			assignJobUIDs(kube)
			move := repo.moves[0]
			spec := rollbackCleanupSpec(move, incoming)
			journal, pod := completedLegacyWithLivePod(t, r, kube, spec, missingJob)
			state := repo.volumes[move.Spec.VolumeID]
			pool, _ := cleanupPoolObject(t, r.Cleanups, spec.Target.PoolName)
			kube.ClearActions()
			if err := r.reconcilePersistedMove(ctx, move); !errors.Is(err, nodeexecutor.ErrLegacyExecution) {
				t.Fatalf("live completed rollback executor did not retain hold: %v", err)
			}
			if !sameMoveAuthority(move.Status, repo.moves[0].Status) || !repo.moves[0].Status.CapacityApproved || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
				t.Fatalf("completed rollback skipped executor observation: move=%+v state=%+v", repo.moves[0], repo.volumes[move.Spec.VolumeID])
			}
			assertCompletedObservationReadOnly(t, r, kube, move, journal, pool.Object)
			deleteCompletedReceiptPod(t, kube, pod)
			if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
				t.Fatal(err)
			}
			if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
				t.Fatalf("rollback settlement did not release hold before separate source restoration: move=%+v state=%+v", repo.moves[0], repo.volumes[move.Spec.VolumeID])
			}
		})
	}
}

func completedJobCase(missing bool) string {
	if missing {
		return "Job absent"
	}
	return "Job complete"
}

// Use the compatible writer to produce all completion evidence before adding
// the surviving receipt Pod. Candidate code can only inspect this history.
func completedLegacyWithLivePod(t *testing.T, r *Reconciler, kube *fake.Clientset, spec cleanupapi.Spec, missingJob bool) (cleanupapi.Cleanup, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	journal, job := settledLegacyJobHistory(t, r, kube, spec)
	// No selector labels or owner reference: exact receipt UID must still find it.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "surviving-receipt-pod", Namespace: "system", UID: types.UID(journal.Status.Executor.PodUID)},
		Spec:   corev1.PodSpec{NodeName: spec.Target.NodeName, Containers: []corev1.Container{{Name: "cleanup", Image: "helper"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	pod, err := kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if missingJob {
		if err := kube.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &job.UID}}); err != nil {
			t.Fatal(err)
		}
	}
	return journal, pod
}

func settledLegacyJobHistory(t *testing.T, r *Reconciler, kube *fake.Clientset, spec cleanupapi.Spec) (cleanupapi.Cleanup, *batchv1.Job) {
	t.Helper()
	ctx := context.Background()
	job := terminalVerifyingJob(t, r, kube, spec)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := kube.BatchV1().Jobs(job.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	settleLegacyCleanupFixture(t, r, kube, spec)
	journal, err := r.Cleanups.Get(ctx, spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return journal, job
}

// Retention tests vary only the parent lifecycle. Supply the independently
// settled history their Completed input claims, including finalizer-free reads.
func seedSettledFinalizerHistory(t *testing.T, r *Reconciler, repo *memoryRepository) {
	t.Helper()
	move := repo.moves[0]
	if move.Status.CleanupPhase != cleanupapi.PhaseCompleted {
		return
	}
	withTestCleanups()(r)
	source, _, _ := testCopyIdentities(move.Spec.VolumeID, move.Spec.SourceNode, "destination")
	move.Status.SourceCopy = &source
	repo.moves[0] = move
	repo.journalParents = r.Cleanups.Client
	kube := r.Client.(*fake.Clientset)
	assignJobUIDs(kube)
	spec, err := moveCleanupSpec(move)
	if err != nil {
		t.Fatal(err)
	}
	settledLegacyJobHistory(t, r, kube, spec)
	parent, err := r.Cleanups.Client.Resource(volumeapi.MoveResource).Get(context.Background(), move.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	parent.SetFinalizers(move.Finalizers)
	if _, err := r.Cleanups.Client.Resource(volumeapi.MoveResource).Update(context.Background(), parent, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func deleteCompletedReceiptPod(t *testing.T, kube *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	if err := kube.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil {
		t.Fatal(err)
	}
}

func assertCompletedObservationReadOnly(t *testing.T, r *Reconciler, kube *fake.Clientset, move volumeapi.Move, before cleanupapi.Cleanup, poolBefore map[string]any) {
	t.Helper()
	if current := listedJournal(t, r, move); !reflect.DeepEqual(current, before) {
		t.Fatalf("candidate changed completed journal: before=%+v after=%+v", before, current)
	}
	pool, _ := cleanupPoolObject(t, r.Cleanups, before.Spec.Target.PoolName)
	if !reflect.DeepEqual(pool.Object, poolBefore) {
		t.Fatalf("candidate opened/replaced absence fence: before=%+v after=%+v", poolBefore, pool.Object)
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetResource().Resource != "events" {
			t.Fatalf("candidate mutated legacy executor: %+v", action)
		}
	}
}
