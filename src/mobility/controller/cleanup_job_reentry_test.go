package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
)

func TestMoveCleanupReentryRequiresLegacyWriterSettlement(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		for _, settlement := range []string{"Job complete", "Job absent but Pod remains"} {
			t.Run(phase+"/"+settlement, func(t *testing.T) {
				ctx := context.Background()
				r, repository, kube := cleanupMoveFixture()
				repository.moves[0].Status.Phase = "CleaningSource"
				repository.moves[0].Status.CapacityApproved = true
				move := repository.moves[0]
				spec, err := moveCleanupSpec(move)
				if err != nil {
					t.Fatal(err)
				}
				prepareLegacyCleanupPool(t, r.Cleanups, spec.Target.PoolName)
				registry := &volumeapi.Registry{Client: r.Cleanups.Client}
				helper := &helperpod.Runner{Client: kube, Namespace: "system", Pools: registry, Image: "helper", Timeout: 10 * time.Millisecond}
				operator := &nodeexecutor.Client{Volumes: registry, Inspector: &helperpod.Inspector{Client: helper.Client, Namespace: helper.Namespace}, Timeout: helper.Timeout}
				current, err := r.Cleanups.Ensure(ctx, spec)
				if err != nil {
					t.Fatal(err)
				}
				// The real helper creates and binds its deterministic Job. Simulate
				// its later API receipt, followed by a controller process restart.
				if _, err := helper.Reclaim(ctx, current, r.Cleanups); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected live helper Job: %v", err)
				}
				current, err = r.Cleanups.Get(ctx, spec.Authority)
				if err != nil {
					t.Fatal(err)
				}
				next := current.Status
				executor := *next.Executor
				executor.PodUID = "cleanup-pod-uid"
				next.Executor = &executor
				if err := r.Cleanups.UpdateStatus(ctx, current, next); err != nil {
					t.Fatal(err)
				}
				current, err = r.Cleanups.Get(ctx, spec.Authority)
				if err != nil {
					t.Fatal(err)
				}
				next = current.Status
				next.Phase = cleanupapi.PhaseVerifying
				next.Receipt = &cleanupapi.Receipt{OperationID: spec.OperationID, ExecutorUID: executor.JobUID,
					ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true,
					LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
				if err := r.Cleanups.UpdateStatus(ctx, current, next); err != nil {
					t.Fatal(err)
				}
				job, err := kube.BatchV1().Jobs("system").Get(ctx, executor.JobName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				before := seedCleanupObservationPhase(t, r, spec, phase)
				moveBefore := repository.moves[0].Status
				reconcile := func() error {
					// Reconstruct the controller each time; only API journals survive.
					restarted := newTestReconciler(kube, repository, withCleanupsFrom(r))
					restarted.CleanupOperator = operator
					return restarted.reconcileMove(ctx, repository.moves[0])
				}
				assertHeld := func() {
					t.Helper()
					if err := reconcile(); !errors.Is(err, nodeexecutor.ErrLegacyExecution) {
						t.Fatalf("unsettled legacy journal was not reported: %v", err)
					}
					current, err := r.Cleanups.Get(ctx, spec.Authority)
					if err != nil || !reflect.DeepEqual(current, before) {
						t.Fatalf("pending executor changed its journal/fence: before=%+v after=%+v err=%v", before, current, err)
					}
					pool, err := registry.Client.Resource(volumeapi.PoolResource).Get(ctx, spec.Target.PoolName, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					epoch, _, _ := unstructured.NestedInt64(pool.Object, "spec", "scanEpoch")
					if epoch != cleanupObservationEpoch(phase) || !sameMoveAuthority(moveBefore, repository.moves[0].Status) ||
						!repository.moves[0].Status.CapacityApproved || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
						t.Fatalf("executor wait released hold/lock or requested scan: epoch=%d move=%+v error=%v", epoch, repository.moves[0], err)
					}
				}
				assertHeld()
				if settlement == "Job complete" {
					job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
					if _, err := kube.BatchV1().Jobs("system").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
				} else {
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-pod", Namespace: "system", UID: "cleanup-pod-uid",
						Labels: job.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}}}
					if _, err := kube.CoreV1().Pods("system").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
					if err := kube.BatchV1().Jobs("system").Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
					assertHeld()
					if err := kube.CoreV1().Pods("system").Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				// A finished Job is insufficient: only the compatible writer may
				// settle the old journal and its independent absence fence.
				assertHeld()
				settleLegacyCleanupFixture(t, r, kube, spec)
				settledHistory := listedJournal(t, r, move)
				if err := reconcile(); err != nil {
					t.Fatal(err)
				}
				current, err = r.Cleanups.Get(ctx, spec.Authority)
				if err != nil || current.Status.Phase != cleanupapi.PhaseCompleted || current.Status.AbsenceProof == nil || !reflect.DeepEqual(current, settledHistory) {
					t.Fatalf("candidate did not preserve independently settled history: %+v %v", current, err)
				}
				if repository.moves[0].Status.Phase != "Completing" || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
					t.Fatalf("cleanup observation skipped the separate completion boundary: move=%+v volume=%+v", repository.moves[0], repository.volumes[move.Spec.VolumeID])
				}
				if err := reconcile(); err != nil {
					t.Fatal(err)
				}
				if repository.moves[0].Status.Phase != "Succeeded" || repository.volumes[move.Spec.VolumeID].ActiveMove != "" {
					t.Fatalf("completion did not settle source authority: move=%+v volume=%+v", repository.moves[0], repository.volumes[move.Spec.VolumeID])
				}
			})
		}
	}
}

// Reproduce a historical fence opened before its Job exited. Only one absence
// step is taken: consuming this otherwise satisfiable proof must remain gated.
func seedCleanupObservationPhase(t *testing.T, r *Reconciler, spec cleanupapi.Spec, phase string) cleanupapi.Cleanup {
	t.Helper()
	current, err := r.Cleanups.Get(context.Background(), spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if phase == cleanupapi.PhaseConfirmingAbsence {
		current, _, err = r.Cleanups.ReconcileAbsence(context.Background(), current)
		if err != nil {
			t.Fatal(err)
		}
	}
	if current.Status.Phase != phase {
		t.Fatalf("cleanup fixture phase=%s want=%s", current.Status.Phase, phase)
	}
	return current
}

func cleanupObservationEpoch(phase string) int64 {
	if phase == cleanupapi.PhaseConfirmingAbsence {
		return 1
	}
	return 0
}

func prepareLegacyCleanupPool(t *testing.T, store *cleanupapi.Store, name string) {
	t.Helper()
	pool, err := store.Client.Resource(volumeapi.PoolResource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(pool.Object, "/mnt/pool", "spec", "mountPath")
	_ = unstructured.SetNestedField(pool.Object, "1Gi", "spec", "capacity", "limit")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(pool.Object, now, "status", "lastProbeTime")
	conditions := []any{}
	for _, kind := range []string{volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
		conditions = append(conditions, map[string]any{"type": kind, "status": "True", "reason": "ProbeSucceeded", "lastTransitionTime": now, "observedGeneration": pool.GetGeneration()})
	}
	_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
	// The fixture's ordinary Update reactor simulates a scan generation bump;
	// fixture initialization itself is not an absence scan request.
	if _, err := store.Client.Resource(volumeapi.PoolResource).UpdateStatus(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// Settle the old execution before the candidate is permitted to consume history.
func settleLegacyCleanupFixture(t *testing.T, r *Reconciler, kube *fake.Clientset, spec cleanupapi.Spec) {
	t.Helper()
	ctx := context.Background()
	current, err := r.Cleanups.Get(ctx, spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	helper := &helperpod.Runner{Client: kube, Namespace: "system", Pools: &volumeapi.Registry{Client: r.Cleanups.Client}, Image: "helper", Timeout: time.Second}
	if _, ready, err := helper.AdvanceReclaim(ctx, current, r.Cleanups); err != nil || !ready {
		t.Fatalf("compatible writer settlement: ready=%v err=%v", ready, err)
	}
	if err := r.driveCleanupAbsence(ctx, current); err != nil {
		t.Fatal(err)
	}
	current, err = r.Cleanups.Get(ctx, spec.Authority)
	if err != nil || cleanupapi.ValidateSettled(current) != nil {
		t.Fatalf("legacy writer did not close journal: %+v %v", current, err)
	}
}

// Diagnostics may report the compatibility blocker; execution authority may not change.
func sameMoveAuthority(before, after volumeapi.MoveStatus) bool {
	after.Reason = before.Reason
	after.Message = before.Message
	after.LastTransitionTime = before.LastTransitionTime
	after.LastProgressTime = before.LastProgressTime
	after.RecoveryReason = before.RecoveryReason
	after.RecoveryMessage = before.RecoveryMessage
	return reflect.DeepEqual(before, after)
}
