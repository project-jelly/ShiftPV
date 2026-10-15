package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
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
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// Older releases could leave a receipt-bearing cleanup on a terminal Move
// after releasing ActiveMove. Repair may observe its old Job; it cannot start
// an executor or release protection while that executor is still present.
func TestTerminalMoveJobReceiptWaitsForExecutorSettlement(t *testing.T) {
	for _, cleanupPhase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		for _, phase := range []string{"Succeeded", "Blocked"} {
			for _, settlement := range []string{"Job complete", "Job absent but Pod remains"} {
				t.Run(cleanupPhase+"/"+phase+"/"+settlement, func(t *testing.T) {
					ctx := context.Background()
					r, repo, _, move, incoming := settledTerminalMoveFixture(t, phase, recoveryRecovered, recoveryCapacitySettled)
					if phase == "Succeeded" {
						move.Status.RecoveryPhase, move.Status.CapacityReason = "", ""
						repo.moves[0] = move
					}
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
					job := terminalVerifyingJob(t, r, kube, spec)
					before := seedCleanupObservationPhase(t, r, spec, cleanupPhase)
					state := repo.volumes[move.Spec.VolumeID]
					if state.ActiveMove != "" {
						t.Fatal("fixture has not released ActiveMove")
					}
					stripMoveProtection(t, repo, move)
					reconcile := func() error {
						moves, err := repo.ListMoves(ctx)
						if err != nil {
							return err
						}
						restarted := newTestReconciler(kube, repo, withCleanupsFrom(r))
						return restarted.reconcilePersistedMove(ctx, moves[0])
					}
					// The first pass only restores the old journal's protection.
					if err := reconcile(); err != nil {
						t.Fatal(err)
					}
					kube.ClearActions()
					assertHeld := func() {
						t.Helper()
						if err := reconcile(); err != nil {
							t.Fatalf("terminal repair did not return normal pending: %v", err)
						}
						current, err := r.Cleanups.Get(ctx, spec.Authority)
						if err != nil || !reflect.DeepEqual(current.Status, before.Status) {
							t.Fatalf("terminal pending changed receipt/fence: before=%+v after=%+v err=%v", before.Status, current.Status, err)
						}
						pool, err := r.Cleanups.Client.Resource(volumeapi.PoolResource).Get(ctx, spec.Target.PoolName, metav1.GetOptions{})
						if err != nil {
							t.Fatal(err)
						}
						epoch, _, _ := unstructured.NestedInt64(pool.Object, "spec", "scanEpoch")
						if epoch != cleanupObservationEpoch(cleanupPhase) || !slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) || repo.moves[0].Status.CapacityApproved || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
							t.Fatalf("terminal wait changed authority, capacity or protection: epoch=%d move=%+v", epoch, repo.moves[0])
						}
						for _, action := range kube.Actions() {
							if action.GetVerb() != "get" && action.GetVerb() != "list" {
								t.Fatalf("terminal wait started/mutated executor: %+v", action)
							}
						}
					}
					assertHeld()
					if settlement == "Job complete" {
						job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
						if _, err := kube.BatchV1().Jobs("system").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
							t.Fatal(err)
						}
					} else {
						pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminal-cleanup-pod", Namespace: "system", UID: "cleanup-pod-uid", Labels: job.Spec.Template.Labels,
							OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}}}
						if _, err := kube.CoreV1().Pods("system").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
							t.Fatal(err)
						}
						if err := kube.BatchV1().Jobs("system").Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
							t.Fatal(err)
						}
						kube.ClearActions()
						assertHeld()
						if err := kube.CoreV1().Pods("system").Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					kube.ClearActions()
					if err := reconcile(); err != nil {
						t.Fatal(err)
					}
					current, err := r.Cleanups.Get(ctx, spec.Authority)
					if err != nil || current.Status.Phase != cleanupapi.PhaseCompleted || current.Status.AbsenceProof == nil {
						t.Fatalf("terminal cleanup did not settle: %+v %v", current, err)
					}
					if err := reconcile(); err != nil {
						t.Fatal(err)
					}
					if slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) || repo.moves[0].Status.CapacityApproved || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
						t.Fatal("terminal settlement changed released authority or retained protection")
					}
					for _, action := range kube.Actions() {
						if action.GetVerb() != "get" && action.GetVerb() != "list" {
							t.Fatalf("terminal observation started/mutated executor: %+v", action)
						}
					}
				})
			}
		}
	}
}

func TestTerminalJobObservationDistinguishesLostPoolFromTransientRead(t *testing.T) {
	for _, missing := range []bool{true, false} {
		name := "transient Pool read"
		if missing {
			name = "Pool deleted"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, repo, _, move, incoming := settledTerminalMoveFixture(t, "Blocked", recoveryRecovered, recoveryCapacitySettled)
			move.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
			repo.moves[0] = move
			spec := rollbackCleanupSpec(move, incoming)
			kube := r.Client.(*fake.Clientset)
			assignJobUIDs(kube)
			terminalVerifyingJob(t, r, kube, spec)
			before, err := r.Cleanups.Get(ctx, spec.Authority)
			if err != nil {
				t.Fatal(err)
			}
			readFailure := errors.New("temporary Pool API read failure")
			if missing {
				if err := r.Cleanups.Client.Resource(volumeapi.PoolResource).Delete(ctx, spec.Target.PoolName, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				api := r.Cleanups.Client.(*dynamicfake.FakeDynamicClient)
				api.PrependReactor("get", "shiftpvpools", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, readFailure
				})
			}
			// The real observer preserves its current journal on either error.
			observed, _, err := r.CleanupOperator.AdvanceReclaim(ctx, before, r.Cleanups)
			if err == nil || !reflect.DeepEqual(observed, before) || missing && !errors.Is(err, volumeapi.ErrPoolNotFound) || !missing && !errors.Is(err, readFailure) {
				t.Fatalf("unexpected observer error/result: %+v %v", observed, err)
			}
			// An observer is still allowed to return zero with an error. Ensure
			// classification uses the caller's original exact journal in that case.
			r.CleanupOperator = zeroCleanupOnError{r.CleanupOperator.AdvanceReclaim}
			kube.ClearActions()
			if err := r.settleTerminalCleanup(ctx, move); err == nil {
				t.Fatal("observation error was hidden")
			}
			after, err := r.Cleanups.Get(ctx, spec.Authority)
			if err != nil {
				t.Fatal(err)
			}
			if missing {
				if after.Status.Phase != cleanupapi.PhaseNeedsReview || after.Status.Reason != "CleanupPoolIdentityChanged" ||
					after.Spec != before.Spec || !reflect.DeepEqual(after.Status.Receipt, before.Status.Receipt) ||
					!reflect.DeepEqual(after.Status.Executor, before.Status.Executor) || after.Status.AbsenceProof != nil {
					t.Fatalf("lost Pool did not retain the exact journal in review: %+v", after)
				}
				moves, err := repo.ListMoves(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.reconcilePersistedMove(ctx, moves[0]); err != nil {
					t.Fatal(err)
				}
				if slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) {
					t.Fatal("terminal review retained the finalizer forever")
				}
			} else if !reflect.DeepEqual(after, before) || !slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) {
				t.Fatalf("transient read changed the journal or protection: %+v", after)
			}
			for _, action := range kube.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("failed observation mutated executor: %+v", action)
				}
			}
		})
	}
}

type zeroCleanupOnError struct {
	observe func(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error)
}

func (o zeroCleanupOnError) AdvanceReclaim(ctx context.Context, expected cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error) {
	current, ready, err := o.observe(ctx, expected, store)
	if err != nil {
		return cleanupapi.Cleanup{}, false, err
	}
	return current, ready, nil
}

func terminalVerifyingJob(t *testing.T, r *Reconciler, kube *fake.Clientset, spec cleanupapi.Spec) *batchv1.Job {
	return verifyingCleanupJob(t, r, kube, spec, 10*time.Millisecond)
}

func verifyingCleanupJob(t *testing.T, r *Reconciler, kube *fake.Clientset, spec cleanupapi.Spec, timeout time.Duration) *batchv1.Job {
	t.Helper()
	ctx := context.Background()
	prepareLegacyCleanupPool(t, r.Cleanups, spec.Target.PoolName)
	registry := &volumeapi.Registry{Client: r.Cleanups.Client}
	helper := &helperpod.Runner{Client: kube, Namespace: "system", Pools: registry, Image: "helper", Timeout: timeout}
	r.CleanupOperator = &nodeexecutor.Client{Volumes: registry, Fallback: helper, Timeout: helper.Timeout}
	current, err := r.Cleanups.Ensure(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ready, err := helper.AdvanceReclaim(ctx, current, r.Cleanups); err != nil || ready {
		t.Fatalf("live helper setup: ready=%t err=%v", ready, err)
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
	next.Receipt = &cleanupapi.Receipt{OperationID: spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	if err := r.Cleanups.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	job, err := kube.BatchV1().Jobs("system").Get(ctx, executor.JobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return job
}
