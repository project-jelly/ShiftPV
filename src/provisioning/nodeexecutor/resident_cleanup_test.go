package nodeexecutor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func cleanupClientFixture(t *testing.T, reason string) moveClientFixture {
	t.Helper()
	if reason != "VolumeDelete" {
		return newMoveClientFixture(t, reason)
	}
	client, node, dynamic, kube, state := fixture(t)
	ctx := context.Background()
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	state, err := client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy,
		Reason: reason, Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
	if err != nil {
		t.Fatal(err)
	}
	return moveClientFixture{client: client, node: node, dynamic: dynamic, kube: kube, store: node.Cleanups, cleanup: cleanup}
}

func assertOnlyCompatibilityReads(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("compatibility observation mutated API: %#v", action)
		}
	}
}

func TestResidentCleanupCapabilityRecoveryPreservesExactIntent(t *testing.T) {
	for _, reason := range []string{"VolumeDelete", "MoveSource", "MoveRollback"} {
		for _, unavailable := range []string{"missing capability", "unknown capability", "not ready", "missing Pod"} {
			t.Run(reason+"/"+unavailable, func(t *testing.T) {
				f := cleanupClientFixture(t, reason)
				ctx := context.Background()
				original, err := f.kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod := original.DeepCopy()
				capability := volumeapi.NodeEffectsAnnotation
				if reason != "VolumeDelete" {
					capability = volumeapi.NodeMoveCleanupAnnotation
				}
				switch unavailable {
				case "missing capability":
					delete(pod.Annotations, capability)
				case "unknown capability":
					pod.Annotations[capability] = "v999"
				case "not ready":
					pod.Status.Conditions = nil
				case "missing Pod":
					if err := f.kube.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name); err != nil {
						t.Fatal(err)
					}
				}
				if unavailable != "missing Pod" {
					if err := f.kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
						t.Fatal(err)
					}
				}
				before := f.current(t)
				volumeBefore, err := f.client.Volumes.Get(ctx, testID)
				if err != nil {
					t.Fatal(err)
				}
				f.kube.ClearActions()
				f.dynamic.ClearActions()
				for _, advance := range []bool{false, true} {
					var current cleanupapi.Cleanup
					var err error
					if advance {
						var ready bool
						current, ready, err = f.client.AdvanceReclaim(ctx, before, f.store)
						if ready {
							t.Fatal("missing capability completed cleanup")
						}
					} else {
						current, err = f.client.Reclaim(ctx, before, f.store)
					}
					var retryable interface{ Retryable() bool }
					if err == nil || !errors.As(err, &retryable) || !retryable.Retryable() || !reflect.DeepEqual(current, before) {
						t.Fatalf("unavailable executor changed intent or cannot retry: %+v %v", current, err)
					}
				}
				volumeAfter, err := f.client.Volumes.Get(ctx, testID)
				if err != nil || !reflect.DeepEqual(volumeBefore, volumeAfter) || !reflect.DeepEqual(before, f.current(t)) {
					t.Fatalf("blocked cleanup changed Volume authority or journal: %+v %v", volumeAfter, err)
				}
				assertOnlyCompatibilityReads(t, f.kube.Actions())
				assertOnlyCompatibilityReads(t, f.dynamic.Actions())
				if unavailable == "missing Pod" {
					err = f.kube.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), original, original.Namespace)
				} else {
					err = f.kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), original, original.Namespace)
				}
				if err != nil {
					t.Fatal(err)
				}
				current, ready, err := f.client.AdvanceReclaim(ctx, before, f.store)
				if err != nil || ready || current.Spec != before.Spec || current.UID != before.UID || current.Status.Phase != cleanupapi.PhaseRunning ||
					current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode || current.Status.Executor.PodUID != f.node.Identity.PodUID || current.Status.Receipt != nil {
					t.Fatalf("capability recovery did not bind original intent: %+v ready=%v err=%v", current, ready, err)
				}
				f.assertNoJobs(t)
			})
		}
	}
}

// Construct only previous-writer input through the real journal transition
// contract. Reclaim must never produce any of these historical writes itself.
func (f moveClientFixture) completedLegacyJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job := f.boundLegacyJob(t)
	ctx := context.Background()
	current := f.current(t)
	if err := f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
		t.Fatal(err)
	}
	current = f.current(t)
	next := current.Status
	next.Phase = cleanupapi.PhaseConfirmingAbsence
	next.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "historical-scan", PoolName: current.Spec.Target.PoolName, PoolUID: current.Spec.Target.PoolUID, RequiredGeneration: 2}
	if err := f.store.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	current = f.current(t)
	next = current.Status
	next.Phase = cleanupapi.PhaseCompleted
	proof := *next.AbsenceProof
	proof.ObservedGeneration, proof.Valid, proof.Complete, proof.Absent = 3, true, true, true
	proof.ConfirmedAt = time.Now().UTC().Format(time.RFC3339Nano)
	next.AbsenceProof, next.SettledAt = &proof, proof.ConfirmedAt
	if err := f.store.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	if err := cleanupapi.ValidateSettled(f.current(t)); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := f.kube.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestResidentCleanupCompletedLegacyHistoryRequiresQuiescence(t *testing.T) {
	for _, state := range []string{"complete", "active Job", "live exact Pod", "live orphan receipt Pod", "replacement Job UID", "Job absent", "Job absent with terminal Pod", "invalid absence proof"} {
		t.Run(state, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			job := f.completedLegacyJob(t)
			ctx := context.Background()
			blocked, conflict := false, false
			switch state {
			case "active Job":
				job.Status.Active, blocked = 1, true
			case "replacement Job UID":
				job.UID, conflict = "replacement-job-uid", true
			case "live exact Pod", "live orphan receipt Pod", "Job absent with terminal Pod":
				blocked = true
				controller := true
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shiftpv", Name: "old-helper", UID: "helper-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}},
					Spec:   corev1.PodSpec{NodeName: f.cleanup.Spec.Target.NodeName, Containers: []corev1.Container{{Name: "helper", Image: "old:original"}}},
					Status: corev1.PodStatus{Phase: corev1.PodRunning}}
				if state == "live orphan receipt Pod" {
					pod.OwnerReferences = nil
				}
				if state == "Job absent with terminal Pod" {
					pod.Status.Phase = corev1.PodSucceeded
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "helper", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
				}
				if err := f.kube.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
					t.Fatal(err)
				}
			case "invalid absence proof":
				parent, err := f.dynamic.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				_ = unstructured.SetNestedField(parent.Object, false, "status", "cleanup", "status", "absenceProof", "complete")
				if err := f.dynamic.Tracker().Update(volumeapi.MoveResource, parent, ""); err != nil {
					t.Fatal(err)
				}
				conflict = true
			}
			if err := f.kube.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace); err != nil {
				t.Fatal(err)
			}
			if state == "Job absent" || state == "Job absent with terminal Pod" {
				if err := f.kube.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), job.Namespace, job.Name); err != nil {
					t.Fatal(err)
				}
			}
			before := f.current(t)
			f.kube.ClearActions()
			f.dynamic.ClearActions()
			for _, advance := range []bool{false, true} {
				var current cleanupapi.Cleanup
				var err error
				if advance {
					var ready bool
					current, ready, err = f.client.AdvanceReclaim(ctx, before, f.store)
					if ready != (!blocked && !conflict) {
						t.Fatalf("history readiness=%v err=%v", ready, err)
					}
				} else {
					current, err = f.client.Reclaim(ctx, before, f.store)
				}
				if errors.Is(err, ErrLegacyExecution) != blocked || errors.Is(err, cleanupapi.ErrConflict) != conflict || err != nil && !blocked && !conflict || !reflect.DeepEqual(before, current) {
					t.Fatalf("invalid legacy history outcome: %+v blocked=%v conflict=%v err=%v", current, blocked, conflict, err)
				}
			}
			ready, err := f.client.ObserveCompletedCleanup(ctx, before)
			if ready != (!blocked && !conflict) || errors.Is(err, ErrLegacyExecution) != blocked || errors.Is(err, cleanupapi.ErrConflict) != conflict || err != nil && !blocked && !conflict {
				t.Fatalf("readonly completion port bypassed history guard: ready=%v blocked=%v conflict=%v err=%v", ready, blocked, conflict, err)
			}
			if !reflect.DeepEqual(before, f.current(t)) {
				t.Fatal("historical journal changed")
			}
			assertOnlyCompatibilityReads(t, f.kube.Actions())
			assertOnlyCompatibilityReads(t, f.dynamic.Actions())
			for _, action := range f.kube.Actions() {
				if action.GetResource().Resource == "daemonsets" {
					t.Fatal("historical completion selected a new executor")
				}
			}
		})
	}
}

func TestObserveCompletedCleanupDoesNotRequireProtectedParentOrRegistry(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "Job history", true: "Node history"}[native], func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			f.completedLegacyJob(t)
			current := f.current(t)
			ctx := context.Background()
			parent, err := f.dynamic.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			parent.SetFinalizers(nil)
			if err := f.dynamic.Tracker().Update(volumeapi.MoveResource, parent, ""); err != nil {
				t.Fatal(err)
			}
			observer := &Client{Inspector: f.client.Inspector}
			if native {
				observer = &Client{}
				current.Status.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: "shiftpv", PodName: "retired-node", PodUID: "retired-pod-uid", NodeName: current.Spec.Target.NodeName}
				receipt := *current.Status.Receipt
				receipt.ExecutorUID = current.Status.Executor.PodUID
				current.Status.Receipt = &receipt
			}
			f.dynamic.ClearActions()
			f.kube.ClearActions()
			if ready, err := observer.ObserveCompletedCleanup(ctx, current); err != nil || !ready {
				t.Fatalf("settled history required a writer or protected parent: ready=%v err=%v", ready, err)
			}
			if len(f.dynamic.Actions()) != 0 || native && len(f.kube.Actions()) != 0 {
				t.Fatalf("history observer accessed a parent or Node runtime: dynamic=%+v typed=%+v", f.dynamic.Actions(), f.kube.Actions())
			}
			assertOnlyCompatibilityReads(t, f.kube.Actions())
			stored, err := f.dynamic.Tracker().Get(volumeapi.MoveResource, "", "move-a")
			if err != nil || !reflect.DeepEqual(stored, parent) {
				t.Fatalf("history observation changed unprotected parent: %+v %v", stored, err)
			}
			current.Status.AbsenceProof.Complete = false
			if ready, err := observer.ObserveCompletedCleanup(ctx, current); ready || !errors.Is(err, cleanupapi.ErrConflict) {
				t.Fatalf("damaged completion proof accepted: ready=%v err=%v", ready, err)
			}
		})
	}
}

func TestObserveCompletedCleanupPreservesObservationFailureCause(t *testing.T) {
	for _, test := range []struct {
		name      string
		cause     error
		retryable bool
	}{
		{"API timeout", apierrors.NewTimeoutError("Job observation unavailable", 1), true},
		{"deadline", context.DeadlineExceeded, true},
		{"canceled", context.Canceled, false},
		{"forbidden", apierrors.NewForbidden(batchv1.Resource("jobs"), "old-helper", errors.New("denied")), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			f.completedLegacyJob(t)
			current := f.current(t)
			f.kube.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, test.cause })
			f.kube.ClearActions()
			f.dynamic.ClearActions()
			ready, err := (&Client{Inspector: f.client.Inspector}).ObserveCompletedCleanup(context.Background(), current)
			var retryable interface{ Retryable() bool }
			isRetryable := errors.As(err, &retryable) && retryable.Retryable()
			if ready || !errors.Is(err, test.cause) || isRetryable != test.retryable || len(f.dynamic.Actions()) != 0 {
				t.Fatalf("observation failure granted completion or lost cause: ready=%v retry=%v err=%v", ready, isRetryable, err)
			}
			assertOnlyCompatibilityReads(t, f.kube.Actions())
		})
	}
}
