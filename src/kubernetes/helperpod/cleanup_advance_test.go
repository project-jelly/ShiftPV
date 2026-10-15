package helperpod

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// An older controller could request its post-receipt scan before the Job
// terminated. The existing fence is valid, but does not prove executor exit.
func TestCleanupRunnerConfirmingAbsenceStillWaitsForJobSettlement(t *testing.T) {
	for _, remaining := range []string{"Job", "owned Pod", "owned Pod without labels", "orphaned receipt Pod"} {
		t.Run(remaining, func(t *testing.T) {
			runner, store, current, job := receiptCleanupFixture(t, cleanupapi.PhaseConfirmingAbsence)
			client := runner.Client.(*fake.Clientset)
			if remaining != "Job" {
				if err := client.BatchV1().Jobs(runner.Namespace).Delete(context.Background(), job.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "remaining", Namespace: runner.Namespace, UID: "pod-uid",
					Labels:          map[string]string{cleanupNameLabel: current.Name, cleanupUIDLabel: current.UID},
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", UID: job.UID}}}}
				if remaining == "owned Pod without labels" || remaining == "orphaned receipt Pod" {
					pod.Labels = nil
				}
				if remaining == "orphaned receipt Pod" {
					pod.OwnerReferences = nil
				}
				if _, err := client.CoreV1().Pods(runner.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			client.ClearActions()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if _, err := runner.Reclaim(ctx, current, store); !errors.Is(err, context.DeadlineExceeded) || !isRetryable(err) {
				t.Fatalf("receipt and old fence bypassed running %s: %v", remaining, err)
			}
			after, err := store.Get(context.Background(), current.Spec.Authority)
			if err != nil || !reflect.DeepEqual(after, current) {
				t.Fatalf("observer changed journal/fence: %+v, err=%v", after, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("receipt reentry performed effect: %+v", action)
				}
			}
		})
	}
}

func TestCleanupAdvanceReusesBoundJobWithoutWaitingOrResettingDeadline(t *testing.T) {
	store, initial := moveCleanupBackendFixture(t)
	client := fake.NewClientset()
	runner := cleanupBackendRunner(client, initial)
	runner.Timeout = 7 * time.Minute
	creates := 0
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		creates++
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = "stable-job"
		return true, job, client.Tracker().Add(job)
	})
	client.PrependReactor("update", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		current, err := store.Get(context.Background(), initial.Spec.Authority)
		if err != nil || current.Status.Executor == nil || current.Status.Executor.JobUID != "stable-job" {
			t.Fatalf("Job started before durable binding: %+v %v", current, err)
		}
		return false, nil, nil
	})
	var bound cleanupapi.Cleanup
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		current, ready, err := runner.AdvanceReclaim(ctx, initial, store)
		cancel()
		if err != nil || ready || current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Executor == nil || current.Status.Executor.JobUID != "stable-job" {
			t.Fatalf("advance %d waited or lost pending journal: %+v ready=%v err=%v", i, current, ready, err)
		}
		if i > 0 && !reflect.DeepEqual(current, bound) {
			t.Fatalf("pending advance changed binding: %+v -> %+v", bound, current)
		}
		bound = current
	}
	job, err := client.BatchV1().Jobs(runner.Namespace).Get(context.Background(), initial.Name+"-effect", metav1.GetOptions{})
	if err != nil || creates != 1 || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 420 || job.Spec.Suspend == nil || *job.Spec.Suspend {
		t.Fatalf("reconcile reset Job or execution timeout: job=%+v creates=%d err=%v", job, creates, err)
	}
}

func TestCleanupAdvanceReceiptObservationPreservesFence(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		t.Run(phase, func(t *testing.T) {
			runner, store, current, job := receiptCleanupFixture(t, phase)
			client := runner.Client.(*fake.Clientset)
			client.ClearActions()
			ctx := context.Background()
			observed, ready, err := runner.AdvanceReclaim(ctx, current, store)
			if err != nil || ready || !reflect.DeepEqual(observed, current) {
				t.Fatalf("running receipt should remain pending: %+v ready=%v err=%v", observed, ready, err)
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if err := client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, runner.Namespace); err != nil {
				t.Fatal(err)
			}
			observed, ready, err = runner.AdvanceReclaim(ctx, current, store)
			if err != nil || !ready || !reflect.DeepEqual(observed, current) {
				t.Fatalf("settlement changed receipt/fence: %+v ready=%v err=%v", observed, ready, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("receipt observer mutated executor: %+v", action)
				}
			}
		})
	}
}

func TestCleanupAdvanceConfirmingContradictionPreservesFence(t *testing.T) {
	for _, change := range []string{"Job UID", "Job failed"} {
		t.Run(change, func(t *testing.T) {
			runner, store, current, job := receiptCleanupFixture(t, cleanupapi.PhaseConfirmingAbsence)
			if change == "Job UID" {
				job.UID = "wrong-uid"
			} else {
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			}
			client := runner.Client.(*fake.Clientset)
			if err := client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, runner.Namespace); err != nil {
				t.Fatal(err)
			}
			if _, ready, err := runner.AdvanceReclaim(context.Background(), current, store); err == nil || ready {
				t.Fatalf("contradictory executor accepted: ready=%v err=%v", ready, err)
			}
			after, err := store.Get(context.Background(), current.Spec.Authority)
			if err != nil || after.Status.Phase != cleanupapi.PhaseNeedsReview ||
				!reflect.DeepEqual(after.Status.AbsenceProof, current.Status.AbsenceProof) ||
				!reflect.DeepEqual(after.Status.Receipt, current.Status.Receipt) ||
				!reflect.DeepEqual(after.Status.Executor, current.Status.Executor) {
				t.Fatalf("contradiction did not preserve proof in NeedsReview: %+v err=%v", after, err)
			}
		})
	}
}

func TestCleanupAdvanceResponseLossReusesAcceptedJobAndBinding(t *testing.T) {
	for _, boundary := range []string{"create", "binding"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			store, initial := moveCleanupBackendFixture(t)
			client := fake.NewClientset()
			runner := cleanupBackendRunner(client, initial)
			creates := 0
			client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				creates++
				job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				job.UID = "accepted-job"
				if err := client.Tracker().Add(job); err != nil {
					return true, nil, err
				}
				if boundary == "create" {
					return true, nil, apierrors.NewTimeoutError("accepted Job response lost", 1)
				}
				return true, job, nil
			})
			journal := cleanupBackendJournal{CleanupJournal: store}
			if boundary == "binding" {
				journal.write = func(ctx context.Context, expected cleanupapi.Cleanup, next cleanupapi.Status) error {
					if err := store.UpdateStatus(ctx, expected, next); err != nil {
						return err
					}
					return apierrors.NewTimeoutError("accepted binding response lost", 1)
				}
			}
			_, ready, err := runner.AdvanceReclaim(ctx, initial, journal)
			if ready || boundary == "binding" && !apierrors.IsTimeout(err) || boundary == "create" && err != nil {
				t.Fatalf("response loss ready=%v err=%v", ready, err)
			}
			current, ready, err := runner.AdvanceReclaim(ctx, initial, store)
			if err != nil || ready || creates != 1 || current.Status.Executor == nil || current.Status.Executor.JobUID != "accepted-job" {
				t.Fatalf("accepted effect not reused: %+v ready=%v creates=%d err=%v", current, ready, creates, err)
			}
		})
	}
}

func TestCleanupReceiptObservationAllowsRunningJobPodReplacement(t *testing.T) {
	ctx := context.Background()
	store, initial := cleanupFixture(t)
	client := fake.NewClientset()
	runner := cleanupBackendRunner(client, initial)
	job := runner.cleanupJob(initial, "/mnt/shiftpv")
	job.UID, job.Spec.Suspend = "stable-job", boolPtr(false)
	if err := client.Tracker().Add(job); err != nil {
		t.Fatal(err)
	}
	status := cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &cleanupapi.Executor{
		JobName: job.Name, JobUID: string(job.UID), NodeName: initial.Spec.Target.NodeName, PodUID: "old-pod"}}
	if err := store.UpdateStatus(ctx, initial, status); err != nil {
		t.Fatal(err)
	}
	old, err := store.Get(ctx, initial.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	status.Executor = &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), NodeName: initial.Spec.Target.NodeName, PodUID: "new-pod"}
	if err := store.UpdateStatus(ctx, old, status); err != nil {
		t.Fatal(err)
	}
	current, ready, err := runner.observeReceipt(ctx, store, old, job)
	if err != nil || ready || current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Executor.PodUID != "new-pod" {
		t.Fatalf("same Job's legal Pod retry was rejected: %+v ready=%v err=%v", current, ready, err)
	}
}

func TestCleanupAdvancePreservesObservationErrorClassification(t *testing.T) {
	for _, injected := range []error{context.DeadlineExceeded, context.Canceled, apierrors.NewTimeoutError("API read timeout", 1)} {
		t.Run(injected.Error(), func(t *testing.T) {
			runner, store, current, _ := receiptCleanupFixture(t, cleanupapi.PhaseConfirmingAbsence)
			client := runner.Client.(*fake.Clientset)
			client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, injected
			})
			_, ready, err := runner.AdvanceReclaim(context.Background(), current, store)
			if ready || !errors.Is(err, injected) || isRetryable(err) != !errors.Is(injected, context.Canceled) {
				t.Fatalf("observation error lost classification: ready=%v err=%v", ready, err)
			}
			after, getErr := store.Get(context.Background(), current.Spec.Authority)
			if getErr != nil || !reflect.DeepEqual(after, current) {
				t.Fatalf("incomplete observation mutated journal: %+v err=%v", after, getErr)
			}
		})
	}
}

func receiptCleanupFixture(t *testing.T, phase string) (Runner, *cleanupapi.Store, cleanupapi.Cleanup, *batchv1.Job) {
	t.Helper()
	ctx := context.Background()
	store, cleanup := cleanupFixture(t)
	client := fake.NewClientset()
	runner := cleanupBackendRunner(client, cleanup)
	job := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	job.UID, job.Spec.Suspend = "job-uid", boolPtr(false)
	if err := client.Tracker().Add(job); err != nil {
		t.Fatal(err)
	}
	executor := &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	status := cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}
	if err := store.UpdateStatus(ctx, cleanup, status); err != nil {
		t.Fatal(err)
	}
	status.Phase = cleanupapi.PhaseVerifying
	status.Receipt = &cleanupapi.Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest}
	if err := store.UpdateStatus(ctx, cleanup, status); err != nil {
		t.Fatal(err)
	}
	if phase == cleanupapi.PhaseConfirmingAbsence {
		status.Phase = phase
		status.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "existing-fence", PoolName: cleanup.Spec.Target.PoolName,
			PoolUID: cleanup.Spec.Target.PoolUID, RequiredGeneration: 2}
		if err := store.UpdateStatus(ctx, cleanup, status); err != nil {
			t.Fatal(err)
		}
	}
	current, err := store.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return runner, store, current, job
}
