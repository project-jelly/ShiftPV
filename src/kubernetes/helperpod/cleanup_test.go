package helperpod

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const testCleanupReceiptDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCleanupRunnerBindsExactJobAndWaitsForReceipt(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	client := fake.NewClientset()
	var created *batchv1.Job
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.SetUID(types.UID("job-uid"))
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		created = job.DeepCopy()
		if err := client.Tracker().Add(job); err != nil {
			return true, nil, err
		}
		return true, job, nil
	})
	writeReceiptOnFirstJobGet(client, cleanups, cleanup)
	runner := validRunner(client)
	runner.ServiceAccountName = "shiftpv-controller"
	runner.PoolReadinessStaleAfter = 7 * time.Minute
	runner.Pools = fakePoolResolver{
		pool:    readyCleanupPool(volumeapi.Pool{Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID, NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv"}),
		nodeErr: fmt.Errorf("%w: duplicate node registration", volumeapi.ErrPoolConfiguration),
	}
	result, err := runner.Reclaim(ctx, cleanup, cleanups)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Phase != cleanupapi.PhaseVerifying || result.Status.Receipt == nil || created == nil {
		t.Fatalf("result=%#v job=%#v", result, created)
	}
	assertCleanupJobIdentity(t, created, cleanup, "shiftpv-controller")
	started, err := client.BatchV1().Jobs(runner.Namespace).Get(ctx, created.Name, metav1.GetOptions{})
	if err != nil || started.Spec.Suspend == nil || *started.Spec.Suspend {
		t.Fatalf("bound cleanup Job was not started: suspend=%v err=%v", started.Spec.Suspend, err)
	}
	if second, err := runner.Reclaim(ctx, result, cleanups); err != nil || second.Status.Phase != cleanupapi.PhaseVerifying {
		t.Fatalf("retry=%#v err=%v", second, err)
	}
}

// writeReceiptOnFirstJobGet makes the first read of the executor Job observe a
// cleanup whose Pod has already bound itself and written its API receipt.
func writeReceiptOnFirstJobGet(client *fake.Clientset, cleanups *cleanupapi.Store, cleanup cleanupapi.Cleanup) {
	ctx := context.Background()
	receiptWritten := false
	client.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if receiptWritten {
			return false, nil, nil
		}
		current, err := cleanups.Get(ctx, cleanup.Spec.Authority)
		if err != nil {
			return true, nil, err
		}
		bound := *current.Status.Executor
		bound.PodUID = "pod-uid"
		if err := cleanups.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &bound}); err != nil {
			return true, nil, err
		}
		current, err = cleanups.Get(ctx, cleanup.Spec.Authority)
		if err != nil {
			return true, nil, err
		}
		receipt := &cleanupapi.Receipt{
			OperationID: current.Spec.OperationID, ExecutorUID: current.Status.Executor.JobUID,
			ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
		}
		if err := cleanups.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: current.Status.Executor, Receipt: receipt}); err != nil {
			return true, nil, err
		}
		receiptWritten = true
		return false, nil, nil
	})
}

// assertCleanupJobIdentity pins the exact executor the approved effect names:
// its node, service account, command, retry budget, authority arguments, and
// the single owner reference that ties it to the cleanup parent.
func assertCleanupJobIdentity(t *testing.T, created *batchv1.Job, cleanup cleanupapi.Cleanup, serviceAccount string) {
	t.Helper()
	container := created.Spec.Template.Spec.Containers[0]
	arguments := strings.Join(container.Args, " ")
	if created.Name != cleanup.Name+"-effect" || created.Spec.Template.Spec.NodeName != cleanup.Spec.Target.NodeName ||
		created.Spec.Template.Spec.ServiceAccountName != serviceAccount || container.Command[0] != "/shiftpv-volume-helper" ||
		created.Spec.BackoffLimit == nil || *created.Spec.BackoffLimit < 1 || created.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		!strings.Contains(arguments, "--authority-kind=ShiftPVVolume") ||
		!strings.Contains(arguments, "--authority-name="+cleanup.Spec.Authority.Name) ||
		!strings.Contains(arguments, "--authority-uid="+cleanup.Spec.Authority.UID) ||
		len(created.OwnerReferences) != 1 || created.OwnerReferences[0].UID != types.UID(cleanup.Spec.Authority.UID) {
		t.Fatalf("cleanup Job identity=%#v", created)
	}
}

// TestCleanupJobForwardsPoolReadinessBudget pins that the cleanup executor Job
// carries the controller's configured probe staleness budget. The cleanup
// helper re-proves destination publication against a Ready Pool before it
// purges the source copy, so a helper on the package default could accept a
// probe this Runner already treats as stale.
func TestCleanupJobForwardsPoolReadinessBudget(t *testing.T) {
	_, cleanup := cleanupFixture(t)
	runner := validRunner(fake.NewClientset())
	runner.PoolReadinessStaleAfter = 7 * time.Minute
	args := runner.cleanupJob(cleanup, "/mnt/shiftpv").Spec.Template.Spec.Containers[0].Args
	want := volumeapi.PoolReadinessStaleAfterArgument(7 * time.Minute)
	if want != "--pool-readiness-stale-after=7m0s" {
		t.Fatalf("forwarded argument = %q", want)
	}
	if !slices.Contains(args, want) {
		t.Fatalf("cleanup helper does not receive the Runner budget: %v", args)
	}
	unset := validRunner(fake.NewClientset())
	defaulted := unset.cleanupJob(cleanup, "/mnt/shiftpv").Spec.Template.Spec.Containers[0].Args
	if !slices.Contains(defaulted, volumeapi.PoolReadinessStaleAfterArgument(volumeapi.DefaultPoolReadinessStaleAfter)) {
		t.Fatalf("unset budget did not resolve to the default: %v", defaulted)
	}
}

func TestCleanupRunnerRejectsChangedPoolAndJob(t *testing.T) {
	cleanups, cleanup := cleanupFixture(t)
	runner := validRunner(fake.NewClientset())
	runner.Pools = fakePoolResolver{pool: volumeapi.Pool{Name: "replacement", UID: cleanup.Spec.Target.PoolUID, MountPath: "/mnt/shiftpv"}}
	if _, err := runner.Reclaim(context.Background(), cleanup, cleanups); err == nil || !strings.Contains(err.Error(), "PoolIdentityChanged") {
		t.Fatalf("replacement Pool accepted: %v", err)
	}
	if current, err := cleanups.Get(context.Background(), cleanup.Spec.Authority); err != nil || current.Status.Phase != cleanupapi.PhaseNeedsReview || current.Status.Reason != "PoolIdentityChanged" {
		t.Fatalf("Pool mismatch did not converge to review: %#v err=%v", current, err)
	}

	cleanups, cleanup = cleanupFixture(t)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID, MountPath: "/mnt/shiftpv"})}
	wanted := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	wanted.UID = "foreign-uid"
	wanted.Spec.Template.Spec.NodeName = "other-node"
	runner.Client = fake.NewClientset(wanted)
	if _, err := runner.Reclaim(context.Background(), cleanup, cleanups); err == nil || !strings.Contains(err.Error(), "JobIdentityChanged") {
		t.Fatalf("changed Job accepted: %v", err)
	}
}

func TestCleanupRunnerRejectsPoolWithoutLifecycleProtection(t *testing.T) {
	cleanups, cleanup := cleanupFixture(t)
	client := fake.NewClientset()
	pool := readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})
	pool.Finalizers = nil
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: pool}
	if _, err := runner.Reclaim(context.Background(), cleanup, cleanups); err == nil || !strings.Contains(err.Error(), "PoolProtectionChanged") {
		t.Fatalf("unprotected Pool was accepted: %v", err)
	}
	current, err := cleanups.Get(context.Background(), cleanup.Spec.Authority)
	if err != nil || current.Status.Phase != cleanupapi.PhaseNeedsReview || current.Status.Reason != "PoolProtectionChanged" {
		t.Fatalf("unprotected Pool did not converge to review: cleanup=%#v err=%v", current, err)
	}
	jobs, err := client.BatchV1().Jobs(runner.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("unprotected Pool started cleanup Jobs: jobs=%#v err=%v", jobs.Items, err)
	}
}

func TestCleanupRunnerRejectsExecutorStartedBeforeUIDBinding(t *testing.T) {
	cleanups, cleanup := cleanupFixture(t)
	runner := validRunner(fake.NewClientset())
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	job := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	job.UID = "job-uid"
	job.Spec.Suspend = boolPtr(false)
	runner.Client = fake.NewClientset(job)
	if _, err := runner.Reclaim(context.Background(), cleanup, cleanups); err == nil || !strings.Contains(err.Error(), "ExecutorStartedBeforeBinding") {
		t.Fatalf("unbound executor was accepted: %v", err)
	}
	current, err := cleanups.Get(context.Background(), cleanup.Spec.Authority)
	if err != nil || current.Status.Phase != cleanupapi.PhaseNeedsReview || current.Status.Reason != "ExecutorStartedBeforeBinding" {
		t.Fatalf("unbound executor did not converge to review: %#v err=%v", current, err)
	}
}

func TestCleanupRunnerDefersEffectWhilePoolIsNotReady(t *testing.T) {
	cleanups, cleanup := cleanupFixture(t)
	client := fake.NewClientset()
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv", Finalizers: []string{volumeapi.PoolProtectionFinalizer},
	}}
	_, err := runner.Reclaim(context.Background(), cleanup, cleanups)
	if err == nil || !isRetryable(err) {
		t.Fatalf("unready Pool was not deferred retryably: %v", err)
	}
	current, getErr := cleanups.Get(context.Background(), cleanup.Spec.Authority)
	jobs, listErr := client.BatchV1().Jobs(runner.Namespace).List(context.Background(), metav1.ListOptions{})
	if getErr != nil || listErr != nil || current.Status.Phase != cleanupapi.PhasePending || len(jobs.Items) != 0 {
		t.Fatalf("unready Pool changed cleanup or started effect: cleanup=%#v jobs=%#v getErr=%v listErr=%v", current, jobs.Items, getErr, listErr)
	}
}

func TestCleanupRunnerJoinsStartedExecutorFromStalePendingSnapshot(t *testing.T) {
	ctx := context.Background()
	cleanups, stale := cleanupFixture(t)
	runner := validRunner(fake.NewClientset())
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: stale.Spec.Target.PoolName, UID: stale.Spec.Target.PoolUID,
		NodeName: stale.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	job := runner.cleanupJob(stale, "/mnt/shiftpv")
	job.UID = "job-uid"
	job.Spec.Suspend = boolPtr(false)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	client := fake.NewClientset(job)
	runner.Client = client
	executor := &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), PodUID: "pod-uid", NodeName: stale.Spec.Target.NodeName}
	if err := cleanups.UpdateStatus(ctx, stale, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	receiptWritten := false
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !receiptWritten {
			receipt := &cleanupapi.Receipt{
				OperationID: stale.Spec.OperationID, ExecutorUID: executor.JobUID,
				ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
			}
			if err := cleanups.UpdateStatus(ctx, stale, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
				return true, nil, err
			}
			receiptWritten = true
		}
		return false, nil, nil
	})
	result, err := runner.Reclaim(ctx, stale, cleanups)
	if err != nil || result.Status.Phase != cleanupapi.PhaseVerifying {
		t.Fatalf("stale reconciler did not join bound executor: result=%#v err=%v", result, err)
	}
}

func TestCleanupRunnerWaitsForJobTerminationAfterReceipt(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	runner := validRunner(nil)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	job := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	job.UID = "job-uid"
	job.Spec.Suspend = boolPtr(false)
	client := fake.NewClientset(job)
	runner.Client = client
	executor := &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	runner.Timeout = 20 * time.Millisecond
	if _, err := runner.Reclaim(ctx, cleanup, cleanups); !isRetryable(err) {
		t.Fatalf("receipt without executor termination was accepted: %v", err)
	}
	current, err := client.BatchV1().Jobs(runner.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs(runner.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	runner.Timeout = time.Second
	result, err := runner.Reclaim(ctx, cleanup, cleanups)
	if err != nil || result.Status.Phase != cleanupapi.PhaseVerifying {
		t.Fatalf("terminated executor did not settle receipt: result=%#v err=%v", result, err)
	}
}

func TestCleanupRunnerAcceptsVerifyingReceiptAfterTTLRemovedExecutor(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	executor := &cleanupapi.Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset()
	createCalls := 0
	client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		createCalls++
		return false, nil, nil
	})
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	result, err := runner.Reclaim(ctx, cleanup, cleanups)
	if err != nil || result.Status.Phase != cleanupapi.PhaseVerifying || createCalls != 0 {
		t.Fatalf("TTL-settled executor did not converge: result=%#v createCalls=%d err=%v", result, createCalls, err)
	}
}

func TestCleanupRunnerWaitsForTTLOwnedPodAfterJobDisappears(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	executor := &cleanupapi.Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "terminating-helper", Namespace: "shiftpv-system",
		Labels:          map[string]string{cleanupNameLabel: cleanup.Name, cleanupUIDLabel: cleanup.UID},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", UID: "job-uid"}},
	}}
	client := fake.NewClientset(pod)
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	runner.Timeout = 20 * time.Millisecond
	if _, err := runner.Reclaim(ctx, cleanup, cleanups); !isRetryable(err) {
		t.Fatalf("live TTL-owned Pod was treated as settled: %v", err)
	}
	if err := client.CoreV1().Pods(runner.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	runner.Timeout = time.Second
	result, err := runner.Reclaim(ctx, cleanup, cleanups)
	if err != nil || result.Status.Phase != cleanupapi.PhaseVerifying {
		t.Fatalf("removed TTL-owned Pod did not settle: result=%#v err=%v", result, err)
	}
}

func TestCleanupJournalRejectsIncompleteVerifyingReceipt(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	executor := &cleanupapi.Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, LocalReceiptDigest: testCleanupReceiptDigest,
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	if err := cleanups.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err == nil {
		t.Fatal("incomplete receipt entered Verifying")
	}
	current, err := cleanups.Get(ctx, cleanup.Spec.Authority)
	if err != nil || current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Receipt != nil {
		t.Fatalf("incomplete receipt changed journal: current=%#v err=%v", current, err)
	}
}

func TestOwnedByJobRequiresExactControllerUID(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{
		{APIVersion: "batch/v1", Kind: "Job", UID: "job-uid"},
	}}}
	if !ownedByJob(pod, "job-uid") {
		t.Fatal("exact Job owner was not recognized")
	}
	if ownedByJob(pod, "replacement-job-uid") {
		t.Fatal("replacement Job owner was accepted")
	}
	pod.OwnerReferences[0].Kind = "Pod"
	if ownedByJob(pod, "job-uid") {
		t.Fatal("non-Job owner was accepted")
	}
}

func TestCleanupRunnerReacquiresJobAfterAcceptedCreateResponseLoss(t *testing.T) {
	ctx := context.Background()
	cleanups, cleanup := cleanupFixture(t)
	client := fake.NewClientset()
	createCalls := 0
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = "job-uid"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		if err := client.Tracker().Add(job); err != nil {
			return true, nil, err
		}
		createCalls++
		return true, nil, apierrors.NewTimeoutError("accepted cleanup Job, response lost", 1)
	})
	receiptWritten := false
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		current, err := cleanups.Get(ctx, cleanup.Spec.Authority)
		if err == nil && current.Status.Phase == cleanupapi.PhaseRunning && !receiptWritten {
			bound := *current.Status.Executor
			bound.PodUID = "pod-uid"
			if err := cleanups.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &bound}); err != nil {
				return true, nil, err
			}
			current, err = cleanups.Get(ctx, cleanup.Spec.Authority)
			if err != nil {
				return true, nil, err
			}
			receipt := &cleanupapi.Receipt{
				OperationID: current.Spec.OperationID, ExecutorUID: current.Status.Executor.JobUID,
				ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testCleanupReceiptDigest,
			}
			if err := cleanups.UpdateStatus(ctx, current, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: current.Status.Executor, Receipt: receipt}); err != nil {
				return true, nil, err
			}
			receiptWritten = true
		}
		return false, nil, nil
	})
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{
		Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv",
	})}
	result, err := runner.Reclaim(ctx, cleanup, cleanups)
	if err != nil || result.Status.Phase != cleanupapi.PhaseVerifying || createCalls != 1 {
		t.Fatalf("accepted cleanup Job was not reacquired: result=%#v creates=%d err=%v", result, createCalls, err)
	}
}

func TestSameCleanupJobAcceptsAPIDefaultsButRejectsEffectChanges(t *testing.T) {
	cleanups, cleanup := cleanupFixture(t)
	_ = cleanups
	runner := &Runner{Namespace: "shiftpv-system", ServiceAccountName: "shiftpv-controller", Image: "helper:test", Timeout: 500 * time.Millisecond}
	expected := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	if expected.Spec.Suspend == nil || !*expected.Spec.Suspend {
		t.Fatal("cleanup Job must be suspended until its UID is durable")
	}
	current := expected.DeepCopy()
	fieldRef := current.Spec.Template.Spec.Containers[0].Env[0].ValueFrom.FieldRef
	if fieldRef == nil || fieldRef.APIVersion != "v1" || fieldRef.FieldPath != "metadata.name" {
		t.Fatalf("cleanup executor identity env=%#v", fieldRef)
	}
	current.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	current.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
	terminationGrace := int64(30)
	current.Spec.Template.Spec.TerminationGracePeriodSeconds = &terminationGrace
	if !sameCleanupJob(current, expected) {
		t.Fatal("Kubernetes API default fields changed cleanup identity")
	}
	current.Spec.Template.Spec.Containers[0].Args = append(current.Spec.Template.Spec.Containers[0].Args, "--pool-root=/other")
	if sameCleanupJob(current, expected) {
		t.Fatal("changed cleanup effect was accepted")
	}
	parallel := int32(2)
	current = expected.DeepCopy()
	current.Spec.Parallelism = &parallel
	if sameCleanupJob(current, expected) {
		t.Fatal("parallel cleanup effect was accepted")
	}
	short := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	if short.Spec.ActiveDeadlineSeconds == nil || *short.Spec.ActiveDeadlineSeconds != 1 {
		t.Fatalf("sub-second timeout deadline = %v", short.Spec.ActiveDeadlineSeconds)
	}
}

// TestCleanupJobDifferenceLocalizesEveryComparedField flips one compared field
// at a time. Every row of the comparison table must reject its own change and
// report itself, and no row may reject a Job that is still the approved effect.
func TestCleanupJobDifferenceLocalizesEveryComparedField(t *testing.T) {
	_, cleanup := cleanupFixture(t)
	runner := &Runner{Namespace: "shiftpv-system", ServiceAccountName: "shiftpv-controller", Image: "helper:test", Timeout: 500 * time.Millisecond}
	expected := runner.cleanupJob(cleanup, "/mnt/shiftpv")
	if field := cleanupJobDifference(expected.DeepCopy(), expected); field != "" {
		t.Fatalf("approved cleanup effect rejected at %q", field)
	}
	if sameCleanupJob(nil, expected) || sameCleanupJob(expected, nil) {
		t.Fatal("missing cleanup Job accepted")
	}
	deleted := metav1.Now()
	two, zero, deadline := int32(2), int32(0), int64(9999)
	indexed := batchv1.IndexedCompletion
	foreign := "example.com/cleanup-controller"
	for _, testCase := range []struct {
		field  string
		change func(*batchv1.Job)
	}{
		{"metadata.labels[" + cleanupUIDLabel + "]", func(job *batchv1.Job) { job.Labels[cleanupUIDLabel] = "other-uid" }},
		{"metadata.labels[" + cleanupNameLabel + "]", func(job *batchv1.Job) { job.Labels[cleanupNameLabel] = "other-name" }},
		{"metadata.deletionTimestamp", func(job *batchv1.Job) { job.DeletionTimestamp = &deleted }},
		{"metadata.namespace", func(job *batchv1.Job) { job.Namespace = "other-namespace" }},
		{"metadata.name", func(job *batchv1.Job) { job.Name = "other-effect" }},
		{"metadata.ownerReferences", func(job *batchv1.Job) { job.OwnerReferences = nil }},
		{"spec.parallelism", func(job *batchv1.Job) { job.Spec.Parallelism = &two }},
		{"spec.completions", func(job *batchv1.Job) { job.Spec.Completions = &two }},
		{"spec.manualSelector", func(job *batchv1.Job) { job.Spec.ManualSelector = boolPtr(true) }},
		{"spec.completionMode", func(job *batchv1.Job) { job.Spec.CompletionMode = &indexed }},
		{"spec.podFailurePolicy", func(job *batchv1.Job) { job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{} }},
		{"spec.successPolicy", func(job *batchv1.Job) { job.Spec.SuccessPolicy = &batchv1.SuccessPolicy{} }},
		{"spec.backoffLimitPerIndex", func(job *batchv1.Job) { job.Spec.BackoffLimitPerIndex = &zero }},
		{"spec.maxFailedIndexes", func(job *batchv1.Job) { job.Spec.MaxFailedIndexes = &zero }},
		{"spec.managedBy", func(job *batchv1.Job) { job.Spec.ManagedBy = &foreign }},
		{"spec.template.labels[" + cleanupUIDLabel + "]", func(job *batchv1.Job) {
			job.Spec.Template.Labels[cleanupUIDLabel] = "other-uid"
		}},
		{"spec.template.labels[" + cleanupNameLabel + "]", func(job *batchv1.Job) {
			job.Spec.Template.Labels[cleanupNameLabel] = "other-name"
		}},
		{"spec.template.spec.nodeName", func(job *batchv1.Job) { job.Spec.Template.Spec.NodeName = "other-node" }},
		{"spec.template.spec.serviceAccountName", func(job *batchv1.Job) {
			job.Spec.Template.Spec.ServiceAccountName = "other-account"
		}},
		{"spec.template.spec.restartPolicy", func(job *batchv1.Job) {
			job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		}},
		{"spec.template.spec.hostPID", func(job *batchv1.Job) { job.Spec.Template.Spec.HostPID = true }},
		{"spec.template.spec.hostIPC", func(job *batchv1.Job) { job.Spec.Template.Spec.HostIPC = true }},
		{"spec.template.spec.hostNetwork", func(job *batchv1.Job) { job.Spec.Template.Spec.HostNetwork = true }},
		{"spec.template.spec.initContainers", func(job *batchv1.Job) {
			job.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "init"}}
		}},
		{"spec.template.spec.ephemeralContainers", func(job *batchv1.Job) {
			job.Spec.Template.Spec.EphemeralContainers = []corev1.EphemeralContainer{{}}
		}},
		{"spec.template.spec.containers", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers = append(job.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar"})
		}},
		{"spec.template.spec.volumes", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{Name: "extra"})
		}},
		{"spec.template.spec.containers[0].name", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Name = "other"
		}},
		{"spec.template.spec.containers[0].image", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Image = "other:tag"
		}},
		{"spec.template.spec.containers[0].command", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh"}
		}},
		{"spec.template.spec.containers[0].args", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Args = append(job.Spec.Template.Spec.Containers[0].Args, "--pool-root=/other")
		}},
		{"spec.template.spec.containers[0].env", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Env = nil
		}},
		{"spec.template.spec.containers[0].envFrom", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{Prefix: "SHIFTPV_"}}
		}},
		{"spec.template.spec.containers[0].resources", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
		}},
		{"spec.template.spec.containers[0].securityContext", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = boolPtr(true)
		}},
		{"spec.template.spec.containers[0].volumeMounts", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = "/other"
		}},
		{"spec.template.spec.containers[0].volumeDevices", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "pool", DevicePath: "/dev/pool"}}
		}},
		{"spec.template.spec.containers[0].lifecycle", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{}
		}},
		{"spec.template.spec.containers[0].livenessProbe", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{}
		}},
		{"spec.template.spec.containers[0].readinessProbe", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{}
		}},
		{"spec.template.spec.containers[0].startupProbe", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Containers[0].StartupProbe = &corev1.Probe{}
		}},
		{"spec.template.spec.volumes[0]", func(job *batchv1.Job) {
			job.Spec.Template.Spec.Volumes[0].HostPath.Path = "/other"
		}},
		{"spec.backoffLimit", func(job *batchv1.Job) { job.Spec.BackoffLimit = &zero }},
		{"spec.activeDeadlineSeconds", func(job *batchv1.Job) { job.Spec.ActiveDeadlineSeconds = &deadline }},
	} {
		t.Run(strings.ReplaceAll(testCase.field, "/", "_"), func(t *testing.T) {
			current := expected.DeepCopy()
			testCase.change(current)
			if field := cleanupJobDifference(current, expected); field != testCase.field {
				t.Fatalf("difference = %q, want %q", field, testCase.field)
			}
			if sameCleanupJob(current, expected) {
				t.Fatal("changed cleanup effect accepted")
			}
		})
	}
}

func TestCleanupRunnerReportsFailedAndReceiptlessJobs(t *testing.T) {
	for name, condition := range map[string]batchv1.JobConditionType{
		"failed": batchv1.JobFailed, "receiptless": batchv1.JobComplete,
	} {
		t.Run(name, func(t *testing.T) {
			cleanups, cleanup := cleanupFixture(t)
			client := fake.NewClientset()
			client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				job.SetUID("job-uid")
				job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, Message: name}}
				_ = client.Tracker().Add(job)
				return true, job, nil
			})
			runner := validRunner(client)
			runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID, MountPath: "/mnt/shiftpv"})}
			if _, err := runner.Reclaim(context.Background(), cleanup, cleanups); err == nil {
				t.Fatal("terminal Job without receipt accepted")
			}
			current, err := cleanups.Get(context.Background(), cleanup.Spec.Authority)
			if err != nil || current.Status.Phase != cleanupapi.PhaseNeedsReview {
				t.Fatalf("terminal Job did not converge to review: %#v err=%v", current, err)
			}
			wantReason := "ExecutorFailed"
			if condition == batchv1.JobComplete {
				wantReason = "ReceiptMissing"
			}
			if current.Status.Reason != wantReason {
				t.Fatalf("review reason=%q want=%q", current.Status.Reason, wantReason)
			}
		})
	}
}

func cleanupFixture(t *testing.T) (*cleanupapi.Store, cleanupapi.Cleanup) {
	t.Helper()
	target := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: testVolumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	parent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{
			"name": target.VolumeID, "uid": target.VolumeUID, "resourceVersion": "1", "generation": int64(1),
			"finalizers": []any{volumeapi.VolumeProtectionFinalizer},
		},
		"spec": map[string]any{},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList",
		volumeapi.MoveResource:   "ShiftPVMoveList",
		volumeapi.PoolResource:   "ShiftPVPoolList",
	}, parent)
	store := &cleanupapi.Store{Client: dynamicClient}
	cleanup, err := store.Ensure(context.Background(), cleanupapi.Spec{
		OperationID: "delete-volume-uid", Target: target, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: target.VolumeID, UID: target.VolumeUID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, cleanup
}

func readyCleanupPool(pool volumeapi.Pool) volumeapi.Pool {
	now := metav1.Now()
	if pool.NodeName == "" {
		pool.NodeName = "worker-a"
	}
	pool.PoolGroup = volumeapi.DefaultPoolGroup
	pool.CapacityLimit = "1Gi"
	pool.Generation = 1
	pool.Finalizers = []string{volumeapi.PoolProtectionFinalizer}
	pool.Status = volumeapi.PoolStatus{
		ObservedGeneration: 1,
		LastProbeTime:      now,
		Conditions: []metav1.Condition{{
			Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, LastTransitionTime: now, Reason: "PoolReady",
		}},
	}
	return pool
}
