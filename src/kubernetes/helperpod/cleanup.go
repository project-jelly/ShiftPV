package helperpod

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

const (
	cleanupNameLabel = "shiftpv.io/cleanup-name"
	cleanupUIDLabel  = "shiftpv.io/cleanup-uid"
)

// Reclaim preserves the synchronous CSI contract while sharing binding and
// observation with the mobility controller's nonblocking advance operation.
func (r *Runner) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal) (cleanupapi.Cleanup, error) {
	defer r.observeStep("reclaim_helper")()
	current, job, done, err := r.prepareCleanup(ctx, cleanup, store)
	if err != nil || done {
		return current, err
	}
	if cleanupHasReceipt(current) {
		return r.resumeVerifying(ctx, current, store, job)
	}
	return r.awaitReceipt(ctx, store, current, job)
}

// AdvanceReclaim starts or observes the exact bound Job once, without waiting
// for it. A receipt is ready only after that Job and its owned Pods have settled.
func (r *Runner) AdvanceReclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal) (result cleanupapi.Cleanup, ready bool, resultErr error) {
	defer r.observeStep("reclaim_helper")()
	defer func() {
		if errors.Is(resultErr, context.DeadlineExceeded) {
			resultErr = retryableError{err: resultErr}
		}
	}()
	current, job, done, err := r.prepareCleanup(ctx, cleanup, store)
	if err != nil || done {
		return current, done && err == nil, err
	}
	if cleanupHasReceipt(current) {
		return r.observeReceiptSettlement(ctx, current, store, job)
	}
	return r.observeReceipt(ctx, store, current, job)
}

// prepareCleanup re-reads the journal before any effect. Receipt-bearing
// records, including historical absence fences, take the observe-only path.
func (r *Runner) prepareCleanup(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal) (cleanupapi.Cleanup, *batchv1.Job, bool, error) {
	if r == nil || r.Client == nil || r.Pools == nil || r.Namespace == "" || r.Image == "" || r.Timeout <= 0 || store == nil || cleanup.UID == "" || cleanup.Name == "" || cleanup.Spec.Validate() != nil {
		return cleanup, nil, false, fmt.Errorf("cleanup runner configuration is incomplete")
	}
	if done, result, err := triageCleanup(cleanup); done {
		return result, nil, done, err
	}
	pool, err := r.approvedPool(ctx, store, cleanup)
	if err != nil {
		return cleanup, nil, false, err
	}
	current, err := refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return current, nil, false, err
	}
	if done, result, err := triageCleanup(current); done {
		return result, nil, done, err
	}
	job := r.cleanupJob(current, pool.MountPath)
	if cleanupHasReceipt(current) {
		return current, job, false, nil
	}
	if err := r.cleanupPoolReady(pool); err != nil {
		return current, nil, false, err
	}
	current, err = r.startCleanupExecutor(ctx, current, store, job)
	return current, job, current.Status.Phase == cleanupapi.PhaseCompleted, err
}

// startCleanupExecutor binds exactly one suspended Job before starting it.
// Running journals reuse their Job without submitting another create request.
func (r *Runner) startCleanupExecutor(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal, job *batchv1.Job) (cleanupapi.Cleanup, error) {
	created, err := r.resolveCleanupJob(ctx, store, cleanup, job)
	if err != nil {
		return cleanup, err
	}
	cleanup, err = refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return cleanup, err
	}
	if done, result, err := triageCleanup(cleanup); done {
		return result, err
	}
	if cleanupHasReceipt(cleanup) {
		return cleanup, nil
	}
	cleanup, err = r.bindExecutor(ctx, store, cleanup, created)
	if err != nil {
		return cleanup, err
	}
	if err := r.startCleanupJob(ctx, store, cleanup, created, job); err != nil {
		return cleanup, err
	}
	return cleanup, nil
}

func (r *Runner) resolveCleanupJob(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, expected *batchv1.Job) (*batchv1.Job, error) {
	if cleanup.Status.Phase != cleanupapi.PhaseRunning {
		return r.ensureJob(ctx, store, cleanup, expected)
	}
	job, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, r.needsReview(ctx, store, cleanup, "ExecutorDisappeared", "bound cleanup Job disappeared before execution")
	}
	if err != nil {
		return nil, classifyKubernetesAPIError(err)
	}
	if cleanup.Status.Executor == nil || string(job.UID) != cleanup.Status.Executor.JobUID || !sameCleanupJob(job, expected) {
		return nil, r.needsReview(ctx, store, cleanup, "ExecutorIdentityChanged", "bound cleanup Job differs from the approved effect")
	}
	return job, nil
}

func cleanupHasReceipt(current cleanupapi.Cleanup) bool {
	return current.Status.Phase == cleanupapi.PhaseVerifying || current.Status.Phase == cleanupapi.PhaseConfirmingAbsence
}

// triageCleanup reports whether the durable phase already decides this
// reconcile: a settled cleanup is returned as it stands, and one already under
// review is an error no executor work may follow. A resident Node binding also
// ends helper work without changing the winning backend's journal.
func triageCleanup(current cleanupapi.Cleanup) (bool, cleanupapi.Cleanup, error) {
	switch current.Status.Phase {
	case cleanupapi.PhaseCompleted:
		return true, current, nil
	case cleanupapi.PhaseNeedsReview:
		return true, cleanupapi.Cleanup{}, fmt.Errorf("cleanup %q needs review: %s", current.Name, current.Status.Reason)
	}
	if current.Status.Executor != nil && current.Status.Executor.Kind == cleanupapi.ExecutorNode {
		return true, current, fmt.Errorf("%w: cleanup is bound to a resident Node", cleanupapi.ErrConflict)
	}
	return false, current, nil
}

// approvedPool resolves the registered Pool the approved cleanup targets. Pool
// identity drift and lost lifecycle protection are contradictions, not transient
// failures, so they send the cleanup to review instead of retrying.
func (r *Runner) approvedPool(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup) (volumeapi.Pool, error) {
	pool, err := r.Pools.PoolForIdentity(ctx, cleanup.Spec.Target.PoolName, cleanup.Spec.Target.PoolUID, cleanup.Spec.Target.NodeName)
	if err != nil {
		if errors.Is(err, volumeapi.ErrStateConflict) {
			return volumeapi.Pool{}, r.needsReview(ctx, store, cleanup, "PoolIdentityChanged", "registered Pool no longer matches the approved cleanup target")
		}
		return volumeapi.Pool{}, classifyKubernetesAPIError(err)
	}
	if pool.Name != cleanup.Spec.Target.PoolName || pool.UID != cleanup.Spec.Target.PoolUID {
		return volumeapi.Pool{}, r.needsReview(ctx, store, cleanup, "PoolIdentityChanged", "registered Pool no longer matches the approved cleanup target")
	}
	if !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) {
		return volumeapi.Pool{}, r.needsReview(ctx, store, cleanup, "PoolProtectionChanged", "cleanup Pool no longer has lifecycle protection")
	}
	return pool, nil
}

// cleanupPoolReady defers an effect that has not started yet while its Pool is
// not currently usable. The staleness budget follows the same rule
// volumeapi.Registry.readiness() applies to placement decisions and
// volumeapi.PoolReadinessStaleAfterArgument applies to the executor argument: a
// non-positive value means DefaultPoolReadinessStaleAfter.
func (r *Runner) cleanupPoolReady(pool volumeapi.Pool) error {
	staleAfter := r.PoolReadinessStaleAfter
	if staleAfter <= 0 {
		staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
	}
	if ready, reason := pool.CleanupReadyAt(time.Now(), staleAfter); !ready {
		return retryableError{err: fmt.Errorf("cleanup Pool %q is not ready: %s", pool.Name, reason)}
	}
	return nil
}

// ensureJob resolves exactly one suspended executor for the approved effect. An
// accepted create whose response was lost is reacquired by name, and a Job under
// that name that is not the approved effect sends the cleanup to review.
func (r *Runner) ensureJob(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, job *batchv1.Job) (*batchv1.Job, error) {
	jobs := r.Client.BatchV1().Jobs(r.Namespace)
	created, err := jobs.Create(ctx, job, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) || isAmbiguousKubernetesError(err) {
		createErr := err
		created, err = jobs.Get(ctx, job.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && isAmbiguousKubernetesError(createErr) {
			return nil, fmt.Errorf("ensure cleanup Job: %w", classifyKubernetesAPIError(createErr))
		}
		if err == nil && !sameCleanupJob(created, job) {
			return nil, r.needsReview(ctx, store, cleanup, "JobIdentityChanged", fmt.Sprintf("cleanup Job %q differs from the approved effect", job.Name))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("ensure cleanup Job: %w", classifyKubernetesAPIError(err))
	}
	if created.UID == "" {
		return nil, r.needsReview(ctx, store, cleanup, "ExecutorIdentityMissing", "cleanup Job has no UID")
	}
	return created, nil
}

// bindExecutor makes the resolved Job UID durable before it may run. A Job that
// is already running while the journal has not bound it, or a bound executor
// that is not this one, is an identity contradiction.
func (r *Runner) bindExecutor(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, created *batchv1.Job) (cleanupapi.Cleanup, error) {
	if (cleanup.Status.Phase == "" || cleanup.Status.Phase == cleanupapi.PhasePending) &&
		(created.Spec.Suspend == nil || !*created.Spec.Suspend) {
		return cleanupapi.Cleanup{}, r.needsReview(ctx, store, cleanup, "ExecutorStartedBeforeBinding", "cleanup Job started before its UID was bound to durable status")
	}
	executor := &cleanupapi.Executor{JobName: created.Name, JobUID: string(created.UID), NodeName: cleanup.Spec.Target.NodeName}
	if cleanup.Status.Phase == "" || cleanup.Status.Phase == cleanupapi.PhasePending {
		if err := store.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
			return cleanupapi.Cleanup{}, fmt.Errorf("bind cleanup executor: %w", err)
		}
		cleanup.Status = cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}
	} else if cleanup.Status.Executor == nil || !sameBoundExecutor(cleanup.Status.Executor, executor) {
		return cleanupapi.Cleanup{}, r.needsReview(ctx, store, cleanup, "ExecutorIdentityChanged", "cleanup executor differs from the durable cleanup status")
	}
	return cleanup, nil
}

// awaitReceipt watches the bound executor until the journal carries its receipt.
// The journal decides settlement; the Job is read only to detect an executor
// that disappeared, changed or terminated without writing one.
func (r *Runner) awaitReceipt(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, expected *batchv1.Job) (cleanupapi.Cleanup, error) {
	result := cleanup
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		current, ready, err := r.observeReceipt(pollCtx, store, cleanup, expected)
		result = current
		return ready, err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return result, fmt.Errorf("wait for cleanup receipt: %w", err)
	}
	return result, nil
}

func (r *Runner) observeReceipt(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, expected *batchv1.Job) (cleanupapi.Cleanup, bool, error) {
	current, err := refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return current, false, err
	}
	if done, result, err := triageCleanup(current); done {
		return result, err == nil, err
	}
	if current.Status.Executor == nil || cleanup.Status.Executor == nil ||
		current.Status.Executor.JobName != cleanup.Status.Executor.JobName || current.Status.Executor.JobUID != cleanup.Status.Executor.JobUID {
		return current, false, r.needsReview(ctx, store, current, "ExecutorIdentityChanged", "cleanup executor differs from the durable cleanup status")
	}
	currentJob, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if cleanupHasReceipt(current) {
			return r.observeReceiptSettlement(ctx, current, store, expected)
		}
		return current, false, r.needsReview(ctx, store, current, "ExecutorDisappeared", "cleanup Job disappeared before termination was acknowledged")
	}
	if err != nil {
		return current, false, classifyKubernetesAPIError(err)
	}
	if string(currentJob.UID) != current.Status.Executor.JobUID || !sameCleanupJob(currentJob, expected) {
		return current, false, r.needsReview(ctx, store, current, "ExecutorIdentityChanged", "cleanup Job changed before termination was acknowledged")
	}
	complete, failure, failed := cleanupJobOutcome(currentJob)
	if failed {
		return current, false, r.needsReview(ctx, store, current, "ExecutorFailed", fmt.Sprintf("cleanup Job failed: %s", failure))
	}
	if !complete {
		return current, false, nil
	}
	if !cleanupHasReceipt(current) || !boundReceipt(current) {
		return current, false, r.needsReview(ctx, store, current, "ReceiptMissing", "cleanup Job completed without a durable receipt")
	}
	return current, true, nil
}

// cleanupJobOutcome reads one Job's terminal conditions. A true Failed condition
// anywhere in the list wins over a true Complete one, because a Job that failed
// after completing a Pod has not delivered the approved effect.
func cleanupJobOutcome(job *batchv1.Job) (complete bool, failure string, failed bool) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return false, condition.Message, true
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			complete = true
		}
	}
	return complete, "", false
}

func (r *Runner) resumeVerifying(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal, expected *batchv1.Job) (cleanupapi.Cleanup, error) {
	result := cleanup
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		current, ready, err := r.observeReceiptSettlement(pollCtx, cleanup, store, expected)
		result = current
		return ready, err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return result, fmt.Errorf("wait for cleanup executor settlement: %w", err)
	}
	return result, nil
}

func (r *Runner) observeReceiptSettlement(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal, expected *batchv1.Job) (cleanupapi.Cleanup, bool, error) {
	if !boundReceipt(cleanup) {
		return cleanup, false, r.needsReview(ctx, store, cleanup, "ReceiptInvalid", "cleanup receipt does not match the durable executor and intent")
	}
	ready, current, err := r.observeSettlement(ctx, cleanup, store, expected)
	return current, ready, err
}

// boundReceipt reports whether the durable receipt belongs to this intent's own
// executor and already records a completed purge.
func boundReceipt(cleanup cleanupapi.Cleanup) bool {
	return cleanup.Status.Executor != nil && cleanup.Status.Receipt != nil &&
		cleanup.Status.Receipt.OperationID == cleanup.Spec.OperationID &&
		cleanup.Status.Receipt.ExecutorUID == cleanup.Status.Executor.JobUID &&
		cleanup.Status.Receipt.Retired && cleanup.Status.Receipt.Purged
}

// observeSettlement performs one poll of a cleanup that already holds its API
// receipt, reporting settlement together with the journal to return.
func (r *Runner) observeSettlement(ctx context.Context, cleanup cleanupapi.Cleanup, store cleanupapi.Journal, expected *batchv1.Job) (bool, cleanupapi.Cleanup, error) {
	current, err := refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return false, cleanupapi.Cleanup{}, err
	}
	if current.Status.Phase == cleanupapi.PhaseCompleted {
		return true, current, nil
	}
	if !cleanupHasReceipt(current) || current.Status.Executor == nil || current.Status.Receipt == nil ||
		!reflect.DeepEqual(current.Status.Executor, cleanup.Status.Executor) || !reflect.DeepEqual(current.Status.Receipt, cleanup.Status.Receipt) {
		return false, cleanupapi.Cleanup{}, cleanupapi.ErrConflict
	}
	job, getErr := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, current.Status.Executor.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return r.settleWithoutExecutor(ctx, current)
	}
	if getErr != nil {
		return false, cleanupapi.Cleanup{}, classifyKubernetesAPIError(getErr)
	}
	if string(job.UID) != current.Status.Executor.JobUID || !sameCleanupJob(job, expected) {
		return false, cleanupapi.Cleanup{}, r.needsReview(ctx, store, current, "ExecutorIdentityChanged", "cleanup executor differs from the durable cleanup status")
	}
	// First condition wins here, unlike cleanupJobOutcome: a receipt already
	// exists, so a Complete recorded before a later Failed still resumes.
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return false, cleanupapi.Cleanup{}, r.needsReview(ctx, store, current, "ExecutorFailed", fmt.Sprintf("cleanup Job failed after writing its receipt: %s", condition.Message))
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true, current, nil
		}
	}
	return false, current, nil
}

// settleWithoutExecutor settles a cleanup whose Job is already gone, unless one
// of that Job's Pods or its receipt-writing Pod remains. Labels and owner refs
// can be removed, so neither is sufficient evidence of Pod absence.
func (r *Runner) settleWithoutExecutor(ctx context.Context, current cleanupapi.Cleanup) (bool, cleanupapi.Cleanup, error) {
	pods, listErr := r.Client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{})
	if listErr != nil {
		return false, cleanupapi.Cleanup{}, classifyKubernetesAPIError(listErr)
	}
	for index := range pods.Items {
		if string(pods.Items[index].UID) == current.Status.Executor.PodUID || ownedByJob(&pods.Items[index], types.UID(current.Status.Executor.JobUID)) {
			return false, current, nil
		}
	}
	return true, current, nil
}

func ownedByJob(pod *corev1.Pod, uid types.UID) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && owner.UID == uid {
			return true
		}
	}
	return false
}

func refreshCleanup(ctx context.Context, store cleanupapi.Journal, expected cleanupapi.Cleanup) (cleanupapi.Cleanup, error) {
	current, err := store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, fmt.Errorf("read cleanup intent: %w", classifyKubernetesAPIError(err))
	}
	if current.UID != expected.UID || current.Spec != expected.Spec {
		return cleanupapi.Cleanup{}, cleanupapi.ErrConflict
	}
	return current, nil
}

func (r *Runner) startCleanupJob(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, created, expected *batchv1.Job) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, created.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return r.needsReview(ctx, store, cleanup, "ExecutorDisappeared", "bound cleanup Job disappeared before execution")
		}
		if err != nil {
			return fmt.Errorf("read bound cleanup Job: %w", classifyKubernetesAPIError(err))
		}
		if current.UID != created.UID || !sameCleanupJob(current, expected) {
			return r.needsReview(ctx, store, cleanup, "ExecutorIdentityChanged", "bound cleanup Job differs from the approved effect")
		}
		if current.Spec.Suspend == nil || !*current.Spec.Suspend {
			return nil
		}
		current.Spec.Suspend = boolPtr(false)
		if _, err := r.Client.BatchV1().Jobs(r.Namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("start bound cleanup Job: %w", classifyKubernetesAPIError(err))
		}
		return nil
	})
}

func (r *Runner) needsReview(ctx context.Context, store cleanupapi.Journal, cleanup cleanupapi.Cleanup, reason, message string) error {
	next := cleanup.Status
	next.Phase, next.Reason, next.Message = cleanupapi.PhaseNeedsReview, reason, message
	next.LastTransitionTime = ""
	if updateErr := store.UpdateStatus(ctx, cleanup, next); updateErr != nil {
		return fmt.Errorf("mark cleanup for review after %s: %w", reason, updateErr)
	}
	return fmt.Errorf("cleanup needs review: %s: %s", reason, message)
}

// CleanupExists preserves suspended Jobs accepted before a controller restart.
func (r *Runner) CleanupExists(ctx context.Context, cleanup cleanupapi.Cleanup) (bool, error) {
	_, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, cleanup.Name+"-effect", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
