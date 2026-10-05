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
	"k8s.io/apimachinery/pkg/labels"
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

// CleanupJournal is the parent-owned journal access Reclaim needs.
type CleanupJournal interface {
	Get(context.Context, cleanupapi.Authority) (cleanupapi.Cleanup, error)
	UpdateStatus(context.Context, cleanupapi.Cleanup, cleanupapi.Status) error
}

// Reclaim drives one approved cleanup to its receipt. The parent journal is the
// only durable truth, so the phase is re-read between steps and handed to
// triageCleanup: a cleanup that settled or was sent to review elsewhere ends
// this call wherever that is observed.
func (r *Runner) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store CleanupJournal) (cleanupapi.Cleanup, error) {
	defer r.observeStep("reclaim_helper")()
	if r == nil || r.Client == nil || r.Pools == nil || r.Namespace == "" || r.Image == "" || r.Timeout <= 0 || store == nil || cleanup.UID == "" || cleanup.Name == "" || cleanup.Spec.Validate() != nil {
		return cleanupapi.Cleanup{}, fmt.Errorf("cleanup runner configuration is incomplete")
	}
	if done, result, err := triageCleanup(cleanup); done {
		return result, err
	}
	pool, err := r.approvedPool(ctx, store, cleanup)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	cleanup, err = refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	if done, result, err := triageCleanup(cleanup); done {
		return result, err
	}
	if cleanup.Status.Phase != cleanupapi.PhaseVerifying {
		if err := r.cleanupPoolReady(pool); err != nil {
			return cleanupapi.Cleanup{}, err
		}
	}
	job := r.cleanupJob(cleanup, pool.MountPath)
	if cleanup.Status.Phase == cleanupapi.PhaseVerifying {
		return r.resumeVerifying(ctx, cleanup, store, job)
	}
	return r.runCleanupExecutor(ctx, cleanup, store, job)
}

// runCleanupExecutor resolves exactly one suspended executor, binds it to the
// journal, starts it, and waits for its API receipt. The phase is re-read once
// more before binding, so a cleanup settled elsewhere still ends this call.
func (r *Runner) runCleanupExecutor(ctx context.Context, cleanup cleanupapi.Cleanup, store CleanupJournal, job *batchv1.Job) (cleanupapi.Cleanup, error) {
	created, err := r.ensureJob(ctx, store, cleanup, job)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	cleanup, err = refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	if done, result, err := triageCleanup(cleanup); done {
		return result, err
	}
	cleanup, err = r.bindExecutor(ctx, store, cleanup, created)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	if err := r.startCleanupJob(ctx, store, cleanup, created, job); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	return r.awaitReceipt(ctx, store, cleanup, created, job)
}

// triageCleanup reports whether the durable phase already decides this
// reconcile: a settled cleanup is returned as it stands, and one already under
// review is an error no executor work may follow.
func triageCleanup(current cleanupapi.Cleanup) (bool, cleanupapi.Cleanup, error) {
	switch current.Status.Phase {
	case cleanupapi.PhaseCompleted, cleanupapi.PhaseConfirmingAbsence:
		return true, current, nil
	case cleanupapi.PhaseNeedsReview:
		return true, cleanupapi.Cleanup{}, fmt.Errorf("cleanup %q needs review: %s", current.Name, current.Status.Reason)
	}
	return false, cleanupapi.Cleanup{}, nil
}

// approvedPool resolves the registered Pool the approved cleanup targets. Pool
// identity drift and lost lifecycle protection are contradictions, not transient
// failures, so they send the cleanup to review instead of retrying.
func (r *Runner) approvedPool(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup) (volumeapi.Pool, error) {
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
func (r *Runner) ensureJob(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup, job *batchv1.Job) (*batchv1.Job, error) {
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
func (r *Runner) bindExecutor(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup, created *batchv1.Job) (cleanupapi.Cleanup, error) {
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
func (r *Runner) awaitReceipt(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup, created, expected *batchv1.Job) (cleanupapi.Cleanup, error) {
	jobs := r.Client.BatchV1().Jobs(r.Namespace)
	result := cleanup
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		current, getErr := store.Get(pollCtx, cleanup.Spec.Authority)
		if getErr != nil {
			return false, getErr
		}
		if current.UID != cleanup.UID || current.Spec != cleanup.Spec {
			return false, cleanupapi.ErrConflict
		}
		switch current.Status.Phase {
		case cleanupapi.PhaseCompleted, cleanupapi.PhaseConfirmingAbsence:
			result = current
			return true, nil
		case cleanupapi.PhaseNeedsReview:
			return false, fmt.Errorf("cleanup needs review: %s", current.Status.Reason)
		}
		if current.Status.Executor == nil || current.Status.Executor.JobName != created.Name || current.Status.Executor.JobUID != string(created.UID) {
			return false, r.needsReview(pollCtx, store, current, "ExecutorIdentityChanged", "cleanup executor differs from the durable cleanup status")
		}
		currentJob, jobErr := jobs.Get(pollCtx, created.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(jobErr) {
			return false, r.needsReview(pollCtx, store, current, "ExecutorDisappeared", "cleanup Job disappeared before termination was acknowledged")
		}
		if jobErr != nil {
			return false, classifyKubernetesAPIError(jobErr)
		}
		if currentJob.UID != created.UID || !sameCleanupJob(currentJob, expected) {
			return false, r.needsReview(pollCtx, store, current, "ExecutorIdentityChanged", "cleanup Job changed before termination was acknowledged")
		}
		complete, failure, failed := cleanupJobOutcome(currentJob)
		if failed {
			return false, r.needsReview(pollCtx, store, current, "ExecutorFailed", fmt.Sprintf("cleanup Job failed: %s", failure))
		}
		if !complete {
			return false, nil
		}
		if current.Status.Phase != cleanupapi.PhaseVerifying || current.Status.Receipt == nil {
			return false, r.needsReview(pollCtx, store, current, "ReceiptMissing", "cleanup Job completed without a durable receipt")
		}
		result = current
		return true, nil
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return cleanupapi.Cleanup{}, fmt.Errorf("wait for cleanup receipt: %w", err)
	}
	return result, nil
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

func (r *Runner) resumeVerifying(ctx context.Context, cleanup cleanupapi.Cleanup, store CleanupJournal, expected *batchv1.Job) (cleanupapi.Cleanup, error) {
	if !boundReceipt(cleanup) {
		return cleanupapi.Cleanup{}, r.needsReview(ctx, store, cleanup, "ReceiptInvalid", "cleanup receipt does not match the durable executor and intent")
	}
	selector := labels.Set{cleanupNameLabel: cleanup.Name, cleanupUIDLabel: cleanup.UID}.AsSelector().String()
	var result cleanupapi.Cleanup
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		settled, current, pollErr := r.observeSettlement(pollCtx, cleanup, store, expected, selector)
		if settled {
			result = current
		}
		return settled, pollErr
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return cleanupapi.Cleanup{}, fmt.Errorf("wait for cleanup executor settlement: %w", err)
	}
	return result, nil
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
func (r *Runner) observeSettlement(ctx context.Context, cleanup cleanupapi.Cleanup, store CleanupJournal, expected *batchv1.Job, selector string) (bool, cleanupapi.Cleanup, error) {
	current, err := refreshCleanup(ctx, store, cleanup)
	if err != nil {
		return false, cleanupapi.Cleanup{}, err
	}
	if current.Status.Phase == cleanupapi.PhaseCompleted || current.Status.Phase == cleanupapi.PhaseConfirmingAbsence {
		return true, current, nil
	}
	if current.Status.Phase != cleanupapi.PhaseVerifying || current.Status.Executor == nil || current.Status.Receipt == nil ||
		!reflect.DeepEqual(current.Status.Executor, cleanup.Status.Executor) || !reflect.DeepEqual(current.Status.Receipt, cleanup.Status.Receipt) {
		return false, cleanupapi.Cleanup{}, cleanupapi.ErrConflict
	}
	job, getErr := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, current.Status.Executor.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return r.settleWithoutExecutor(ctx, current, selector)
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
	return false, cleanupapi.Cleanup{}, nil
}

// settleWithoutExecutor settles a cleanup whose Job is already gone, unless one
// of that Job's own Pods is still around and could still write.
func (r *Runner) settleWithoutExecutor(ctx context.Context, current cleanupapi.Cleanup, selector string) (bool, cleanupapi.Cleanup, error) {
	pods, listErr := r.Client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if listErr != nil {
		return false, cleanupapi.Cleanup{}, classifyKubernetesAPIError(listErr)
	}
	for index := range pods.Items {
		if ownedByJob(&pods.Items[index], types.UID(current.Status.Executor.JobUID)) {
			return false, cleanupapi.Cleanup{}, nil
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

func refreshCleanup(ctx context.Context, store CleanupJournal, expected cleanupapi.Cleanup) (cleanupapi.Cleanup, error) {
	current, err := store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, fmt.Errorf("read cleanup intent: %w", classifyKubernetesAPIError(err))
	}
	if current.UID != expected.UID || current.Spec != expected.Spec {
		return cleanupapi.Cleanup{}, cleanupapi.ErrConflict
	}
	return current, nil
}

func (r *Runner) startCleanupJob(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup, created, expected *batchv1.Job) error {
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

func (r *Runner) needsReview(ctx context.Context, store CleanupJournal, cleanup cleanupapi.Cleanup, reason, message string) error {
	if updateErr := store.UpdateStatus(ctx, cleanup, cleanupapi.Status{
		Phase: cleanupapi.PhaseNeedsReview, Reason: reason, Message: message,
		Executor: cleanup.Status.Executor, Receipt: cleanup.Status.Receipt,
	}); updateErr != nil {
		return fmt.Errorf("mark cleanup for review after %s: %w", reason, updateErr)
	}
	return fmt.Errorf("cleanup needs review: %s: %s", reason, message)
}
