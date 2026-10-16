package helperpod

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// SourceUsagePurposeLabel is a queue hint. Move ownership and the immutable Pod
// execution shape, rather than this mutable label, authorize result adoption.
const SourceUsagePurposeLabel = "shiftpv.io/source-usage"

// AdvanceSourceUsage observes one durable read-only measurement. Successful
// results remain until the caller durably records SourceBytes and finalizes.
func (r *Runner) AdvanceSourceUsage(ctx context.Context, move volumeapi.Move, copy volume.CopyIdentity) (bytes int64, ready bool, resultErr error) {
	defer func() {
		if errors.Is(resultErr, context.DeadlineExceeded) {
			resultErr = retryableError{err: resultErr}
		}
	}()
	if err := r.validateSourceUsage(move, copy); err != nil {
		return 0, false, err
	}
	pool, err := r.Pools.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return 0, false, err
	}
	if pool.Name != copy.PoolName || pool.UID != copy.PoolUID || pool.NodeName != copy.NodeName {
		return 0, false, fmt.Errorf("source usage Pool identity changed")
	}
	pod, err := r.ensureSourceUsagePod(ctx, move, copy, pool)
	if err != nil {
		return 0, false, err
	}
	if err := r.sourceUsagePodOwned(move, pod); err != nil {
		return 0, false, err
	}
	if pod.DeletionTimestamp != nil {
		return 0, false, nil
	}
	if !r.sourceUsagePodCurrent(move, copy, pool, pod) {
		_, err := r.deleteSourceUsagePod(ctx, move, pod)
		return 0, false, err
	}
	return r.sourceUsageResult(ctx, move, pod)
}

func (r *Runner) validateSourceUsage(move volumeapi.Move, copy volume.CopyIdentity) error {
	if r == nil || r.Client == nil || r.Pools == nil || r.Namespace == "" || r.Image == "" || r.Timeout <= 0 ||
		!validSourceUsageMove(move) || move.Deleting || copy.Validate() != nil || copy.Role != volume.RoleServing ||
		copy.VolumeID != move.Spec.VolumeID || copy.NodeName != move.Spec.SourceNode ||
		(move.Status.SourceCopy != nil && *move.Status.SourceCopy != copy) {
		return fmt.Errorf("source usage requires exact Move/copy identity and helper configuration")
	}
	return nil
}

func (r *Runner) ensureSourceUsagePod(ctx context.Context, move volumeapi.Move, copy volume.CopyIdentity, pool volumeapi.Pool) (*corev1.Pod, error) {
	pods := r.Client.CoreV1().Pods(r.Namespace)
	current, err := pods.Get(ctx, sourceUsagePodName(move.UID), metav1.GetOptions{})
	if err == nil {
		return current, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, classifyKubernetesAPIError(err)
	}
	desired, err := r.sourceUsagePod(move, copy, pool)
	if err != nil {
		return nil, err
	}
	current, createErr := pods.Create(ctx, desired, metav1.CreateOptions{})
	if createErr == nil {
		return current, nil
	}
	if !apierrors.IsAlreadyExists(createErr) && !isAmbiguousKubernetesError(createErr) {
		return nil, classifyKubernetesAPIError(createErr)
	}
	current, err = pods.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && isAmbiguousKubernetesError(createErr) {
		return nil, classifyKubernetesAPIError(createErr)
	}
	return current, classifyKubernetesAPIError(err)
}

func (r *Runner) sourceUsagePodCurrent(move volumeapi.Move, copy volume.CopyIdentity, pool volumeapi.Pool, pod *corev1.Pod) bool {
	snapshot, err := sourceUsageSnapshotFrom(pod)
	if err != nil || pool.Generation < snapshot.InitialGeneration {
		return false
	}
	evidence, err := poolcapacity.UsageConfigurationEvidence(pool)
	if err != nil || evidence != snapshot.ConfigurationEvidence {
		return false
	}
	// The generation is projected only to reconstruct the immutable command.
	// The helper checks actual current readiness before and after reading data.
	pool.Generation = snapshot.InitialGeneration
	expected, err := r.sourceUsagePod(move, copy, pool)
	return err == nil && sameSourceUsagePod(expected, pod)
}

func (r *Runner) sourceUsageResult(ctx context.Context, move volumeapi.Move, pod *corev1.Pod) (int64, bool, error) {
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return 0, false, nil
	}
	terminated, terminationErr := helperPodTermination(pod)
	if terminationErr == nil && pod.Status.Phase == corev1.PodSucceeded && terminated.ExitCode == 0 {
		bytes, err := strconv.ParseInt(strings.TrimSpace(terminated.Message), 10, 64)
		if err == nil && bytes > 0 {
			return bytes, true, nil
		}
	}
	// Failed, evicted and malformed terminal Pods cannot yield a result. This
	// readonly operation can be retried after its exact Pod is absent.
	_, err := r.deleteSourceUsagePod(ctx, move, pod)
	if err != nil {
		return 0, false, err
	}
	return 0, false, retryableError{err: fmt.Errorf("source usage helper did not produce a positive successful result")}
}

// FinalizeSourceUsage retires only this Move's read-only Pod. It deliberately
// needs no current Pool, image or execution profile, including during recovery.
func (r *Runner) FinalizeSourceUsage(ctx context.Context, move volumeapi.Move) (bool, error) {
	if r == nil || r.Client == nil || r.Namespace == "" || !validSourceUsageMove(move) {
		return false, fmt.Errorf("source usage finalization requires exact Move identity")
	}
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, sourceUsagePodName(move.UID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, classifyKubernetesAPIError(err)
	}
	return r.deleteSourceUsagePod(ctx, move, pod)
}

func (r *Runner) deleteSourceUsagePod(ctx context.Context, move volumeapi.Move, pod *corev1.Pod) (bool, error) {
	if err := r.sourceUsagePodOwned(move, pod); err != nil {
		return false, err
	}
	pods := r.Client.CoreV1().Pods(r.Namespace)
	if pod.DeletionTimestamp == nil {
		zero, uid := int64(0), pod.UID
		err := pods.Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &uid}})
		if err != nil && !apierrors.IsNotFound(err) && !isAmbiguousKubernetesError(err) {
			return false, classifyKubernetesAPIError(err)
		}
	}
	current, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, classifyKubernetesAPIError(err)
	}
	if current.UID != pod.UID {
		return false, fmt.Errorf("source usage Pod was replaced during deletion")
	}
	return false, r.sourceUsagePodOwned(move, current)
}
