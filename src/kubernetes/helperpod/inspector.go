package helperpod

import (
	"context"
	"crypto/sha256"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Inspector observes helper executions accepted by an older controller. It
// neither adopts them nor changes their execution or recorded completion proof.
type Inspector struct {
	Client    kubernetes.Interface
	Namespace string
}

func (i *Inspector) validate() error {
	if i == nil || i.Client == nil || !volume.ValidObjectName(i.Namespace) {
		return fmt.Errorf("helper inspector configuration is incomplete")
	}
	return nil
}

// CreationExists treats any Pod at the deterministic legacy name as existing
// responsibility, regardless of its current labels or execution shape.
func (i *Inspector) CreationExists(ctx context.Context, identity volume.CopyIdentity) (bool, error) {
	if err := i.validate(); err != nil {
		return false, err
	}
	operationID := "create-" + identity.VolumeUID
	if identity.Validate() != nil || !volume.ValidIdentityToken(operationID) {
		return false, fmt.Errorf("invalid creation helper identity")
	}
	sum := sha256.Sum256([]byte(operationID + "\x00" + identity.CopyID))
	_, err := i.Client.CoreV1().Pods(i.Namespace).Get(ctx, fmt.Sprintf("shiftpv-create-%x", sum[:16]), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// CleanupExists does not bind or activate a previously accepted cleanup Job.
func (i *Inspector) CleanupExists(ctx context.Context, cleanup cleanupapi.Cleanup) (bool, error) {
	if err := i.validate(); err != nil {
		return false, err
	}
	if cleanup.Spec.Validate() != nil || cleanup.UID != cleanup.Spec.Authority.UID || !volume.ValidObjectName(cleanup.Name) || !volume.ValidObjectName(cleanup.Name+"-effect") {
		return false, fmt.Errorf("%w: invalid cleanup helper identity", cleanupapi.ErrConflict)
	}
	_, err := i.Client.BatchV1().Jobs(i.Namespace).Get(ctx, cleanup.Name+"-effect", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// CompletedCleanupSettled requires both self-contained historical proof and
// live quiescence of its exact legacy executor. Callers must exclude new work
// while relying on this observation; these API reads are not an atomic fence.
func (i *Inspector) CompletedCleanupSettled(ctx context.Context, cleanup cleanupapi.Cleanup) (bool, error) {
	if err := cleanupapi.ValidateSettled(cleanup); err != nil {
		return false, err
	}
	if err := i.validate(); err != nil {
		return false, err
	}
	executor := cleanup.Status.Executor
	if executor.Kind != "" || !executor.Valid() || !volume.ValidIdentityToken(executor.PodUID) {
		return false, fmt.Errorf("%w: completed cleanup is not bound to a legacy Job", cleanupapi.ErrConflict)
	}
	job, err := i.Client.BatchV1().Jobs(i.Namespace).Get(ctx, executor.JobName, metav1.GetOptions{})
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return false, fmt.Errorf("read completed cleanup Job: %w", err)
	}
	if !missing {
		if !i.sameCleanupExecutor(job, cleanup) {
			return false, fmt.Errorf("%w: completed cleanup Job identity changed", cleanupapi.ErrConflict)
		}
		if !inspectorJobComplete(job) {
			return false, nil
		}
	}
	pods, err := i.Client.CoreV1().Pods(i.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list completed cleanup Pods: %w", err)
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if !inspectorExecutorPod(pod, executor) {
			continue
		}
		if missing {
			// Preserve the old Job-absence contract: a remaining owned or
			// receipt-writing Pod still prevents absence-based settlement.
			return false, nil
		}
		if !inspectorPodIdentity(pod, executor, i.Namespace) {
			return false, fmt.Errorf("%w: completed cleanup Pod identity changed", cleanupapi.ErrConflict)
		}
		if !inspectorPodTerminated(pod) {
			return false, nil
		}
	}
	return true, nil
}

func (i *Inspector) sameCleanupExecutor(job *batchv1.Job, cleanup cleanupapi.Cleanup) bool {
	executor := cleanup.Status.Executor
	owner := metav1.GetControllerOf(job)
	return job.Namespace == i.Namespace && job.Name == executor.JobName && string(job.UID) == executor.JobUID &&
		job.Spec.Template.Spec.NodeName == executor.NodeName && owner != nil && owner.APIVersion == "shiftpv.io/v1alpha1" &&
		owner.Kind == cleanup.Spec.Authority.Kind && owner.Name == cleanup.Spec.Authority.Name && string(owner.UID) == cleanup.Spec.Authority.UID
}

func inspectorJobComplete(job *batchv1.Job) bool {
	if job.DeletionTimestamp != nil || job.Status.Active != 0 {
		return false
	}
	complete := false
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return false
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			complete = true
		}
	}
	return complete
}

func inspectorExecutorPod(pod *corev1.Pod, executor *cleanupapi.Executor) bool {
	if string(pod.UID) == executor.PodUID {
		return true
	}
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && string(owner.UID) == executor.JobUID {
			return true
		}
	}
	return false
}

func inspectorPodIdentity(pod *corev1.Pod, executor *cleanupapi.Executor, namespace string) bool {
	if pod.UID == "" || pod.Namespace != namespace || pod.Spec.NodeName != executor.NodeName {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && (string(owner.UID) != executor.JobUID || owner.Name != executor.JobName) {
			return false
		}
	}
	return true
}

func inspectorPodTerminated(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) || len(pod.Spec.Containers) == 0 {
		return false
	}
	main := make([]string, len(pod.Spec.Containers))
	for index, container := range pod.Spec.Containers {
		main[index] = container.Name
	}
	init := make([]string, len(pod.Spec.InitContainers))
	for index, container := range pod.Spec.InitContainers {
		init[index] = container.Name
	}
	ephemeral := make([]string, len(pod.Spec.EphemeralContainers))
	for index, container := range pod.Spec.EphemeralContainers {
		ephemeral[index] = container.Name
	}
	return inspectorContainersTerminated(main, pod.Status.ContainerStatuses) &&
		inspectorContainersTerminated(init, pod.Status.InitContainerStatuses) &&
		inspectorContainersTerminated(ephemeral, pod.Status.EphemeralContainerStatuses)
}

func inspectorContainersTerminated(names []string, statuses []corev1.ContainerStatus) bool {
	if len(names) != len(statuses) {
		return false
	}
	remaining := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" || remaining[name] {
			return false
		}
		remaining[name] = true
	}
	for _, status := range statuses {
		if !remaining[status.Name] || status.State.Terminated == nil || status.State.Running != nil || status.State.Waiting != nil {
			return false
		}
		delete(remaining, status.Name)
	}
	return len(remaining) == 0
}
