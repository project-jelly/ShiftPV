package helperpod

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	creationOperationAnnotation = "shiftpv.io/operation-id"
	creationVolumeUIDAnnotation = "shiftpv.io/volume-uid"
	creationCopyIDAnnotation    = "shiftpv.io/copy-id"
)

// runCreation resolves one durable Pod from the recorded creation operation.
// A successful Pod remains as execution evidence until CompleteCreate succeeds.
func (r *Runner) runCreation(ctx context.Context, identity volume.CopyIdentity, operationID string, command []string) (string, error) {
	desired, err := r.creationPod(ctx, identity, operationID, command)
	if err != nil {
		return "", err
	}
	pods := r.Client.CoreV1().Pods(r.Namespace)
	current, createErr := pods.Create(ctx, desired, metav1.CreateOptions{})
	if createErr != nil {
		if !apierrors.IsAlreadyExists(createErr) && !isAmbiguousKubernetesError(createErr) {
			return "", fmt.Errorf("create creation helper Pod: %w", classifyKubernetesAPIError(createErr))
		}
		current, err = pods.Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			if isAmbiguousKubernetesError(createErr) && apierrors.IsNotFound(err) {
				return "", fmt.Errorf("create creation helper Pod: %w", classifyKubernetesAPIError(createErr))
			}
			return "", fmt.Errorf("resolve creation helper Pod: %w", classifyKubernetesAPIError(err))
		}
	}
	if err := sameCreationPod(desired, current); err != nil {
		return "", err
	}
	uid := current.UID
	if uid == "" {
		return "", fmt.Errorf("creation helper Pod has no UID")
	}

	result := ""
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		pod, getErr := pods.Get(pollCtx, desired.Name, metav1.GetOptions{})
		if getErr != nil {
			return false, classifyKubernetesAPIError(getErr)
		}
		if pod.UID != uid {
			return false, fmt.Errorf("creation helper Pod UID changed")
		}
		if err := sameCreationPod(desired, pod); err != nil {
			return false, err
		}
		terminated, terminationErr := creationTermination(pod)
		if terminationErr != nil {
			return false, terminationErr
		}
		if terminated == nil {
			return false, nil
		}
		if pod.Status.Phase == corev1.PodSucceeded && terminated.ExitCode == 0 {
			result = terminated.Message
			return true, nil
		}
		if err := r.deleteCreationPod(pollCtx, desired, uid); err != nil {
			return false, err
		}
		return false, retryableError{err: fmt.Errorf("creation helper exited %d", terminated.ExitCode)}
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return "", fmt.Errorf("wait for creation helper Pod: %w", err)
	}
	return result, nil
}

func (r *Runner) finalizeCreation(ctx context.Context, identity volume.CopyIdentity, operationID string) error {
	desired, err := r.creationPod(ctx, identity, operationID, []string{
		"/shiftpv-volume-helper", "create",
		"--operation-id=" + operationID,
		"--installation-id=" + identity.InstallationID,
		"--pool-name=" + identity.PoolName,
		"--pool-uid=" + identity.PoolUID,
		"--volume-id=" + identity.VolumeID,
		"--volume-uid=" + identity.VolumeUID,
		"--copy-id=" + identity.CopyID,
		"--node-name=" + identity.NodeName,
	})
	if err != nil {
		return err
	}
	current, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read settled creation helper Pod: %w", classifyKubernetesAPIError(err))
	}
	if err := sameCreationPod(desired, current); err != nil {
		return err
	}
	terminated, err := creationTermination(current)
	if err != nil {
		return err
	}
	if terminated == nil || current.Status.Phase != corev1.PodSucceeded || terminated.ExitCode != 0 {
		return retryableError{err: fmt.Errorf("creation helper Pod has not succeeded")}
	}
	return r.deleteCreationPod(ctx, desired, current.UID)
}

func (r *Runner) creationPod(ctx context.Context, identity volume.CopyIdentity, operationID string, command []string) (*corev1.Pod, error) {
	if identity.Validate() != nil || !volume.ValidIdentityToken(operationID) || len(command) == 0 {
		return nil, fmt.Errorf("creation helper identity is incomplete")
	}
	poolRoot, err := r.poolRootForIdentity(ctx, identity)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(poolRoot) {
		return nil, fmt.Errorf("pool root must be absolute")
	}
	if r.Client == nil || r.Namespace == "" || r.Image == "" || r.Timeout <= 0 {
		return nil, fmt.Errorf("helper Pod configuration is incomplete")
	}
	pod := r.helperPod(identity.NodeName, identity.VolumeID, poolRoot, command)
	sum := sha256.Sum256([]byte(operationID + "\x00" + identity.CopyID))
	pod.Name = fmt.Sprintf("shiftpv-create-%x", sum[:16])
	pod.Namespace = r.Namespace
	pod.GenerateName = ""
	pod.Annotations = map[string]string{
		creationOperationAnnotation: operationID,
		creationVolumeUIDAnnotation: identity.VolumeUID,
		creationCopyIDAnnotation:    identity.CopyID,
	}
	pod.Spec.AutomountServiceAccountToken = boolPtr(true)
	return pod, nil
}

func sameCreationPod(expected, current *corev1.Pod) error {
	_, err := creationPodDifference(expected, current)
	return err
}

// creationPodDifference names the first field in which a live creation helper
// Pod departs from the desired one, together with the error that field's group
// is reported as. The groups are evaluated in their original order, so the
// container rows may index the first container: a shape row has already fixed
// the current-side count, and the expected Pod is always the one this package
// rendered.
func creationPodDifference(expected, current *corev1.Pod) (string, error) {
	if expected == nil || current == nil {
		return "pod", fmt.Errorf("creation helper Pod identity or execution shape changed")
	}
	if field := firstDifference(creationPodShapeChecks(expected, current)); field != "" {
		return field, fmt.Errorf("creation helper Pod identity or execution shape changed")
	}
	for key, value := range expected.Labels {
		if current.Labels[key] != value {
			return "metadata.labels[" + key + "]", fmt.Errorf("creation helper Pod label changed")
		}
	}
	for key, value := range expected.Annotations {
		if current.Annotations[key] != value {
			return "metadata.annotations[" + key + "]", fmt.Errorf("creation helper Pod annotation changed")
		}
	}
	if field := firstDifference(helperPodCommandChecks(expected, current)); field != "" {
		return field, fmt.Errorf("creation helper Pod command changed")
	}
	if !helperPoolMountBound(expected, current) {
		return "spec.volumes[pool]", fmt.Errorf("creation helper Pod Pool mount changed")
	}
	return "", nil
}

// creationPodShapeChecks holds the helper to one identity-bound container with
// no host namespace and no second execution path.
func creationPodShapeChecks(expected, current *corev1.Pod) []fieldCheck {
	return []fieldCheck{
		{"metadata.uid", func() bool { return current.UID != "" }},
		{"metadata.deletionTimestamp", func() bool { return current.DeletionTimestamp == nil }},
		{"metadata.name", func() bool { return current.Name == expected.Name }},
		{"metadata.namespace", func() bool { return current.Namespace == expected.Namespace }},
		{"spec.nodeName", func() bool { return current.Spec.NodeName == expected.Spec.NodeName }},
		{"spec.serviceAccountName", func() bool {
			return current.Spec.ServiceAccountName == expected.Spec.ServiceAccountName
		}},
		{"spec.restartPolicy", func() bool { return current.Spec.RestartPolicy == corev1.RestartPolicyNever }},
		{"spec.automountServiceAccountToken", func() bool {
			return current.Spec.AutomountServiceAccountToken != nil && *current.Spec.AutomountServiceAccountToken
		}},
		{"spec.hostNetwork", func() bool { return !current.Spec.HostNetwork }},
		{"spec.hostPID", func() bool { return !current.Spec.HostPID }},
		{"spec.hostIPC", func() bool { return !current.Spec.HostIPC }},
		{"spec.containers", func() bool { return len(current.Spec.Containers) == 1 }},
		{"spec.initContainers", func() bool { return len(current.Spec.InitContainers) == 0 }},
		{"spec.ephemeralContainers", func() bool { return len(current.Spec.EphemeralContainers) == 0 }},
	}
}

// helperPodCommandChecks compares what the container would actually run: the
// recorded operation command with no injected argument or environment.
func helperPodCommandChecks(expected, current *corev1.Pod) []fieldCheck {
	want, got := expected.Spec.Containers[0], current.Spec.Containers[0]
	return []fieldCheck{
		{"spec.containers[0].name", func() bool { return got.Name == want.Name }},
		{"spec.containers[0].image", func() bool { return got.Image == want.Image }},
		{"spec.containers[0].command", func() bool { return reflect.DeepEqual(got.Command, want.Command) }},
		{"spec.containers[0].args", func() bool { return len(got.Args) == 0 }},
		{"spec.containers[0].env", func() bool { return len(got.Env) == 0 }},
		{"spec.containers[0].envFrom", func() bool { return len(got.EnvFrom) == 0 }},
		{"spec.containers[0].resources", func() bool { return reflect.DeepEqual(got.Resources, want.Resources) }},
		{"spec.containers[0].securityContext", func() bool {
			return reflect.DeepEqual(got.SecurityContext, want.SecurityContext)
		}},
	}
}

// helperPoolMountBound reports whether the Pod still mounts exactly the
// approved Pool root at the helper's mount path, with no subpath narrowing or
// redirecting it.
func helperPoolMountBound(expected, current *corev1.Pod) bool {
	got := current.Spec.Containers[0]
	for _, podVolume := range current.Spec.Volumes {
		if podVolume.Name != "pool" || !reflect.DeepEqual(podVolume.HostPath, expected.Spec.Volumes[0].HostPath) {
			continue
		}
		for _, mount := range got.VolumeMounts {
			if mount.Name == "pool" && mount.MountPath == mountPath && mount.SubPath == "" && mount.SubPathExpr == "" &&
				mount.ReadOnly == expected.Spec.Containers[0].VolumeMounts[0].ReadOnly {
				return true
			}
		}
	}
	return false
}

func creationTermination(pod *corev1.Pod) (*corev1.ContainerStateTerminated, error) {
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return nil, nil
	}
	if len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].Name != "operation" ||
		pod.Status.ContainerStatuses[0].State.Terminated == nil {
		return nil, retryableError{err: fmt.Errorf("creation helper Pod lacks termination evidence")}
	}
	return pod.Status.ContainerStatuses[0].State.Terminated, nil
}

func (r *Runner) deleteCreationPod(ctx context.Context, expected *corev1.Pod, uid types.UID) error {
	if uid == "" {
		return fmt.Errorf("creation helper Pod UID is missing")
	}
	zero := int64(0)
	pods := r.Client.CoreV1().Pods(r.Namespace)
	err := pods.Delete(ctx, expected.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) && !isAmbiguousKubernetesError(err) {
		return fmt.Errorf("delete creation helper Pod: %w", classifyKubernetesAPIError(err))
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		current, getErr := pods.Get(pollCtx, expected.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}
		if getErr != nil {
			return false, classifyKubernetesAPIError(getErr)
		}
		if current.UID != uid {
			return false, fmt.Errorf("creation helper Pod was replaced during deletion")
		}
		identity := current.DeepCopy()
		identity.DeletionTimestamp = nil
		if err := sameCreationPod(expected, identity); err != nil {
			return false, err
		}
		return false, nil
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = retryableError{err: err}
		}
		return fmt.Errorf("wait for creation helper Pod deletion: %w", err)
	}
	return nil
}

func isAmbiguousKubernetesError(err error) bool {
	return apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err)
}

// CreationExists keeps already accepted helper executions on their own backend.
func (r *Runner) CreationExists(ctx context.Context, identity volume.CopyIdentity) (bool, error) {
	operationID := "create-" + identity.VolumeUID
	sum := sha256.Sum256([]byte(operationID + "\x00" + identity.CopyID))
	_, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, fmt.Sprintf("shiftpv-create-%x", sum[:16]), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
