package helperpod

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const mountPath = "/pool"

type retryableError struct{ err error }

func (e retryableError) Error() string { return e.err.Error() }
func (e retryableError) Unwrap() error { return e.err }
func (retryableError) Retryable() bool { return true }

type Runner struct {
	Client             kubernetes.Interface
	Namespace          string
	ServiceAccountName string
	Pools              interface {
		PoolForNode(context.Context, string) (volumeapi.Pool, error)
		PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	}
	Image                   string
	Timeout                 time.Duration
	Resources               corev1.ResourceRequirements
	PoolReadinessStaleAfter time.Duration
}

func (r *Runner) CreateCopy(ctx context.Context, identity volume.CopyIdentity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	operationID, err := volumeapi.CreationOperationID(identity.VolumeUID)
	if err != nil {
		return err
	}
	_, err = r.runCreation(ctx, identity, operationID, []string{
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
	return err
}

// FinalizeCreate removes only the exact, successfully terminated helper after
// the controller has made the volume Ready. Absence is the settled state.
func (r *Runner) FinalizeCreate(ctx context.Context, identity volume.CopyIdentity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	operationID, err := volumeapi.CreationOperationID(identity.VolumeUID)
	if err != nil {
		return err
	}
	return r.finalizeCreation(ctx, identity, operationID)
}

func (r *Runner) StatFS(ctx context.Context, nodeName string) (poolcapacity.Filesystem, error) {
	poolRoot, err := r.poolRoot(ctx, nodeName)
	if err != nil {
		return poolcapacity.Filesystem{}, err
	}
	return r.statFSAt(ctx, nodeName, poolRoot)
}

// StatFSForPool probes one exact capacity unit even when its node has others.
func (r *Runner) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	if r.Pools == nil {
		return poolcapacity.Filesystem{}, fmt.Errorf("ShiftPVPool registry is required")
	}
	current, err := r.Pools.PoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName)
	if err != nil {
		return poolcapacity.Filesystem{}, err
	}
	return r.statFSAt(ctx, pool.NodeName, current.MountPath)
}

func (r *Runner) statFSAt(ctx context.Context, nodeName, poolRoot string) (poolcapacity.Filesystem, error) {
	output, err := r.runForResultAtPath(ctx, nodeName, "pool-capacity", poolRoot, []string{
		"sh", "-c", "stat -f -c '%b %a %S %d' /pool > /dev/termination-log",
	})
	if err != nil {
		return poolcapacity.Filesystem{}, err
	}
	stats, err := poolcapacity.ParseStatOutput(output)
	if err != nil {
		return poolcapacity.Filesystem{}, retryableError{err: fmt.Errorf("decode helper statfs result: %w", err)}
	}
	return stats, nil
}

func (r *Runner) VolumeUsage(ctx context.Context, nodeName, volumeID string) (int64, error) {
	poolRoot, err := r.poolRoot(ctx, nodeName)
	if err != nil {
		return 0, err
	}
	return r.volumeUsageAt(ctx, nodeName, volumeID, poolRoot)
}

func (r *Runner) VolumeUsageForCopy(ctx context.Context, copy volume.CopyIdentity) (int64, error) {
	if err := copy.Validate(); err != nil {
		return 0, err
	}
	poolRoot, err := r.poolRootForIdentity(ctx, copy)
	if err != nil {
		return 0, err
	}
	return r.volumeUsageAt(ctx, copy.NodeName, copy.VolumeID, poolRoot)
}

func (r *Runner) volumeUsageAt(ctx context.Context, nodeName, volumeID, poolRoot string) (int64, error) {
	path, err := volume.Path(mountPath, volumeID)
	if err != nil {
		return 0, err
	}
	output, err := r.runForResultAtPath(ctx, nodeName, volumeID, poolRoot, []string{
		"sh", "-c", "du -sb \"$1\" | awk '{print $1}' > /dev/termination-log", "shiftpv-du", path,
	})
	if err != nil {
		return 0, err
	}
	bytes, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil || bytes < 0 {
		return 0, retryableError{err: fmt.Errorf("decode helper volume usage result %q", output)}
	}
	return bytes, nil
}

func (r *Runner) runForResultAtPath(ctx context.Context, nodeName, volumeID, poolRoot string, command []string) (string, error) {
	if nodeName == "" {
		return "", fmt.Errorf("node name is required")
	}
	if !filepath.IsAbs(poolRoot) {
		return "", fmt.Errorf("pool root must be absolute")
	}
	if r.Client == nil {
		return "", fmt.Errorf("Kubernetes client is required")
	}
	if r.Namespace == "" || r.Image == "" || r.Timeout <= 0 {
		return "", fmt.Errorf("helper Pod configuration is incomplete")
	}

	desired := r.helperPod(nodeName, volumeID, poolRoot, command)
	pods := r.Client.CoreV1().Pods(r.Namespace)
	pod, err := pods.Create(ctx, desired, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create helper Pod: %w", classifyKubernetesAPIError(err))
	}
	if pod.UID == "" {
		return "", fmt.Errorf("helper Pod has no UID")
	}
	uid := pod.UID
	zero := int64(0)
	defer func() {
		_ = pods.Delete(context.Background(), pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
			Preconditions:      &metav1.Preconditions{UID: &uid},
		})
	}()
	desired.Name = pod.Name
	desired.Namespace = r.Namespace
	if err := sameResultPod(desired, pod, uid); err != nil {
		return "", err
	}

	result := ""
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, r.Timeout, true, func(pollCtx context.Context) (bool, error) {
		current, getErr := pods.Get(pollCtx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return false, nil
			}
			return false, classifyKubernetesAPIError(getErr)
		}
		if err := sameResultPod(desired, current, uid); err != nil {
			return false, err
		}
		switch current.Status.Phase {
		case corev1.PodSucceeded:
			if len(current.Status.ContainerStatuses) == 1 && current.Status.ContainerStatuses[0].State.Terminated != nil {
				result = current.Status.ContainerStatuses[0].State.Terminated.Message
			}
			return true, nil
		case corev1.PodFailed:
			return false, retryableError{err: fmt.Errorf("helper Pod failed: %s", current.Status.Message)}
		default:
			return false, nil
		}
	})
	if err != nil {
		return "", fmt.Errorf("wait for helper Pod on node %q: %w", nodeName, err)
	}
	return result, nil
}

// sameResultPod binds a short-lived stat/du result to the exact Pod
// incarnation and execution shape that the Runner created. Pod names are
// reusable, so a name-only watch could otherwise accept a replacement Pod's
// termination message as trusted capacity or usage data.
func sameResultPod(expected, current *corev1.Pod, uid types.UID) error {
	if expected == nil || current == nil || uid == "" || current.UID != uid {
		return fmt.Errorf("helper Pod identity changed")
	}
	if firstDifference(resultPodShapeChecks(expected, current)) != "" {
		return fmt.Errorf("helper Pod identity or execution shape changed")
	}
	for key, value := range expected.Labels {
		if current.Labels[key] != value {
			return fmt.Errorf("helper Pod label changed")
		}
	}
	if firstDifference(helperPodCommandChecks(expected, current)) != "" {
		return fmt.Errorf("helper Pod command changed")
	}
	if !helperPoolMountBound(expected, current) {
		return fmt.Errorf("helper Pod Pool mount changed")
	}
	return nil
}

func resultPodShapeChecks(expected, current *corev1.Pod) []fieldCheck {
	return []fieldCheck{
		{"metadata.deletionTimestamp", func() bool { return current.DeletionTimestamp == nil }},
		{"metadata.name", func() bool { return current.Name == expected.Name }},
		{"metadata.namespace", func() bool { return current.Namespace == expected.Namespace }},
		{"spec.nodeName", func() bool { return current.Spec.NodeName == expected.Spec.NodeName }},
		{"spec.serviceAccountName", func() bool {
			return current.Spec.ServiceAccountName == expected.Spec.ServiceAccountName
		}},
		{"spec.restartPolicy", func() bool { return current.Spec.RestartPolicy == corev1.RestartPolicyNever }},
		{"spec.hostNetwork", func() bool { return !current.Spec.HostNetwork }},
		{"spec.hostPID", func() bool { return !current.Spec.HostPID }},
		{"spec.hostIPC", func() bool { return !current.Spec.HostIPC }},
		{"spec.containers", func() bool { return len(current.Spec.Containers) == 1 }},
		{"spec.initContainers", func() bool { return len(current.Spec.InitContainers) == 0 }},
		{"spec.ephemeralContainers", func() bool { return len(current.Spec.EphemeralContainers) == 0 }},
	}
}

func (r *Runner) helperPod(nodeName, volumeID, poolRoot string, command []string) *corev1.Pod {
	hostPathType := corev1.HostPathDirectory
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "shiftpv-volume-op-",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "shiftpv",
				"app.kubernetes.io/component": "volume-helper",
				"shiftpv.io/volume-id":        volumeID,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:           nodeName,
			ServiceAccountName: r.ServiceAccountName,
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:         "operation",
				Image:        r.Image,
				Command:      command,
				Resources:    r.Resources,
				VolumeMounts: []corev1.VolumeMount{{Name: "pool", MountPath: mountPath}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPtr(false),
					RunAsUser:                int64Ptr(0),
					RunAsGroup:               int64Ptr(0),
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "pool",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: poolRoot,
					Type: &hostPathType,
				}},
			}},
		},
	}
}

func (r *Runner) poolRoot(ctx context.Context, nodeName string) (string, error) {
	if r.Pools == nil {
		return "", fmt.Errorf("ShiftPVPool registry is required")
	}
	pool, err := r.Pools.PoolForNode(ctx, nodeName)
	if err != nil {
		return "", fmt.Errorf("resolve ShiftPVPool for node %q: %w", nodeName, err)
	}
	return pool.MountPath, nil
}

func (r *Runner) poolRootForIdentity(ctx context.Context, identity volume.CopyIdentity) (string, error) {
	if r.Pools == nil {
		return "", fmt.Errorf("ShiftPVPool registry is required")
	}
	pool, err := r.Pools.PoolForIdentity(ctx, identity.PoolName, identity.PoolUID, identity.NodeName)
	if err != nil {
		return "", fmt.Errorf("resolve ShiftPVPool %q for copy: %w", identity.PoolName, err)
	}
	return pool.MountPath, nil
}

func classifyKubernetesAPIError(err error) error {
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) {
		return retryableError{err: err}
	}
	return err
}

func boolPtr(value bool) *bool    { return &value }
func int64Ptr(value int64) *int64 { return &value }
