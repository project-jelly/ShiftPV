package helperpod

import (
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// fieldCheck is one row of an ordered comparison table: the name a mismatch is
// reported under, and the comparison itself. Rows are evaluated in order and
// stop at the first false one, so a later row may rely on an earlier row having
// fixed a length or excluded a nil.
type fieldCheck struct {
	field string
	same  func() bool
}

// firstDifference returns the name of the first row that does not hold, or ""
// when every row holds.
func firstDifference(checks []fieldCheck) string {
	for _, check := range checks {
		if !check.same() {
			return check.field
		}
	}
	return ""
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

func helperPodTermination(pod *corev1.Pod) (*corev1.ContainerStateTerminated, error) {
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return nil, nil
	}
	if len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].Name != "operation" ||
		pod.Status.ContainerStatuses[0].State.Terminated == nil {
		return nil, retryableError{err: fmt.Errorf("helper Pod lacks termination evidence")}
	}
	return pod.Status.ContainerStatuses[0].State.Terminated, nil
}

func isAmbiguousKubernetesError(err error) bool {
	return apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err)
}
