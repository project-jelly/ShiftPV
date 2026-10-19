// Exact identity and execution-shape checks for existing Move resources.
package controller

import (
	"reflect"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sameOperationJob compares the live Job against the desired one section by
// section, in the order identity, Pod spec, container. The first section that
// differs decides the answer, so the sections must stay in this order.
func sameOperationJob(current, expected *batchv1.Job) bool {
	if !sameOperationJobIdentity(current, expected) {
		return false
	}
	actual, wanted := current.Spec.Template.Spec, expected.Spec.Template.Spec
	if !sameOperationPodSpec(actual, wanted) {
		return false
	}
	return sameOperationContainer(actual.Containers[0], wanted.Containers[0])
}

func sameOperationJobIdentity(current, expected *batchv1.Job) bool {
	if current == nil || expected == nil {
		return false
	}
	// A running Job retains the bound it was created with, even if controller
	// configuration changes. Verification Jobs deliberately remain unbounded.
	if expected.Spec.ActiveDeadlineSeconds == nil {
		if current.Spec.ActiveDeadlineSeconds != nil {
			return false
		}
	} else if current.Spec.ActiveDeadlineSeconds == nil || *current.Spec.ActiveDeadlineSeconds <= 0 {
		return false
	}
	return current.DeletionTimestamp == nil &&
		current.Name == expected.Name && current.Namespace == expected.Namespace &&
		reflect.DeepEqual(current.OwnerReferences, expected.OwnerReferences) &&
		current.Labels["shiftpv.io/move-uid"] == expected.Labels["shiftpv.io/move-uid"] &&
		reflect.DeepEqual(current.Spec.BackoffLimit, expected.Spec.BackoffLimit)
}

func sameOperationPodSpec(actual, wanted corev1.PodSpec) bool {
	return actual.NodeName == wanted.NodeName && actual.ServiceAccountName == wanted.ServiceAccountName && actual.RestartPolicy == wanted.RestartPolicy &&
		!actual.HostPID && !actual.HostIPC && !actual.HostNetwork && len(actual.InitContainers) == 0 && len(actual.EphemeralContainers) == 0 &&
		len(actual.Containers) == 1 && len(wanted.Containers) == 1 && reflect.DeepEqual(actual.Volumes, wanted.Volumes)
}

func sameOperationContainer(a, w corev1.Container) bool {
	return a.Name == w.Name && a.Image == w.Image && reflect.DeepEqual(a.Command, w.Command) && reflect.DeepEqual(a.Args, w.Args) &&
		reflect.DeepEqual(a.Env, w.Env) && len(a.EnvFrom) == 0 && reflect.DeepEqual(a.Resources, w.Resources) &&
		reflect.DeepEqual(a.VolumeMounts, w.VolumeMounts) && reflect.DeepEqual(a.SecurityContext, w.SecurityContext) &&
		len(a.VolumeDevices) == 0 && a.Lifecycle == nil && a.LivenessProbe == nil && a.ReadinessProbe == nil && a.StartupProbe == nil
}

func sameMoveObject(current, expected *metav1.ObjectMeta) bool {
	return current != nil && expected != nil && current.DeletionTimestamp == nil &&
		current.Name == expected.Name && current.Namespace == expected.Namespace &&
		current.Labels["shiftpv.io/move"] == expected.Labels["shiftpv.io/move"] &&
		current.Labels["shiftpv.io/move-uid"] == expected.Labels["shiftpv.io/move-uid"] &&
		reflect.DeepEqual(current.OwnerReferences, expected.OwnerReferences)
}

func validTransferSecret(secret *corev1.Secret) bool {
	if secret == nil || len(secret.Data) != 2 || len(secret.Data["password"]) == 0 {
		return false
	}
	return string(secret.Data["secrets"]) == "shiftpv:"+string(secret.Data["password"])+"\n"
}

// sameSourcePod compares the live source Pod against the desired one section by
// section, in the order volumes, Pod spec, container. The first section that
// differs decides the answer, so the sections must stay in this order.
func sameSourcePod(current, expected *corev1.Pod) bool {
	if current == nil || expected == nil {
		return false
	}
	a, w := current.Spec, expected.Spec
	serviceAccountVolume, volumesMatch := sameSourceVolumes(a.Volumes, w.Volumes)
	if !sameSourcePodSpec(a, w, volumesMatch) {
		return false
	}
	return sameSourceContainer(a.Containers[0], w.Containers[0], serviceAccountVolume)
}

func sameSourcePodSpec(a, w corev1.PodSpec, volumesMatch bool) bool {
	return a.NodeName == w.NodeName && a.ServiceAccountName == w.ServiceAccountName && a.RestartPolicy == w.RestartPolicy && !a.HostPID && !a.HostIPC && !a.HostNetwork &&
		len(a.InitContainers) == 0 && len(a.EphemeralContainers) == 0 && len(a.Containers) == 1 && len(w.Containers) == 1 && volumesMatch
}

func sameSourceContainer(ac, wc corev1.Container, serviceAccountVolume string) bool {
	return ac.Name == wc.Name && ac.Image == wc.Image && reflect.DeepEqual(ac.Command, wc.Command) && reflect.DeepEqual(ac.Args, wc.Args) &&
		reflect.DeepEqual(ac.Env, wc.Env) && len(ac.EnvFrom) == 0 && reflect.DeepEqual(ac.Ports, wc.Ports) && reflect.DeepEqual(ac.ReadinessProbe, wc.ReadinessProbe) &&
		ac.ImagePullPolicy == wc.ImagePullPolicy && reflect.DeepEqual(ac.Resources, wc.Resources) &&
		ac.TerminationMessagePath == wc.TerminationMessagePath && ac.TerminationMessagePolicy == wc.TerminationMessagePolicy &&
		reflect.DeepEqual(ac.SecurityContext, wc.SecurityContext) && sameSourceVolumeMounts(ac.VolumeMounts, wc.VolumeMounts, serviceAccountVolume) && len(ac.VolumeDevices) == 0 &&
		ac.Lifecycle == nil && ac.LivenessProbe == nil && ac.StartupProbe == nil
}

func sameSourceVolumes(current, expected []corev1.Volume) (string, bool) {
	if len(current) != len(expected) && len(current) != len(expected)+1 {
		return "", false
	}
	remaining := make(map[string]corev1.Volume, len(current))
	for _, volume := range current {
		if volume.Name == "" {
			return "", false
		}
		remaining[volume.Name] = volume
	}
	for _, wanted := range expected {
		actual, ok := remaining[wanted.Name]
		if !ok || !reflect.DeepEqual(actual, wanted) {
			return "", false
		}
		delete(remaining, wanted.Name)
	}
	if len(remaining) == 0 {
		return "", true
	}
	if len(remaining) != 1 {
		return "", false
	}
	for name, volume := range remaining {
		return name, validServiceAccountProjection(volume)
	}
	return "", false
}

func validServiceAccountProjection(volume corev1.Volume) bool {
	if !strings.HasPrefix(volume.Name, "kube-api-access-") || volume.Projected == nil || len(volume.Projected.Sources) != 3 ||
		volume.Projected.DefaultMode == nil || *volume.Projected.DefaultMode != 0o644 {
		return false
	}
	var token, rootCA, namespace bool
	for _, source := range volume.Projected.Sources {
		switch {
		case source.ServiceAccountToken != nil:
			token = validTokenProjection(source.ServiceAccountToken)
		case source.ConfigMap != nil:
			rootCA = validRootCAProjection(source.ConfigMap)
		case source.DownwardAPI != nil:
			namespace = validNamespaceProjection(source.DownwardAPI)
		default:
			return false
		}
	}
	return token && rootCA && namespace
}

// validTokenProjection reports whether the projected token is the short-lived
// API-server audience token the helper identity contract expects.
func validTokenProjection(projection *corev1.ServiceAccountTokenProjection) bool {
	return projection.Path == "token" && projection.Audience == "" && projection.ExpirationSeconds != nil && *projection.ExpirationSeconds > 0
}

// validRootCAProjection reports whether the projected ConfigMap is exactly the
// cluster root CA bundle at its usual path.
func validRootCAProjection(projection *corev1.ConfigMapProjection) bool {
	return projection.Name == "kube-root-ca.crt" && len(projection.Items) == 1 && projection.Items[0].Key == "ca.crt" && projection.Items[0].Path == "ca.crt"
}

// validNamespaceProjection reports whether the downward API projects exactly the
// Pod's own namespace.
func validNamespaceProjection(projection *corev1.DownwardAPIProjection) bool {
	return len(projection.Items) == 1 && projection.Items[0].Path == "namespace" && projection.Items[0].FieldRef != nil &&
		projection.Items[0].FieldRef.APIVersion == "v1" && projection.Items[0].FieldRef.FieldPath == "metadata.namespace"
}

func sameSourceVolumeMounts(current, expected []corev1.VolumeMount, serviceAccountVolume string) bool {
	if len(current) != len(expected) && (serviceAccountVolume == "" || len(current) != len(expected)+1) {
		return false
	}
	remaining := make(map[string]corev1.VolumeMount, len(current))
	for _, mount := range current {
		if mount.Name == "" {
			return false
		}
		remaining[mount.Name] = mount
	}
	for _, wanted := range expected {
		actual, ok := remaining[wanted.Name]
		if !ok || !reflect.DeepEqual(actual, wanted) {
			return false
		}
		delete(remaining, wanted.Name)
	}
	if serviceAccountVolume == "" {
		return len(remaining) == 0
	}
	mount, ok := remaining[serviceAccountVolume]
	return ok && len(remaining) == 1 && mount.MountPath == "/var/run/secrets/kubernetes.io/serviceaccount" && mount.ReadOnly &&
		mount.SubPath == "" && mount.SubPathExpr == "" && mount.MountPropagation == nil
}

func sameServicePorts(current, expected []corev1.ServicePort) bool {
	if len(current) != len(expected) {
		return false
	}
	for i := range current {
		if current[i].Name != expected[i].Name || current[i].Port != expected[i].Port || current[i].TargetPort != expected[i].TargetPort ||
			(current[i].Protocol != "" && current[i].Protocol != corev1.ProtocolTCP) {
			return false
		}
	}
	return true
}
