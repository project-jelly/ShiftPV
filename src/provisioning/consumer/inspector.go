package consumer

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/storage/ephemeral"
)

type Inspector struct{ Reader Reader }

func (i Inspector) Inspect(ctx context.Context, req Request) (Result, error) {
	if req.Namespace == "" || req.Name == "" || req.UID == "" || req.Node == "" {
		return Result{Unknown, "PVC metadata is incomplete"}, nil
	}
	if i.Reader == nil {
		return Result{}, fmt.Errorf("consumer reader is not configured")
	}
	pvc, err := i.Reader.Claim(ctx, req.Namespace, req.Name)
	if apierrors.IsNotFound(err) {
		return Result{Unknown, "PVC is not observable"}, nil
	}
	if err != nil {
		return Result{}, err
	}
	if string(pvc.UID) != req.UID || pvc.Annotations[SelectedNode] != req.Node || pvc.DeletionTimestamp != nil || pvc.Spec.VolumeName != "" {
		return Result{}, ErrIdentity
	}
	if _, ok := pvc.Annotations[PopulatorKind]; ok {
		return i.prime(ctx, pvc, req)
	}
	pods, err := i.Reader.Pods(ctx, req.Namespace)
	if err != nil {
		return Result{}, err
	}
	placement := placementForPods(pods, pvc.Name, req.Node)
	if placement.Placement == Fixed {
		return placement, nil
	}

	return i.podOwnerPlacement(ctx, pvc, req, placement)
}

func (i Inspector) podOwnerPlacement(ctx context.Context, pvc *corev1.PersistentVolumeClaim, req Request, placement Result) (Result, error) {
	// Pod ownership is a lifecycle relationship, not an index of all consumers.
	// An unresolved Pod-owned claim remains uncertain while its owner is created/bound.
	for _, owner := range pvc.OwnerReferences {
		if owner.APIVersion != "v1" || owner.Kind != "Pod" {
			continue
		}
		pod, err := i.Reader.Pod(ctx, req.Namespace, owner.Name)
		if apierrors.IsNotFound(err) {
			return Result{Unknown, "Pod owner is not observable"}, nil
		}
		if err != nil {
			return Result{}, err
		}
		if pod.UID != owner.UID || !usesClaim(*pod, pvc.Name) {
			return Result{Unknown, "Pod owner identity or claim reference is unproven"}, nil
		}
		if pod.Spec.NodeName == "" && !schedulerOwnsEphemeral(*pod, pvc) {
			return Result{Unknown, "Pod owner assignment is pending"}, nil
		}
	}
	return placement, nil
}

func (i Inspector) prime(ctx context.Context, pvc *corev1.PersistentVolumeClaim, req Request) (Result, error) {
	kind := pvc.Annotations[PopulatorKind]
	if kind != "VolumeImportSource" && kind != "VolumeUploadSource" {
		return Result{Unknown, "populator placement contract is unknown"}, nil
	}
	owner := metav1.GetControllerOf(pvc)
	if owner == nil || owner.APIVersion != "v1" || owner.Kind != "PersistentVolumeClaim" {
		return Result{Unknown, "prime PVC parent is unproven"}, nil
	}
	parent, err := i.Reader.Claim(ctx, req.Namespace, owner.Name)
	if apierrors.IsNotFound(err) {
		return Result{Unknown, "prime PVC parent is not observable"}, nil
	}
	if err != nil {
		return Result{}, err
	}
	source := parent.Spec.DataSourceRef
	if parent.UID != owner.UID || parent.DeletionTimestamp != nil || parent.Annotations[SelectedNode] != req.Node || source == nil || source.APIGroup == nil || *source.APIGroup != "cdi.kubevirt.io" || source.Kind != kind {
		return Result{}, ErrIdentity
	}
	if prime := parent.Annotations[PrimeName]; prime != "" && prime != pvc.Name {
		return Result{}, ErrIdentity
	}
	return Result{Fixed, "CDI prime inherits its parent PVC node before consumer creation"}, nil
}

func usesClaim(pod corev1.Pod, name string) bool {
	for _, v := range pod.Spec.Volumes {
		if v.Ephemeral != nil && ephemeral.VolumeClaimName(&pod, &v) == name {
			return true
		}
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == name {
			return true
		}
	}
	return false
}

func placementForPods(pods []corev1.Pod, name, node string) Result {
	result := Result{Reschedulable, "consumer remains scheduler-managed"}
	conflict := false
	for _, pod := range pods {
		if !usesClaim(pod, name) || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Spec.NodeName == node {
			result = Result{Fixed, "consumer is assigned to selected node"}
		}
		if pod.Spec.NodeName != "" && pod.Spec.NodeName != node {
			conflict = true
		}
	}
	if conflict {
		return Result{Unknown, "consumer is assigned to another node"}
	}
	return result
}

func schedulerOwnsEphemeral(pod corev1.Pod, pvc *corev1.PersistentVolumeClaim) bool {
	if ephemeral.VolumeIsForPod(&pod, pvc) != nil {
		return false
	}
	for _, v := range pod.Spec.Volumes {
		if v.Ephemeral != nil && ephemeral.VolumeClaimName(&pod, &v) == pvc.Name {
			return true
		}
	}
	return false
}
