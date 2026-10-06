package controller

import (
	"context"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) hasScheduledPodConsumer(ctx context.Context, parameters map[string]string, requestName, nodeName string) (bool, error) {
	name, namespace := parameters[PVCNameKey], parameters[PVCNamespaceKey]
	if name == "" || namespace == "" {
		return false, nil
	}
	pvc, err := s.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if requestName != "pvc-"+string(pvc.UID) || pvc.Annotations[selectedNodeAnnotation] != nodeName {
		return false, nil
	}
	for _, owner := range pvc.OwnerReferences {
		if owner.APIVersion != "v1" || owner.Kind != "Pod" || owner.Name == "" {
			continue
		}
		pod, err := s.Client.CoreV1().Pods(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if pod.UID != owner.UID || pod.Spec.NodeName != nodeName {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name {
				return true, nil
			}
		}
	}
	return false, nil
}
