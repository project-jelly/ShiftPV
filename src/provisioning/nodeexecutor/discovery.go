package nodeexecutor

import (
	"context"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const capabilityAnnotation = "shiftpv.io/node-effects"

// Discovery trusts the configured DaemonSet incarnation, rather than a label
// or a node name alone. Missing old Nodes select the helper before binding.
type Discovery struct {
	Client               kubernetes.Interface
	Namespace, DaemonSet string
}

func (d Discovery) Find(ctx context.Context, node string) (*volumeapi.NodeExecutor, error) {
	if d.DaemonSet == "" {
		return nil, nil
	}
	ds, err := d.Client.AppsV1().DaemonSets(d.Namespace).Get(ctx, d.DaemonSet, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	pods, err := d.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return nil, err
	}
	var result *volumeapi.NodeExecutor
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != node || !ownedExecutor(pod, ds) {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("multiple resident executors on node %s", node)
		}
		result = &volumeapi.NodeExecutor{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), NodeName: pod.Spec.NodeName}
	}
	return result, nil
}
func ownedExecutor(pod corev1.Pod, ds *appsv1.DaemonSet) bool {
	owner := metav1.GetControllerOf(&pod)
	return readyExecutorPod(pod) && pod.Namespace == ds.Namespace && pod.Spec.ServiceAccountName == ds.Spec.Template.Spec.ServiceAccountName && owner != nil && owner.APIVersion == "apps/v1" && owner.Kind == "DaemonSet" && owner.Name == ds.Name && owner.UID == ds.UID
}
func readyExecutorPod(pod corev1.Pod) bool {
	if pod.UID == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Annotations[capabilityAnnotation] != "v1" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (d Discovery) Retired(ctx context.Context, executor volumeapi.NodeExecutor) (bool, error) {
	pod, err := d.Client.CoreV1().Pods(executor.Namespace).Get(ctx, executor.PodName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return string(pod.UID) != executor.PodUID || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded, nil
}
