package consumer

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type KubernetesReader struct{ Client kubernetes.Interface }

func (r KubernetesReader) Claim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return r.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}
func (r KubernetesReader) Pod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return r.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}
func (r KubernetesReader) Pods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	list, err := r.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}
