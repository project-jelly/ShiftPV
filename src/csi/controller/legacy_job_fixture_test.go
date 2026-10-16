package controller

import (
	"context"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// Seed the durable binding and Job shape left by an older binary. No candidate
// execution API is used to construct a backend that it no longer starts.
func seedLegacyCleanupJob(t *testing.T, store *cleanupapi.Store, kube *fake.Clientset, current cleanupapi.Cleanup, namespace string) {
	t.Helper()
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: current.Name + "-effect", Namespace: namespace,
		UID: types.UID(current.Name + "-job-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1",
			Kind: current.Spec.Authority.Kind, Name: current.Spec.Authority.Name, UID: types.UID(current.Spec.Authority.UID), Controller: &controller}}},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{NodeName: current.Spec.Target.NodeName,
			Containers: []corev1.Container{{Name: "cleanup", Image: "historical-helper"}}, RestartPolicy: corev1.RestartPolicyNever}}}}
	stored, err := kube.BatchV1().Jobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next := current.Status
	next.Phase = cleanupapi.PhaseRunning
	next.Executor = &cleanupapi.Executor{JobName: stored.Name, JobUID: string(stored.UID), NodeName: current.Spec.Target.NodeName}
	if err := store.UpdateStatus(context.Background(), current, next); err != nil {
		t.Fatal(err)
	}
}
