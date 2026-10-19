package controller

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"
)

func terminalSourceFixture(t *testing.T, phase corev1.PodPhase) (*mobilityWalk, *corev1.Pod) {
	t.Helper()
	w := newMobilityWalk()
	w.observeAndLock(t)
	w.ensurePlacementOnDestination(t)
	if err := w.reconciler.ensureCopyResources(w.ctx, w.move, w.observed.Names); err != nil {
		t.Fatal(err)
	}
	pod, err := w.client.CoreV1().Pods("system").Get(w.ctx, w.observed.Names.SourcePod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status = corev1.PodStatus{Phase: phase, Reason: "Evicted", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	pod, err = w.client.CoreV1().Pods("system").UpdateStatus(w.ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	w.client.ClearActions()
	return w, pod
}

func TestTerminalSourcePodIsReplacedBeforeCopyStarts(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		t.Run(string(phase), func(t *testing.T) {
			w, old := terminalSourceFixture(t, phase)
			state := w.repository.volumes[w.volumeID]
			move := w.move
			for pass := 0; pass < 2; pass++ {
				if err := w.reconciler.ensureCopyResources(w.ctx, w.move, w.observed.Names); err != nil {
					t.Fatal(err)
				}
			}
			pod, err := w.client.CoreV1().Pods("system").Get(w.ctx, old.Name, metav1.GetOptions{})
			if err != nil || pod.UID == old.UID || pod.DeletionTimestamp != nil || terminalPod(pod) {
				t.Fatalf("terminal source was not replaced: pod=%+v error=%v", pod, err)
			}
			deleted := false
			for _, action := range w.client.Actions() {
				if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
					options := action.(ktesting.DeleteAction).GetDeleteOptions()
					if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != old.UID {
						t.Fatalf("source deletion lacks its exact UID fence: %+v", options)
					}
					deleted = true
				}
			}
			if !deleted {
				t.Fatal("terminal source deletion was not requested")
			}
			if _, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.CopyJob, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("copy started before replacement source readiness: %v", err)
			}
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			if _, err := w.client.CoreV1().Pods("system").UpdateStatus(w.ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := w.reconciler.ensureCopyResources(w.ctx, w.move, w.observed.Names); err != nil {
				t.Fatal(err)
			}
			if _, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.CopyJob, metav1.GetOptions{}); err != nil {
				t.Fatalf("copy did not resume after source replacement: %v", err)
			}
			if !reflect.DeepEqual(state, w.repository.volumes[w.volumeID]) || !reflect.DeepEqual(move, w.move) {
				t.Fatal("source replacement changed copy authority or the approved Move")
			}
		})
	}
}

func TestTerminalSourcePodDeletionConflictPreservesIt(t *testing.T) {
	w, old := terminalSourceFixture(t, corev1.PodFailed)
	w.client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, old.Name, nil)
	})
	if err := w.reconciler.ensureCopyResources(w.ctx, w.move, w.observed.Names); !apierrors.IsConflict(err) {
		t.Fatalf("source deletion conflict was hidden: %v", err)
	}
	pod, err := w.client.CoreV1().Pods("system").Get(w.ctx, old.Name, metav1.GetOptions{})
	if err != nil || pod.UID != old.UID {
		t.Fatalf("conflicting source was replaced: pod=%+v error=%v", pod, err)
	}
	if _, err := w.client.BatchV1().Jobs("system").Get(w.ctx, w.observed.Names.CopyJob, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("copy started despite source deletion conflict: %v", err)
	}
}

func TestSourceReadinessRejectsForeignReplacementAndDeletingPod(t *testing.T) {
	w, pod := terminalSourceFixture(t, corev1.PodFailed)
	pod.Status.Phase = corev1.PodRunning
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	if _, err := w.client.CoreV1().Pods("system").Update(w.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ready, err := w.reconciler.sourcePodReady(w.ctx, w.move, w.observed.Names)
	if err != nil || ready {
		t.Fatalf("deleting source with stale Ready was accepted: ready=%v error=%v", ready, err)
	}
	pod.DeletionTimestamp = nil
	pod.OwnerReferences[0].UID = "foreign-move-uid"
	if _, err := w.client.CoreV1().Pods("system").Update(w.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ready, err = w.reconciler.sourcePodReady(w.ctx, w.move, w.observed.Names)
	if err == nil || ready || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("foreign ready Pod was accepted after source replacement: ready=%v error=%v", ready, err)
	}
}
