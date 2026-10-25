package controller

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestPreparationPlacementSurvivesOwnerPinSerialization(t *testing.T) {
	for _, changedSelector := range []bool{false, true} {
		t.Run(map[bool]string{false: "API omitted empty selector", true: "different selector remains stale"}[changedSelector], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			w := f.workloads[0]
			consumer, err := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			consumer.Annotations = map[string]string{placementAnnotationKey: "owner"}
			consumer.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "source"}
			if _, err = f.client.CoreV1().Pods(consumer.Namespace).Update(f.ctx, consumer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			// The real API omits an empty nodeSelector in its serialized response.
			// The ordinary fake preserves the empty map and misses this boundary.
			f.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "" {
					return false, nil, nil
				}
				pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
				if pod.Labels["shiftpv.io/role"] != placementRole {
					return false, nil, nil
				}
				if pod.Spec.NodeSelector == nil || len(pod.Spec.NodeSelector) != 0 {
					t.Fatalf("owner pin was not removed to an empty selector: %v", pod.Spec.NodeSelector)
				}
				encoded, err := json.Marshal(pod)
				if err != nil {
					return true, nil, err
				}
				var stored corev1.Pod
				if err = json.Unmarshal(encoded, &stored); err != nil {
					return true, nil, err
				}
				*pod = stored
				return false, nil, nil
			})
			name := namesFor(f.moveNames[0]).PlacementPod
			var placement *corev1.Pod
			for range 6 {
				if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				for _, action := range f.client.Actions() {
					if action.Matches("delete", "pods") {
						t.Fatal("API-normalized placement was deleted before the scheduler could bind it")
					}
				}
				placement, err = f.client.CoreV1().Pods("system").Get(f.ctx, name, metav1.GetOptions{})
				if err == nil {
					break
				}
				if !apierrors.IsNotFound(err) {
					t.Fatal(err)
				}
			}
			if placement == nil || placement.UID == "" || placement.Spec.NodeSelector != nil {
				t.Fatalf("no API-normalized placement: %+v", placement)
			}
			uid := placement.UID
			if changedSelector {
				placement.Spec.NodeSelector = map[string]string{"disk": "different"}
				if _, err = f.client.CoreV1().Pods("system").Update(f.ctx, placement, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			placement, err = f.client.CoreV1().Pods("system").Get(f.ctx, name, metav1.GetOptions{})
			if changedSelector {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("changed selector was not rejected: %v", err)
				}
			} else if err != nil || placement.UID != uid {
				t.Fatalf("unscheduled placement identity changed: pod=%+v err=%v", placement, err)
			}
			if f.move(t, 0).Status.Preparation != nil || f.state(t, 0).Phase != volumeapi.PhaseReady {
				t.Fatal("unscheduled placement acquired a hold or disrupted the source")
			}
			move := f.untilPrepared(t, 0)
			if (move.Status.PlacementPodUID == string(uid)) == changedSelector {
				t.Fatalf("reserved placement UID=%q, previous=%q, selector changed=%t", move.Status.PlacementPodUID, uid, changedSelector)
			}
			assertPreparationOriginals(t, f)
		})
	}
}
