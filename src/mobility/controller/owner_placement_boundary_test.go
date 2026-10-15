package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

func TestOwnerPlacementAdmissionHoldRecoversAfterRestartWithoutMove(t *testing.T) {
	ctx := context.Background()
	r, client, repository, original := ownerPlacementFixture()
	wantState := repository.volumes[ownerPlacementVolumeID]
	node, err := client.CoreV1().Nodes().Get(ctx, "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Spec.Unschedulable = true
	if _, err := client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Pods(original.Namespace).Delete(ctx, original.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	requestPod := original.DeepCopy()
	requestPod.UID = ""
	requestPod.Labels = map[string]string{"app": "keep"}
	requestPod.Annotations = map[string]string{"example.test/note": "keep"}
	requestPod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.test/hold"}}
	raw, err := json.Marshal(requestPod)
	if err != nil {
		t.Fatal(err)
	}
	handler := admission.Handler{Client: client, Volumes: repository}
	response := handler.Admit(ctx, &admissionv1.AdmissionRequest{
		UID: "admission-request", Operation: admissionv1.Create, Namespace: requestPod.Namespace,
		Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
		Object:   runtime.RawExtension{Raw: raw},
	})
	if !response.Allowed || response.PatchType == nil || *response.PatchType != admissionv1.PatchTypeJSONPatch {
		t.Fatalf("admission did not return an allowed JSON patch: %+v", response)
	}
	patch, err := jsonpatch.DecodePatch(response.Patch)
	if err != nil {
		t.Fatal(err)
	}
	patched, err := patch.Apply(raw)
	if err != nil {
		t.Fatal(err)
	}
	var held corev1.Pod
	if err := json.Unmarshal(patched, &held); err != nil {
		t.Fatal(err)
	}
	// The fake API does not allocate a UID when it accepts an admitted Pod.
	held.UID = original.UID
	if _, err := client.CoreV1().Pods(held.Namespace).Create(ctx, &held, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !hasPlacementHold(&held) || held.Labels[admission.ManagedLabel] != "true" || held.Annotations[placementAnnotationKey] != "held" {
		t.Fatalf("real admission did not create the recoverable hold: %+v", held)
	}
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasPlacementHold(ownerPlacementBoundaryPod(t, client, original)) || len(repository.moves) != 0 {
		t.Fatal("cordoned owner must retain the hold without creating a Move")
	}
	node.Spec.Unschedulable = false
	if _, err := client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	// Recover only from persisted API objects, with a fresh reconciler instance.
	if err := newTestReconciler(client, repository).ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	got := ownerPlacementBoundaryPod(t, client, original)
	want := requestPod.DeepCopy()
	want.Spec.NodeSelector[corev1.LabelHostname] = "source"
	if got.UID != held.UID || !reflect.DeepEqual(got.Spec, want.Spec) || got.Annotations[placementAnnotationKey] != "owner" ||
		got.Annotations["example.test/note"] != "keep" || got.Labels["app"] != "keep" {
		t.Fatalf("recovery did not preserve the admitted Pod and unrelated constraints: %+v", got)
	}
	if len(repository.moves) != 0 || !reflect.DeepEqual(repository.volumes[ownerPlacementVolumeID], wantState) {
		t.Fatal("owner placement recovery changed storage authority or created a Move")
	}
	assertNoEviction(t, client)
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "delete" {
			t.Fatalf("recovery replaced an API object instead of updating the held Pod: %#v", action)
		}
	}
}

func TestOwnerPlacementConflictRechecksBindingAndServingIdentity(t *testing.T) {
	for _, change := range []string{"unchanged", "claim recreated", "volume recreated"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			r, client, repository, pod := ownerPlacementFixture()
			updates := 0
			client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				updates++
				if updates != 1 {
					return false, nil, nil
				}
				switch change {
				case "claim recreated":
					claimObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), pod.Namespace, "claim")
					if err != nil {
						t.Fatal(err)
					}
					claim := claimObject.(*corev1.PersistentVolumeClaim).DeepCopy()
					pvObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), "", claim.Spec.VolumeName)
					if err != nil {
						t.Fatal(err)
					}
					pv := pvObject.(*corev1.PersistentVolume).DeepCopy()
					// Keep the new binding internally valid: it must still be rejected
					// because the retry was authorized by the previous claim UID.
					claim.UID = "replacement-claim-uid"
					pv.Spec.ClaimRef.UID = claim.UID
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), claim, claim.Namespace); err != nil {
						t.Fatal(err)
					}
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), pv, ""); err != nil {
						t.Fatal(err)
					}
				case "volume recreated":
					state := repository.volumes[ownerPlacementVolumeID]
					copy := *state.CurrentCopy
					state.UID, copy.VolumeUID = "replacement-volume-uid", "replacement-volume-uid"
					state.CurrentCopy = &copy
					repository.volumes[ownerPlacementVolumeID] = state
				}
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, fmt.Errorf("concurrent update"))
			})
			err := r.ReconcileAll(ctx)
			got := ownerPlacementBoundaryPod(t, client, pod)
			if change == "unchanged" {
				if err != nil || updates != 2 || hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "source" {
					t.Fatalf("unchanged authority did not recover on retry: updates=%d, err=%v, pod=%+v", updates, err, got)
				}
				return
			}
			if err == nil || updates != 1 || !hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "" || got.UID != pod.UID {
				t.Fatalf("stale authority released the hold: updates=%d, err=%v, pod=%+v", updates, err, got)
			}
			if len(repository.moves) != 0 {
				t.Fatal("conflict recovery created a Move")
			}
		})
	}
}

func TestOwnerPlacementInvalidBindingDoesNotBlockAnotherPod(t *testing.T) {
	ctx := context.Background()
	r, client, repository, healthy := ownerPlacementFixture()
	bad := healthy.DeepCopy()
	bad.Name, bad.UID = "aaa-invalid-binding", "invalid-pod-uid"
	bad.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "invalid-claim"
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-claim", Namespace: bad.Namespace, UID: "invalid-claim-uid"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "invalid-pv"},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-pv", UID: "invalid-pv-uid"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: admission.DriverName, VolumeHandle: ownerPlacementVolumeID}},
			ClaimRef:               &corev1.ObjectReference{Namespace: bad.Namespace, Name: claim.Name, UID: "old-claim-uid"},
		},
	}
	for _, object := range []runtime.Object{bad, claim, pv} {
		if err := client.Tracker().Add(object); err != nil {
			t.Fatal(err)
		}
	}
	err := r.ReconcileAll(ctx)
	if err == nil || !strings.Contains(err.Error(), bad.Name) {
		t.Fatalf("invalid binding was not reported: %v", err)
	}
	if hasPlacementHold(ownerPlacementBoundaryPod(t, client, healthy)) || !hasPlacementHold(ownerPlacementBoundaryPod(t, client, bad)) {
		t.Fatal("one invalid binding prevented healthy recovery or released the invalid hold")
	}
	if len(repository.moves) != 0 {
		t.Fatal("owner placement recovery created a Move")
	}
}

func ownerPlacementBoundaryPod(t *testing.T, client *fake.Clientset, identity *corev1.Pod) *corev1.Pod {
	t.Helper()
	pod, err := client.CoreV1().Pods(identity.Namespace).Get(context.Background(), identity.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod
}
