package creationcancellation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/lifecycle/creationcontroller"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/volume"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// This is an API/identity test, not a native filesystem test. The admin client
// owns only fixture namespaces/PVCs; the actor exercises driver permissions.
func testClaimProvenance(t *testing.T, actorConfig *rest.Config, api dynamic.Interface, client kubernetes.Interface) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	adminConfig := rest.CopyConfig(actorConfig)
	adminConfig.Impersonate = rest.ImpersonationConfig{}
	admin, err := kubernetes.NewForConfig(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "shiftpv-claim-proof-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		uid := namespace.UID
		if err := admin.CoreV1().Namespaces().Delete(cleanupCtx, namespace.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextTimeout(cleanupCtx, 100*time.Millisecond, 25*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := admin.CoreV1().Namespaces().Get(ctx, namespace.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Error(err)
		}
	})
	emptyClass := ""
	claimObject := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace.Name, Name: "data", Annotations: map[string]string{consumer.SelectedNode: "unassigned-test-node"}}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &emptyClass, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}}}
	original, err := admin.CoreV1().PersistentVolumeClaims(namespace.Name).Create(ctx, claimObject, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	inspector := creation.ClaimInspector{Reader: consumer.KubernetesReader{Client: client}}
	request := creation.ClaimRequest{Namespace: namespace.Name, Name: original.Name, RequestName: "pvc-" + string(original.UID), NodeName: "unassigned-test-node"}
	verified, err := inspector.Inspect(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	registry := &volumeapi.Registry{Client: api}
	effects := noEffects{t}
	s := &controllercsi.Service{Client: client, Namespace: "shiftpv-system", Volumes: registry, Operator: effects, CleanupOperator: effects, Cleanups: &cleanupapi.Store{Client: api}}
	createIntent := func(requestName string, reference *volumeapi.ClaimReference) *unstructured.Unstructured {
		id, err := volume.IDFromName(requestName)
		if err != nil {
			t.Fatal(err)
		}
		object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": id, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}}, "spec": map[string]any{"volumeID": id, "requestName": requestName, "initialNode": "unassigned-test-node", "capacityBytes": int64(64)}}}
		if reference != nil {
			object.Object["spec"].(map[string]any)["creationClaim"] = map[string]any{"namespace": reference.Namespace, "name": reference.Name, "uid": reference.UID}
		}
		created, err := api.Resource(volumeapi.VolumeResource).Create(ctx, object, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			current, err := registry.Get(cleanupCtx, id)
			if apierrors.IsNotFound(err) {
				return
			}
			if err != nil || current.UID != string(created.GetUID()) {
				t.Errorf("fixture identity changed: %v", err)
				return
			}
			if _, err := s.DeleteVolume(cleanupCtx, &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
				t.Error(err)
			}
		})
		return created
	}
	created := createIntent(request.RequestName, verified)
	state, err := registry.Get(ctx, created.GetName())
	if err != nil || state.CreationClaim == nil || *state.CreationClaim != *verified || !state.UnstartedCreation() {
		t.Fatalf("API pruned or changed first-intent claim: state=%+v err=%v", state, err)
	}
	for _, change := range []string{"name", "uid", "remove"} {
		current, err := api.Resource(volumeapi.VolumeResource).Get(ctx, created.GetName(), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if change == "remove" {
			unstructured.RemoveNestedField(current.Object, "spec", "creationClaim")
		} else {
			_ = unstructured.SetNestedField(current.Object, "replacement", "spec", "creationClaim", change)
		}
		if _, err := api.Resource(volumeapi.VolumeResource).Update(ctx, current, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
			t.Fatalf("API accepted original claim %s change: %v", change, err)
		}
	}
	uid := original.UID
	if err := admin.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, original.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := admin.CoreV1().PersistentVolumeClaims(namespace.Name).Get(ctx, original.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	replacement, err := admin.CoreV1().PersistentVolumeClaims(namespace.Name).Create(ctx, claimObject, metav1.CreateOptions{})
	if err != nil || replacement.UID == original.UID {
		t.Fatalf("PVC recreation failed: %v", err)
	}
	if _, err := inspector.Inspect(ctx, request); !errors.Is(err, creation.ErrClaimIdentity) {
		t.Fatalf("same-name replacement was adopted: %v", err)
	}
	state, err = registry.Get(ctx, created.GetName())
	if err != nil || state.CreationClaim == nil || state.CreationClaim.UID != string(original.UID) {
		t.Fatal("PVC recreation rewrote original reference")
	}
	legacy := createIntent("pvc-"+string(replacement.UID), nil)
	if err := unstructured.SetNestedMap(legacy.Object, map[string]any{"namespace": namespace.Name, "name": replacement.Name, "uid": string(replacement.UID)}, "spec", "creationClaim"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.Resource(volumeapi.VolumeResource).Update(ctx, legacy, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("API allowed provenance to be added after creation: %v", err)
	}
	unknown, err := registry.Get(ctx, legacy.GetName())
	if err != nil || unknown.CreationClaim != nil {
		t.Fatal("legacy unknown provenance changed")
	}
	// Check exact actor identities before cleanup; no status or receipt is made.
	if state.UID != string(created.GetUID()) || !unknown.UnstartedCreation() {
		t.Fatal("provenance check changed creation approval")
	}
	retirement := &creationcontroller.Reconciler{Volumes: registry,
		Claims: creation.RetirementInspector{Reader: creation.KubernetesRetirementReader{Client: client}},
		Cancel: &creation.Canceller{Volumes: registry},
	}
	if complete, err := retirement.Reconcile(ctx, created.GetName(), state.UID); !complete || err != nil {
		t.Fatalf("automatic retirement did not settle original spec-only intent: %v %v", complete, err)
	}
	if _, err := registry.Get(ctx, created.GetName()); !apierrors.IsNotFound(err) {
		t.Fatalf("original reservation remained after retirement: %v", err)
	}
	if complete, err := retirement.Reconcile(ctx, legacy.GetName(), unknown.UID); !complete || err != nil {
		t.Fatal(complete, err)
	}
	if preserved, err := registry.Get(ctx, legacy.GetName()); err != nil || preserved.UID != unknown.UID || !preserved.UnstartedCreation() {
		t.Fatalf("retirement adopted unknown history: %+v %v", preserved, err)
	}
	t.Log("PASS real API preserved immutable provenance and automatically retired only the ended original incarnation without effects or a future CSI retry")
}
