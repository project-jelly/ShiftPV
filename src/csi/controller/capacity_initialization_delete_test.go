package controller

import (
	"context"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func TestDeletingUnstartedCreationRetainsReservationWithoutBlockingOtherPools(t *testing.T) {
	ctx := context.Background()
	api, registry, newService, _ := initializationFixture(t)
	request := validCreateRequest("worker-a")
	id, _ := volume.IDFromName(request.Name)
	interrupted := false
	api.PrependReactor("update", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		object := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if a.GetSubresource() == "status" && !interrupted {
			interrupted = true
			return true, nil, apierrors.NewTimeoutError("initial status write not stored", 1)
		}
		// FakeDynamicClient does not implement finalizer-aware API deletion.
		if object.GetDeletionTimestamp() != nil && len(object.GetFinalizers()) == 0 {
			return true, object, api.Tracker().Delete(volumeapi.VolumeResource, "", object.GetName())
		}
		return false, nil, nil
	})
	service := newService()
	if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("initialization failure=%v", err)
	}
	initial, err := registry.Get(ctx, id)
	if err != nil || !initial.Initializing() || initial.InitialPool == nil {
		t.Fatalf("initial intent=%+v err=%v", initial, err)
	}
	// Model DELETE's real API boundary: the protected object remains readable
	// with its original UID/spec and a deletionTimestamp, but still no status.
	api.PrependReactor("delete", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		action := a.(k8stesting.DeleteAction)
		object, err := api.Tracker().Get(volumeapi.VolumeResource, "", action.GetName())
		if err != nil {
			return true, nil, err
		}
		current := object.(*unstructured.Unstructured).DeepCopy()
		if action.GetDeleteOptions().Preconditions == nil || action.GetDeleteOptions().Preconditions.UID == nil || *action.GetDeleteOptions().Preconditions.UID != current.GetUID() {
			t.Fatal("DELETE omitted exact UID")
		}
		now := metav1.Now()
		current.SetDeletionTimestamp(&now)
		return true, nil, api.Tracker().Update(volumeapi.VolumeResource, current, "")
	})
	if err := registry.Delete(ctx, id, initial.UID); err != nil {
		t.Fatal(err)
	}
	deleting, err := registry.Get(ctx, id)
	if err != nil || !deleting.Deleting || deleting.Initializing() || deleting.CurrentCopy != nil || deleting.OwnerNode != "" {
		t.Fatalf("deleting intent=%+v err=%v", deleting, err)
	}
	if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("deleting intent resumed: %v", err)
	}
	if service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
		t.Fatal("deleting intent executed")
	}
	assertInitialReservation(t, registry, "pool-a-uid", request.CapacityRange.RequiredBytes)
	assertInitialReservation(t, registry, "pool-b-uid", 0)
	other := validCreateRequest("worker-b")
	other.Name = "pvc-healthy-peer"
	if _, err := newService().CreateVolume(ctx, other); err != nil {
		t.Fatalf("terminating initial reservation blocked healthy Pool: %v", err)
	}
	competitor := validCreateRequest("worker-a")
	competitor.Name = "pvc-over-limit"
	competitor.CapacityRange.RequiredBytes = 1 << 30
	if _, err := newService().CreateVolume(ctx, competitor); err == nil {
		t.Fatal("terminating initial reservation was dropped from capacity admission")
	}
	// No filesystem work was ever authorized for this intent. Removing its
	// protection models final deletion; only then may the logical hold disappear.
	if err := registry.RemoveVolumeFinalizer(ctx, id, initial.UID); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Get(ctx, id); !apierrors.IsNotFound(err) {
		t.Fatalf("intent remained after finalization: %v", err)
	}
	assertInitialReservation(t, registry, "pool-a-uid", 0)
	if _, err := newService().CreateVolume(ctx, competitor); err != nil {
		t.Fatalf("released initial hold still blocked capacity: %v", err)
	}
}

func TestInitialPoolCannotMaskIncompleteExecutionIdentity(t *testing.T) {
	ctx := context.Background()
	api, _, newService, probe := initializationFixture(t)
	id, _ := volume.IDFromName("pvc-damaged")
	_, err := api.Resource(volumeapi.VolumeResource).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": id, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": id, "requestName": "pvc-damaged", "initialNode": "worker-a", "capacityBytes": int64(64 << 20), "initialPool": map[string]any{"name": "pool-a", "uid": "pool-a-uid"}},
		"status":   map[string]any{"phase": volumeapi.PhasePending},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service := newService()
	if _, err := service.CreateVolume(ctx, validCreateRequest("worker-b")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("damaged execution identity was treated as transient or admitted: %v", err)
	}
	if probe.callCount() != 0 || service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
		t.Fatal("unknown ownership allowed capacity admission/effects")
	}
}
