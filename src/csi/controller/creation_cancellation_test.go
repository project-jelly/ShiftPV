package controller

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func TestUnstartedCreationCanSettleWithoutFilesystemEffects(t *testing.T) {
	ctx := context.Background()
	api, registry, newService, _ := initializationFixture(t)
	api.PrependReactor("create", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		object := a.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		object.SetResourceVersion("1")
		return false, nil, nil
	})
	api.PrependReactor("update", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "status" {
			return true, nil, apierrors.NewTimeoutError("before first status write", 1)
		}
		object := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if object.GetDeletionTimestamp() != nil && len(object.GetFinalizers()) == 0 {
			return true, object, api.Tracker().Delete(volumeapi.VolumeResource, "", object.GetName())
		}
		return false, nil, nil
	})
	service := newService()
	request := validCreateRequest("worker-a")
	if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	volumes, err := registry.ListVolumes(ctx)
	if err != nil || len(volumes) != 1 {
		t.Fatalf("initial intent=%v err=%v", volumes, err)
	}
	var id string
	for name := range volumes {
		id = name
	}
	state := volumes[id]
	if !state.HasInitialReservation() {
		t.Fatalf("not an unexecuted reservation: %+v", state)
	}
	api.PrependReactor("delete", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		action := a.(k8stesting.DeleteAction)
		options := action.GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != state.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "1" {
			t.Fatal("unstarted DELETE omitted exact UID and resourceVersion")
		}
		object, err := api.Tracker().Get(volumeapi.VolumeResource, "", action.GetName())
		if err != nil {
			return true, nil, err
		}
		current := object.(*unstructured.Unstructured).DeepCopy()
		now := metav1.Now()
		current.SetDeletionTimestamp(&now)
		return true, nil, api.Tracker().Update(volumeapi.VolumeResource, current, "")
	})
	if _, err := newService().DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
		t.Fatalf("unstarted cancellation has no exit: %v", err)
	}
	if _, err := registry.Get(ctx, id); !apierrors.IsNotFound(err) {
		t.Fatalf("unstarted reservation remained: %v", err)
	}
	assertInitialReservation(t, registry, "pool-a-uid", 0)
	if service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
		t.Fatal("cancellation invented a creation effect")
	}
}
