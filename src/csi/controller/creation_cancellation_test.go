package controller

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
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

func TestPendingCreationCanFenceCleanupWithoutBecomingReady(t *testing.T) {
	ctx := context.Background()
	api, registry, newService, _ := initializationFixture(t)
	api.PrependReactor("update", "shiftpvpools", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "" {
			object := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	before, err := registry.BeginCreate(ctx, volumeapi.CreationRequest{VolumeID: id, RequestName: "pvc-cancel", NodeName: "worker-a", CapacityBytes: 64 << 20, Pool: &volumeapi.PoolReference{Name: "pool-a", UID: "pool-a-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	s := newService()
	s.CreationInspector = &helperpod.Inspector{Client: s.Client, Namespace: s.Namespace}
	cleanup := &verifyingCleanupOperator{}
	s.CleanupOperator = cleanup
	if _, err := s.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}); status.Code(err) != codes.Unavailable {
		t.Fatalf("unfinished creation did not reach receipt/absence settlement: %v", err)
	}
	after, err := registry.Get(ctx, id)
	if err != nil || after.UID != before.UID || !after.CancellingCreation() || after.CreationReceipt != nil {
		t.Fatalf("cancellation identity/fence=%+v err=%v", after, err)
	}
	journal, err := s.Cleanups.Get(ctx, cleanupapi.Authority{Kind: "ShiftPVVolume", Name: id, UID: before.UID})
	if err != nil || journal.Spec.Reason != "CreationCancelled" || cleanup.calls != 1 {
		t.Fatalf("cancellation journal=%+v calls=%d err=%v", journal, cleanup.calls, err)
	}
	assertInitialReservation(t, registry, "pool-a-uid", 64<<20)
}

type creationInspectorFunc func(context.Context, volume.CopyIdentity) (bool, error)

func (f creationInspectorFunc) CreationExists(ctx context.Context, copy volume.CopyIdentity) (bool, error) {
	return f(ctx, copy)
}

func TestCreationCancellationPreservesLegacyOrUnknownResponsibility(t *testing.T) {
	for _, test := range []struct {
		name    string
		inspect creationInspectorFunc
		code    codes.Code
	}{
		{"unconfigured", nil, codes.Unavailable},
		{"legacy helper", func(context.Context, volume.CopyIdentity) (bool, error) { return true, nil }, codes.FailedPrecondition},
		{"unknown API observation", func(context.Context, volume.CopyIdentity) (bool, error) {
			return false, apierrors.NewTimeoutError("lookup failed", 1)
		}, codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, registry, newService, _ := initializationFixture(t)
			ctx := context.Background()
			id := "shiftpv-0123456789abcdef0123456789abcdef"
			initial, err := registry.BeginCreate(ctx, volumeapi.CreationRequest{VolumeID: id, RequestName: "pvc-cancel", NodeName: "worker-a", CapacityBytes: 64 << 20, Pool: &volumeapi.PoolReference{Name: "pool-a", UID: "pool-a-uid"}})
			if err != nil {
				t.Fatal(err)
			}
			s := newService()
			if test.inspect != nil {
				s.CreationInspector = test.inspect
			}
			if _, err := s.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}); status.Code(err) != test.code {
				t.Fatalf("DeleteVolume: %v", err)
			}
			after, err := registry.Get(ctx, id)
			if err != nil || after.UID != initial.UID || after.Phase != volumeapi.PhasePending || after.DeletionOperationID != "" || after.HasCleanup || after.CreationReceipt != nil {
				t.Fatalf("unknown responsibility mutated: %+v err=%v", after, err)
			}
			assertInitialReservation(t, registry, "pool-a-uid", 64<<20)
		})
	}
}
