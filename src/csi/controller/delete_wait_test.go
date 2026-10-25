package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/retry"
)

func TestDeleteVolumeRetriesExhaustedPoolScanConflicts(t *testing.T) {
	ctx := context.Background()
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
	registry := &retryDeleteVolumeRegistry{state: volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName,
		CurrentCopy: &copy, CapacityBytes: 64 << 20}, exists: true, deleteCalls: 1}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds,
		cleanupParentVolume(copy.VolumeID, copy), cleanupPool(copy, true))
	conflicts := true
	updates := 0
	api.PrependReactor("update", "shiftpvpools", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		updates++
		object := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if conflicts {
			return true, nil, apierrors.NewConflict(volumeapi.PoolResource.GroupResource(), object.GetName(), errors.New("object has been modified"))
		}
		object.SetGeneration(object.GetGeneration() + 1)
		return false, nil, nil
	})
	cleanups := &cleanupapi.Store{Client: api}
	operator := &verifyingCleanupOperator{}
	service := &Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: operator}
	request := &csi.DeleteVolumeRequest{VolumeId: copy.VolumeID}
	if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("exhausted API conflict code=%s error=%v", status.Code(err), err)
	}
	if updates != retry.DefaultBackoff.Steps || !registry.exists || registry.finalizersRemoved != 0 || registry.deleteCalls != 1 {
		t.Fatalf("conflict bypassed retry or cleanup proof: updates=%d registry=%#v", updates, registry)
	}
	authority := cleanupapi.Authority{Kind: "ShiftPVVolume", Name: copy.VolumeID, UID: copy.VolumeUID}
	journal, err := cleanups.Get(ctx, authority)
	if err != nil || journal.Status.Phase != cleanupapi.PhaseVerifying || journal.Status.AbsenceProof != nil || journal.Status.Receipt == nil {
		t.Fatalf("conflict changed cleanup evidence: journal=%#v error=%v", journal, err)
	}
	conflicts = false
	if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale inventory accepted: %v", err)
	}
	if !registry.exists || registry.finalizersRemoved != 0 {
		t.Fatal("volume metadata removed before fresh absence proof")
	}
	pool, err := api.Resource(volumeapi.PoolResource).Get(ctx, copy.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(pool.Object, []any{}, "status", "inventory", "copies"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.DeleteVolume(ctx, request); err != nil {
		t.Fatal(err)
	}
	if registry.exists || registry.finalizersRemoved != 1 || operator.calls != 1 {
		t.Fatalf("retry did not settle exactly once: registry=%#v effects=%d", registry, operator.calls)
	}
}

// Use the real CAS registry through a fake API, so publication races are not
// hidden by a fake BeginDelete implementation that mirrors CSI classification.
func TestDeleteWaitsForPublicationWithoutStartingCleanup(t *testing.T) {
	ctx := context.Background()
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: id, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
	for _, tc := range []struct {
		name, phase, move, owner string
		published                []any
		want                     codes.Code
	}{
		{"unpublish pending", volumeapi.PhaseReady, "", "worker-a", []any{"worker-a"}, codes.Unavailable},
		{"active move", volumeapi.PhaseReady, "move-a", "worker-a", []any{"worker-a"}, codes.FailedPrecondition},
		{"wrong owner", volumeapi.PhaseReady, "", "worker-b", []any{"worker-a"}, codes.FailedPrecondition},
		{"blocked", volumeapi.PhaseBlocked, "", "worker-a", []any{"worker-a"}, codes.FailedPrecondition},
		{"foreign publication", volumeapi.PhaseReady, "", "worker-a", []any{"worker-b"}, codes.FailedPrecondition},
		{"multiple publications", volumeapi.PhaseReady, "", "worker-a", []any{"worker-a", "worker-b"}, codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := cleanupParentVolume(id, copy)
			object.Object["status"] = map[string]any{"phase": tc.phase, "ownerNode": tc.owner, "activeMove": tc.move, "publishedNodes": tc.published}
			identity, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
			object.Object["status"].(map[string]any)["currentCopy"] = identity
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, object)
			registry := &volumeapi.Registry{Client: client}
			cleanups := &cleanupapi.Store{Client: client}
			effect := &receiptCleanupOperator{}
			s := &Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry, Cleanups: cleanups, CleanupOperator: effect}
			req := &csi.DeleteVolumeRequest{VolumeId: id}
			if _, err := s.DeleteVolume(ctx, req); status.Code(err) != tc.want {
				t.Fatalf("code=%v want=%v error=%v", status.Code(err), tc.want, err)
			}
			state, err := registry.Get(ctx, id)
			if err != nil || state.Phase != tc.phase || state.DeletionOperationID != "" || effect.calls != 0 {
				t.Fatalf("wait changed lifecycle: %#v err=%v effects=%d", state, err, effect.calls)
			}
			items, err := cleanups.List(ctx)
			if err != nil || len(items) != 0 {
				t.Fatalf("cleanup started before unpublish: %v %v", items, err)
			}
			if tc.want != codes.Unavailable {
				return
			}
			if err := registry.ReconcilePublished(ctx, id, copy.NodeName, copy, false); err != nil {
				t.Fatal(err)
			}
			if _, err := s.DeleteVolume(ctx, req); err != nil {
				t.Fatal(err)
			}
			if effect.calls != 1 {
				t.Fatalf("effect count=%d", effect.calls)
			}
			if _, err := client.Resource(volumeapi.VolumeResource).Get(ctx, id, metav1.GetOptions{}); err == nil {
				t.Fatal("settled volume remains")
			}
			if _, err := s.DeleteVolume(ctx, req); err != nil || effect.calls != 1 {
				t.Fatalf("completed deletion retry repeated effects: %v calls=%d", err, effect.calls)
			}
		})
	}
}
