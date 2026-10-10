package controller

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

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
