package node

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

type failingAuthorityRead struct {
	*fakeVolumeRegistry
	reads   int
	failure error
}

func (r *failingAuthorityRead) Get(ctx context.Context, id string) (volumeapi.State, error) {
	r.reads++
	if r.reads == 2 && r.failure != nil {
		return volumeapi.State{}, r.failure
	}
	return r.fakeVolumeRegistry.Get(ctx, id)
}

func TestPublicationAuthorityReadFailureRetriesUnderLock(t *testing.T) {
	for name, failure := range map[string]error{
		"API timeout":     apierrors.NewTimeoutError("injected authority GET timeout", 1),
		"context timeout": context.DeadlineExceeded,
	} {
		for _, publish := range []bool{true, false} {
			op := "unpublish"
			if publish {
				op = "publish"
			}
			t.Run(op+"/"+name, func(t *testing.T) {
				binder := &fakeBinder{}
				service := configuredService(t, binder)
				registry := &failingAuthorityRead{fakeVolumeRegistry: service.Volumes.(*fakeVolumeRegistry), failure: failure}
				registry.published = !publish
				service.Volumes = registry
				invoke := func() error {
					if publish {
						_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
						return err
					}
					_, err := service.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
						VolumeId: registry.state.CurrentCopy.VolumeID, TargetPath: validPublishRequest().TargetPath,
					})
					return err
				}
				if err := invoke(); status.Code(err) != codes.Unavailable || registry.reads != 2 {
					t.Fatalf("locked authority read failure lost retry semantics: code=%s reads=%d error=%v", status.Code(err), registry.reads, err)
				}
				if registry.published != !publish || binder.publishedSource != "" {
					t.Fatal("uncertain authority reached publish or changed the publication record")
				}
				registry.reads, registry.failure = 0, nil
				if err := invoke(); err != nil || registry.published != publish {
					t.Fatalf("publication did not converge after API recovery: published=%v error=%v", registry.published, err)
				}
			})
		}
	}
}

func TestRealRegistryPoolReplacementPreservesPublicationContract(t *testing.T) {
	for _, publish := range []bool{true, false} {
		t.Run(map[bool]string{true: "publish", false: "unpublish"}[publish], func(t *testing.T) {
			binder := &fakeBinder{}
			service := configuredService(t, binder)
			registry := service.Volumes.(*fakeVolumeRegistry)
			registry.published = !publish
			copy := *registry.state.CurrentCopy
			pool := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
				"metadata": map[string]any{"name": copy.PoolName, "uid": "replacement-pool-uid"},
				"spec":     map[string]any{"nodeName": copy.NodeName, "mountPath": "/pool"},
			}}
			service.Pools = &volumeapi.Registry{Client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), pool)}
			if publish {
				_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
				if status.Code(err) != codes.FailedPrecondition || binder.publishedSource != "" {
					t.Fatalf("replaced Pool was not rejected before publish: code=%s error=%v", status.Code(err), err)
				}
			} else {
				request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: validPublishRequest().TargetPath}
				if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil || binder.unpublished != request.TargetPath {
					t.Fatalf("replaced Pool blocked target teardown: target=%q error=%v", binder.unpublished, err)
				}
			}
			if registry.reconcileCalls.Load() != 0 || registry.published != !publish {
				t.Fatal("replaced Pool changed the old copy's publication authority")
			}
		})
	}
}

func TestReadOnlyPublishReportsPreconditionAndReconcilesIntent(t *testing.T) {
	binder := &fakeBinder{publishErr: &os.PathError{Op: "mount", Path: "/pool", Err: syscall.EROFS}}
	service := configuredService(t, binder)
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("read-only publish code=%s error=%v", status.Code(err), err)
	}
	registry := service.Volumes.(*fakeVolumeRegistry)
	if registry.published || registry.reconcileCalls.Load() != 1 {
		t.Fatal("failed bind left a false publication intent")
	}
	if status.Code(publicationError("unpublish", syscall.EROFS, false)) != codes.Unavailable {
		t.Fatal("unreached unpublish reconcile lost retry semantics")
	}
	joined := errors.Join(volumeapi.ErrStateConflict, apierrors.NewTimeoutError("uncertain authority", 1))
	if status.Code(publicationError("publish", joined, true)) != codes.Unavailable {
		t.Fatal("an observation timeout was hidden by a wrapped state conflict")
	}
}
