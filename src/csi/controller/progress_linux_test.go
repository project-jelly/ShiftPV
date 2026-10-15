//go:build linux

package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/observation"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Inject the transport/executor boundary. CSI, the registry, local ownership,
// purge, scanner, readiness writer and causal cleanup journal stay real.
// A legacy Pool avoids requiring a privileged block device in this test.
type localProgressEffects struct {
	root            string
	volumes         *volumeapi.Registry
	creates, purges int
}

func (e *localProgressEffects) authority(ctx context.Context, copy volume.CopyIdentity, deleting bool) error {
	state, err := e.volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	if state.CurrentCopy == nil || *state.CurrentCopy != copy || (state.Phase == volumeapi.PhaseDeleting) != deleting {
		return volumeapi.ErrStateConflict
	}
	return nil
}

func (e *localProgressEffects) CreateCopy(ctx context.Context, copy volume.CopyIdentity) error {
	e.creates++
	return ownership.PrepareServing(ctx, e.root, copy, func(ctx context.Context) error { return e.authority(ctx, copy, false) })
}

func (*localProgressEffects) FinalizeCreate(context.Context, volume.CopyIdentity) error { return nil }

func (e *localProgressEffects) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, journal helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	// This in-process executor has already settled when it returns a receipt.
	// Re-entry observes that result; it must not start another destructive run.
	if cleanup.Status.Phase == cleanupapi.PhaseVerifying || cleanup.Status.Phase == cleanupapi.PhaseConfirmingAbsence {
		return cleanup, nil
	}
	if _, err := e.volumes.CleanupPoolForIdentity(ctx, cleanup.Spec.Target.PoolName, cleanup.Spec.Target.PoolUID, cleanup.Spec.Target.NodeName); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	executor := &cleanupapi.Executor{JobName: "job", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	if err := journal.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	running, err := journal.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	local, digest, err := ownership.ReclaimWithResume(ctx, e.root, cleanup.Spec.Target, cleanup.Spec.OperationID, func(ctx context.Context, _ bool) error {
		return e.authority(ctx, cleanup.Spec.Target, true)
	})
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	e.purges++
	receipt := &cleanupapi.Receipt{OperationID: local.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: local.Retired, Purged: local.Purged, LocalReceiptDigest: digest}
	if err := journal.UpdateStatus(ctx, running, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	return journal.Get(ctx, cleanup.Spec.Authority)
}

type unpublishedProgressCopies struct{}

func (unpublishedProgressCopies) HasPublishedTarget(string, string) (bool, error) { return false, nil }

type heldProgressObservation struct {
	*volumeapi.Registry
	status volumeapi.PoolStatus
}

func (h *heldProgressObservation) SetPoolStatus(_ context.Context, _, _, _ string, status volumeapi.PoolStatus) error {
	h.status = status
	return nil
}

func TestSharedPoolCleanupFencesConvergeAfterObservationRace(t *testing.T) {
	ctx := context.Background()
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool", "uid": "pool-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "worker-a", "mountPath": "/pool", "poolGroup": "default", "capacity": map[string]any{"limit": "128Mi"}},
	}}
	installation := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"}}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, pool, installation)
	// Model API-assigned UID and spec generation. Status writes never bump it.
	api.PrependReactor("create", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		obj := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		obj.SetUID(types.UID("uid-" + obj.GetName()[len(obj.GetName())-8:]))
		return false, nil, nil
	})
	api.PrependReactor("update", "shiftpvpools", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			obj := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			obj.SetGeneration(obj.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	registry := &volumeapi.Registry{Client: api}
	scanner := &observation.Scanner{HostRoot: host, TargetRoot: "/pods", Installation: registry, Publications: unpublishedProgressCopies{}, Limit: 32}
	node, err := readiness.NewReconciler("worker-a", registry, readiness.NewProbe(host), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory = scanner.Scan
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	effects := &localProgressEffects{root: root, volumes: registry}
	cleanups := &cleanupapi.Store{Client: api}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv", Operator: effects, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: effects, CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}}})
	create := func(name string) string {
		t.Helper()
		r := validCreateRequest("worker-a")
		r.Name = name
		response, err := service.CreateVolume(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		return response.Volume.VolumeId
	}
	first, second := create("first"), create("second")
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	deleteWaiting := func(id string) {
		t.Helper()
		if _, err := service.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}); status.Code(err) != codes.Unavailable {
			t.Fatalf("delete without fresh absence accepted: %v", err)
		}
		state, err := registry.Get(ctx, id)
		if err != nil || state.Phase != volumeapi.PhaseDeleting || state.CapacityBytes != 64<<20 || len(state.Finalizers) == 0 {
			t.Fatalf("waiting delete lost reservation/protection: %+v %v", state, err)
		}
	}
	deleteWaiting(first)
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	// Hold the first post-purge scan at its API-write boundary. A second
	// deletion then advances the shared Pool generation before that write.
	held := &heldProgressObservation{Registry: registry}
	node.Pools = held
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	deleteWaiting(second)
	deleteWaiting(first)
	if err := registry.SetPoolStatus(ctx, "pool", "pool-uid", "worker-a", held.status); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("stale observation overwrote the newer fence: %v", err)
	}
	request := validCreateRequest("worker-a")
	request.Name = "third"
	if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale registered Pool did not request an observation retry: %v", err)
	}
	if effects.creates != 2 {
		t.Fatalf("stale Pool executed a new create: %d", effects.creates)
	}
	parsed, _ := parseCreateRequest(request)
	if _, err := registry.Get(ctx, parsed.id); !apierrors.IsNotFound(err) {
		t.Fatalf("rejected create recorded intent: %v", err)
	}
	// Restart both controller state and node reconciliation. All recovery
	// evidence comes from the API journal and a new physical scan.
	service = configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv", Operator: effects, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: effects, CapacityPools: registry, CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}}})
	node, err = readiness.NewReconciler("worker-a", registry, readiness.NewProbe(host), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory = scanner.Scan
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("fresh physical absence released unsettled API holds: %v", err)
	}
	for _, id := range []string{first, second} {
		if _, err := service.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := registry.Get(ctx, id); !apierrors.IsNotFound(err) {
			t.Fatalf("settled volume remains: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(root, "volumes", id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("purged path remains: %v", err)
		}
	}
	if effects.purges != 2 {
		t.Fatalf("restart repeated destructive effects: %d", effects.purges)
	}
	if _, err := service.CreateVolume(ctx, request); err != nil {
		t.Fatalf("fresh observation did not restore creation: %v", err)
	}
	if effects.creates != 3 {
		t.Fatalf("unexpected creates: %d", effects.creates)
	}
}
