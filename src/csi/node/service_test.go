package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	shiftmount "github.com/project-jelly/ShiftPV/src/node/mount"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type fakePoolRegistry struct {
	pool volumeapi.Pool
	err  error
}

func TestNodePoolRootRejectsUnmountedRequiredMountPoint(t *testing.T) {
	hostRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(hostRoot, "pool"), 0o700); err != nil {
		t.Fatal(err)
	}
	pool := volumeapi.Pool{NodeName: "node-a", MountPath: "/pool", MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint,
		Status: volumeapi.PoolStatus{MountIdentity: &volumeapi.PoolMountIdentity{
			Device: "8:2", Root: "/", Source: "/dev/disk-a", Filesystem: "ext4",
		}}}
	service := &Service{NodeName: "node-a", HostRoot: hostRoot, Pools: fakePoolRegistry{pool: pool}}
	if _, _, err := service.poolRoot(context.Background(), volume.CopyIdentity{NodeName: "node-a", PoolName: "pool-a", PoolUID: "pool-a-uid"}); err == nil {
		t.Fatal("unmounted Pool path was accepted for publication")
	}
}

func (f fakePoolRegistry) PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return f.pool, f.err
}

type poolsByIdentity map[string]volumeapi.Pool

func (p poolsByIdentity) PoolForIdentity(_ context.Context, name, uid, node string) (volumeapi.Pool, error) {
	pool, found := p[name]
	if !found || pool.UID != uid || pool.NodeName != node {
		return volumeapi.Pool{}, volumeapi.ErrPoolNotFound
	}
	return pool, nil
}

type sequencedPoolRegistry struct {
	pools []volumeapi.Pool
	calls atomic.Int32
}

func (f *sequencedPoolRegistry) PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	call := int(f.calls.Add(1)) - 1
	if call >= len(f.pools) {
		call = len(f.pools) - 1
	}
	return f.pools[call], nil
}

type fakeVolumeRegistry struct {
	state          volumeapi.State
	getStates      []volumeapi.State
	getErr         error
	setErr         error
	publishedNode  string
	published      bool
	getCalls       atomic.Int32
	reconcileCalls atomic.Int32
}

type identityVolumeRegistryFake struct {
	*fakeVolumeRegistry
	copy       volume.CopyIdentity
	beginCalls int
	beginErr   error
}

func (f *identityVolumeRegistryFake) BeginPublish(_ context.Context, _ string, node string, copy volume.CopyIdentity) error {
	f.beginCalls++
	if node != f.state.OwnerNode || copy != f.copy {
		return volumeapi.ErrStateConflict
	}
	f.publishedNode = node
	f.published = true
	return f.beginErr
}

func (f *identityVolumeRegistryFake) ReconcilePublished(_ context.Context, _ string, node string, copy volume.CopyIdentity, published bool) error {
	f.reconcileCalls.Add(1)
	if node != f.state.OwnerNode || copy != f.copy {
		return volumeapi.ErrStateConflict
	}
	f.publishedNode = node
	f.published = published
	return f.setErr
}

func (f *fakeVolumeRegistry) Get(context.Context, string) (volumeapi.State, error) {
	call := int(f.getCalls.Add(1)) - 1
	if call < len(f.getStates) {
		return f.getStates[call], f.getErr
	}
	return f.state, f.getErr
}

func (f *fakeVolumeRegistry) BeginPublish(_ context.Context, _ string, node string, copy volume.CopyIdentity) error {
	if f.state.CurrentCopy == nil || *f.state.CurrentCopy != copy || f.state.OwnerNode != node {
		return volumeapi.ErrStateConflict
	}
	f.publishedNode = node
	f.published = true
	return f.setErr
}

func (f *fakeVolumeRegistry) ReconcilePublished(_ context.Context, _ string, node string, copy volume.CopyIdentity, published bool) error {
	f.reconcileCalls.Add(1)
	if f.state.CurrentCopy == nil || *f.state.CurrentCopy != copy || f.state.OwnerNode != node {
		return volumeapi.ErrStateConflict
	}
	f.publishedNode = node
	f.published = published
	return f.setErr
}

type fakeBinder struct {
	publishedSource string
	publishedTarget string
	unpublishedID   string
	unpublished     string
	publishErr      error
	unpublishErr    error
	remaining       bool
	remainingErr    error
}

func (f *fakeBinder) Publish(source, target string) error {
	f.publishedSource = source
	f.publishedTarget = target
	return f.publishErr
}
func (f *fakeBinder) Unpublish(volumeID, target string) error {
	f.unpublishedID = volumeID
	f.unpublished = target
	return f.unpublishErr
}
func (f *fakeBinder) HasPublishedTarget(string, string) (bool, error) {
	return f.remaining, f.remainingErr
}

func TestNodePublishUsesCanonicalOwnerPath(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	request := validPublishRequest()

	if _, err := service.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	expectedSource := filepath.Join(service.HostRoot, "pool", "volumes", request.VolumeId)
	if binder.publishedSource != expectedSource || binder.publishedTarget != request.TargetPath {
		t.Fatalf("unexpected publish: source=%q target=%q", binder.publishedSource, binder.publishedTarget)
	}
}

func TestNodePublishUsesRegisteredNodeMountPath(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	copy := *service.Volumes.(*fakeVolumeRegistry).state.CurrentCopy
	service.HostRoot = t.TempDir()
	poolRoot := filepath.Join(service.HostRoot, "srv", "storage-a")
	if err := os.MkdirAll(poolRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: "worker-a", MountPath: "/srv/storage-a"}}
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(service.HostRoot, "srv", "storage-a", "volumes", validPublishRequest().VolumeId)
	if binder.publishedSource != want {
		t.Fatalf("published source = %q, want %q", binder.publishedSource, want)
	}
}

func TestNodePublishResolvesExactPoolWithTwoRegistrationsOnNode(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	copy := *service.Volumes.(*fakeVolumeRegistry).state.CurrentCopy
	poolA := volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}
	poolB := volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: copy.NodeName, MountPath: "/pool-b"}
	service.Pools = poolsByIdentity{poolA.Name: poolA, poolB.Name: poolB}
	if err := os.Mkdir(filepath.Join(service.HostRoot, "pool-b"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(service.HostRoot, "pool", "volumes", copy.VolumeID)
	if binder.publishedSource != want {
		t.Fatalf("published source = %q, want %q", binder.publishedSource, want)
	}
}

func TestNodePublishRejectsReplacementPoolIdentity(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{
		Name: copy.PoolName, UID: "replacement-pool-uid", NodeName: copy.NodeName, MountPath: "/pool",
	}}
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement Pool publish code=%s err=%v", status.Code(err), err)
	}
	if binder.publishedSource != "" || registry.published {
		t.Fatalf("replacement Pool reached publish effects: binder=%#v registry=%#v", binder, registry)
	}
}

func TestNodePublishRechecksPoolIdentityUnderStorageLock(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	pools := &sequencedPoolRegistry{pools: []volumeapi.Pool{
		{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"},
		{Name: copy.PoolName, UID: "replacement-pool-uid", NodeName: copy.NodeName, MountPath: "/pool"},
	}}
	service.Pools = pools
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("racing replacement Pool publish code=%s err=%v", status.Code(err), err)
	}
	if pools.calls.Load() != 2 || binder.publishedSource != "" || registry.published {
		t.Fatalf("Pool replacement was not stopped before effects: calls=%d binder=%#v registry=%#v", pools.calls.Load(), binder, registry)
	}
}

func TestNodePublishUsesDynamicOwnerAndRecordsPublication(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	request := validPublishRequest()
	request.VolumeContext[controllercsi.NodeContextKey] = "stale-owner"

	if _, err := service.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.publishedSource == "" || registry.publishedNode != "worker-a" || !registry.published {
		t.Fatalf("publish was not recorded: binder=%#v registry=%#v", binder, registry)
	}
}

func TestNodePublishRecordsIntentBeforeBindingIdentifiedCopy(t *testing.T) {
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{state: volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy,
	}}, copy: copy}
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.HostRoot = hostRoot
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	service.Volumes = registry
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); err != nil {
		t.Fatal(err)
	}
	if registry.beginCalls != 1 || !registry.published || binder.publishedSource != filepath.Join(poolRoot, "volumes", copy.VolumeID) {
		t.Fatalf("registry=%#v binder=%#v", registry, binder)
	}
}

func TestNodePublishFailureReconcilesActualRemainingMounts(t *testing.T) {
	for name, remaining := range map[string]bool{"another target remains": true, "no target remains": false} {
		t.Run(name, func(t *testing.T) {
			hostRoot := t.TempDir()
			poolRoot := filepath.Join(hostRoot, "pool")
			if err := os.Mkdir(poolRoot, 0755); err != nil {
				t.Fatal(err)
			}
			copy := volume.CopyIdentity{
				InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
				VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
				CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
			}
			if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{state: volumeapi.State{
				UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy,
			}}, copy: copy}
			binder := &fakeBinder{publishErr: errors.New("mount failed"), remaining: remaining}
			service := configuredService(t, binder)
			service.HostRoot = hostRoot
			service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
			service.Volumes = registry
			if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); status.Code(err) != codes.Internal {
				t.Fatalf("publish failure code=%s err=%v", status.Code(err), err)
			}
			if registry.published != remaining {
				t.Fatalf("publication=%v want actual remaining=%v", registry.published, remaining)
			}
		})
	}
}

func TestNodePublishRetriesAcceptedIntentResponseLossBeforeMount(t *testing.T) {
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{state: volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy,
	}}, copy: copy, beginErr: apierrors.NewTimeoutError("accepted publication intent, response lost", 1)}
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.HostRoot = hostRoot
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	service.Volumes = registry
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous intent code=%s err=%v", status.Code(err), err)
	}
	if binder.publishedSource != "" || !registry.published {
		t.Fatalf("mount ran before intent was confirmed: binder=%#v registry=%#v", binder, registry)
	}
	registry.beginErr = nil
	if _, err := service.NodePublishVolume(context.Background(), validPublishRequest()); err != nil {
		t.Fatal(err)
	}
	if binder.publishedSource == "" || registry.beginCalls != 2 {
		t.Fatalf("retry did not converge on the recorded intent: binder=%#v calls=%d", binder, registry.beginCalls)
	}
}

func TestNodePublishFailsClosedForDynamicVolumeState(t *testing.T) {
	for name, test := range map[string]struct {
		registry *fakeVolumeRegistry
		wantCode codes.Code
	}{
		"moving": {
			registry: &fakeVolumeRegistry{state: volumeapi.State{Phase: volumeapi.PhaseMoving, OwnerNode: "worker-a"}},
			wantCode: codes.FailedPrecondition,
		},
		"blocked": {
			registry: &fakeVolumeRegistry{state: volumeapi.State{Phase: volumeapi.PhaseBlocked, OwnerNode: "worker-a"}},
			wantCode: codes.FailedPrecondition,
		},
		"wrong owner": {
			registry: &fakeVolumeRegistry{state: volumeapi.State{Phase: volumeapi.PhaseReady, OwnerNode: "worker-b"}},
			wantCode: codes.FailedPrecondition,
		},
		"registry unavailable": {
			registry: &fakeVolumeRegistry{getErr: errors.New("API timeout")},
			wantCode: codes.Unavailable,
		},
	} {
		t.Run(name, func(t *testing.T) {
			binder := &fakeBinder{}
			service := configuredService(t, binder)
			service.Volumes = test.registry
			_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
			if status.Code(err) != test.wantCode {
				t.Fatalf("expected %s, got %v", test.wantCode, err)
			}
			if binder.publishedSource != "" {
				t.Fatalf("fail-closed request reached binder: %#v", binder)
			}
		})
	}
}

func TestConcurrentMovingPublishesUseOneLiveReadAndReturnFailClosed(t *testing.T) {
	const requests = 64
	registry := &fakeVolumeRegistry{state: volumeapi.State{Phase: volumeapi.PhaseMoving, OwnerNode: "worker-a"}}
	service := configuredService(t, &fakeBinder{})
	service.Volumes = registry
	var wait sync.WaitGroup
	errors := make(chan error, requests)
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
			errors <- err
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("concurrent Moving publish returned %v", err)
		}
	}
	if got := registry.getCalls.Load(); got != requests {
		t.Fatalf("volume state reads = %d, want exactly %d", got, requests)
	}
}

func TestNodeGetInfoPublishesTopology(t *testing.T) {
	service := configuredService(t, &fakeBinder{})
	response, err := service.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.AccessibleTopology.Segments[volume.TopologyKey]; got != "worker-a" {
		t.Fatalf("unexpected topology: %q", got)
	}
}

func TestNodePublishRejectsInvalidRequests(t *testing.T) {
	tests := map[string]struct {
		mutate func(*csi.NodePublishVolumeRequest)
		code   codes.Code
	}{
		"missing volume ID": {
			mutate: func(req *csi.NodePublishVolumeRequest) { req.VolumeId = "" },
			code:   codes.InvalidArgument,
		},
		"read only": {
			mutate: func(req *csi.NodePublishVolumeRequest) { req.Readonly = true },
			code:   codes.InvalidArgument,
		},
		"raw block": {
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.VolumeCapability.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
			},
			code: codes.InvalidArgument,
		},
		"unsupported access mode": {
			mutate: func(req *csi.NodePublishVolumeRequest) {
				req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
			},
			code: codes.InvalidArgument,
		},
		"outside kubelet root": {
			mutate: func(req *csi.NodePublishVolumeRequest) { req.TargetPath = "/tmp/escape" },
			code:   codes.InvalidArgument,
		},
		"invalid volume ID": {
			mutate: func(req *csi.NodePublishVolumeRequest) { req.VolumeId = "../escape" },
			code:   codes.InvalidArgument,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			request := validPublishRequest()
			test.mutate(request)
			_, err := configuredService(t, &fakeBinder{}).NodePublishVolume(context.Background(), request)
			if status.Code(err) != test.code {
				t.Fatalf("expected %s, got %v", test.code, err)
			}
		})
	}
}

func TestNodePublishReportsBinderFailure(t *testing.T) {
	service := configuredService(t, &fakeBinder{publishErr: errors.New("mount failed")})
	_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestNodePublishFailsClosedForForeignStorageIdentity(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	foreign := *registry.state.CurrentCopy
	foreign.InstallationID = "other-installation"
	registry.state.CurrentCopy = &foreign

	_, err := service.NodePublishVolume(context.Background(), validPublishRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("foreign storage identity code=%s err=%v", status.Code(err), err)
	}
	if binder.publishedSource != "" || registry.published {
		t.Fatalf("foreign storage identity reached publish effects: binder=%#v registry=%#v", binder, registry)
	}
}

func TestNodePublishRejectsUnconfiguredService(t *testing.T) {
	_, err := (&Service{}).NodePublishVolume(context.Background(), validPublishRequest())
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestNodeUnpublishValidatesAndDelegates(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	request := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
	}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublishedID != request.VolumeId || binder.unpublished != request.TargetPath {
		t.Fatalf("unexpected unpublish: volume=%q target=%q", binder.unpublishedID, binder.unpublished)
	}
}

func TestNodeUnpublishRejectsDifferentMountedVolume(t *testing.T) {
	binder := &fakeBinder{unpublishErr: shiftmount.ErrTargetVolumeMismatch}
	service := configuredService(t, binder)
	request := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
	}
	_, err := service.NodeUnpublishVolume(context.Background(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mismatched mount code=%s err=%v", status.Code(err), err)
	}
	if service.Volumes.(*fakeVolumeRegistry).reconcileCalls.Load() != 0 {
		t.Fatal("mismatched mount reached publication reconciliation")
	}
}

func TestNodeUnpublishRecordsDynamicPublicationState(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	request := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
	}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if registry.publishedNode != "worker-a" || registry.published {
		t.Fatalf("unpublish was not recorded: %#v", registry)
	}

	registry.setErr = apierrors.NewTimeoutError("API timeout", 1)
	if _, err := service.NodeUnpublishVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable when unpublish state cannot be recorded, got %v", err)
	}
}

func TestNodeUnpublishIdentifiedCopyReconcilesUnderStorageLock(t *testing.T) {
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{state: volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: copy.NodeName, CurrentCopy: &copy,
	}}, copy: copy}
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.HostRoot = hostRoot
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	service.Volumes = registry
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublished != request.TargetPath || registry.published {
		t.Fatalf("unpublish did not settle actual mount state: binder=%#v registry=%#v", binder, registry)
	}
}

func TestNodeUnpublishRetriesAfterAcceptedStateResponseLoss(t *testing.T) {
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{state: volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: copy.NodeName, CurrentCopy: &copy,
	}, setErr: apierrors.NewTimeoutError("accepted unpublish state, response lost", 1)}, copy: copy}
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.HostRoot = hostRoot
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	service.Volumes = registry
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous state code=%s err=%v", status.Code(err), err)
	}
	if registry.published {
		t.Fatal("accepted state update was not retained")
	}
	destination := copy
	destination.PoolName, destination.PoolUID, destination.CopyID, destination.NodeName = "pool-b", "pool-b-uid", "destination-copy", "worker-b"
	registry.state = volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: destination.NodeName, CurrentCopy: &destination,
	}
	registry.setErr = nil
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatalf("stale source unpublish retry after owner commit: %v", err)
	}
	if binder.unpublished != request.TargetPath {
		t.Fatalf("stale source target was not unmounted: %#v", binder)
	}
}

func TestNodeUnpublishOwnerCommitDuringStorageLock(t *testing.T) {
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	sourceState := volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: copy.NodeName, CurrentCopy: &copy}
	destination := copy
	destination.PoolName, destination.PoolUID, destination.CopyID, destination.NodeName = "pool-b", "pool-b-uid", "destination-copy", "worker-b"
	destinationState := volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: destination.NodeName, CurrentCopy: &destination}
	registry := &identityVolumeRegistryFake{fakeVolumeRegistry: &fakeVolumeRegistry{
		state: sourceState, getStates: []volumeapi.State{sourceState, destinationState},
	}, copy: copy}
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.HostRoot = hostRoot
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	service.Volumes = registry
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatalf("owner commit during unpublish: %v", err)
	}
	if registry.getCalls.Load() != 2 || binder.unpublished != request.TargetPath {
		t.Fatalf("stale target did not converge after racing owner commit: reads=%d binder=%#v", registry.getCalls.Load(), binder)
	}
}

func TestNodeUnpublishMissingVolumeStillRemovesValidatedTarget(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	service.Volumes = &fakeVolumeRegistry{getErr: apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, "missing")}
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: "shiftpv-0123456789abcdef0123456789abcdef", TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublished != request.TargetPath {
		t.Fatalf("absent volume target was not unmounted: %#v", binder)
	}
}

func TestNodeUnpublishRemovesTargetWhenPoolIsUnavailable(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	service.Pools = fakePoolRegistry{err: apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvpools"}, "pool-a")}
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: "shiftpv-0123456789abcdef0123456789abcdef", TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("unavailable Pool blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}
}

func TestNodeUnpublishRetriesPublicationAfterTransientPoolFailure(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}
	service.Pools = fakePoolRegistry{err: apierrors.NewTimeoutError("Pool read timed out", 1)}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("transient Pool failure code=%s err=%v", status.Code(err), err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("transient Pool failure blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}

	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatalf("retry after Pool recovery: %v", err)
	}
	if registry.reconcileCalls.Load() != 1 || registry.published {
		t.Fatalf("retry did not reconcile publication state: reconciles=%d published=%v", registry.reconcileCalls.Load(), registry.published)
	}
}

func TestNodeUnpublishRemovesTargetWhenPoolIdentityWasReplaced(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	service.Pools = fakePoolRegistry{pool: volumeapi.Pool{
		Name: copy.PoolName, UID: "replacement-pool-uid", NodeName: copy.NodeName, MountPath: "/pool",
	}}
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("replacement Pool blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}
}

func TestNodeUnpublishRetriesPublicationWhenLocalPoolCannotBeOpened(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	poolMarker := filepath.Join(service.HostRoot, "pool", ".shiftpv", "pool.json")
	markerData, err := os.ReadFile(poolMarker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(poolMarker); err != nil {
		t.Fatal(err)
	}
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable local Pool code=%s err=%v", status.Code(err), err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("unavailable local Pool blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}
	if err := os.WriteFile(poolMarker, markerData, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatalf("retry after local Pool recovery: %v", err)
	}
	if registry.reconcileCalls.Load() != 1 || registry.published {
		t.Fatalf("retry did not reconcile publication state: reconciles=%d published=%v", registry.reconcileCalls.Load(), registry.published)
	}
}

func TestNodeUnpublishSkipsPublicationReconcileWhenPoolChangesUnderLock(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	pools := &sequencedPoolRegistry{pools: []volumeapi.Pool{
		{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"},
		{Name: copy.PoolName, UID: "replacement-pool-uid", NodeName: copy.NodeName, MountPath: "/pool"},
	}}
	service.Pools = pools
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if pools.calls.Load() != 2 || binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("racing Pool replacement changed publication state: calls=%d binder=%#v reconciles=%d", pools.calls.Load(), binder, registry.reconcileCalls.Load())
	}
}

func TestNodeUnpublishKeepsNodePublishedWhileAnotherTargetRemains(t *testing.T) {
	binder := &fakeBinder{remaining: true}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	request := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/old-uid/volumes/csi/mount",
	}
	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if registry.publishedNode != "worker-a" || !registry.published {
		t.Fatalf("remaining target was cleared from publication state: %#v", registry)
	}
}

func TestNodeUnpublishFailsClosedWhenRemainingTargetsCannotBeInspected(t *testing.T) {
	binder := &fakeBinder{remainingErr: errors.New("mountinfo unavailable")}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	_, err := service.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/old-uid/volumes/csi/mount",
	})
	if status.Code(err) != codes.Internal || registry.publishedNode != "" {
		t.Fatalf("expected fail-closed inspection error without state change, got err=%v registry=%#v", err, registry)
	}
}

func TestNodeUnpublishRemovesTargetWhenStorageIdentityIsForeign(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	foreign := *registry.state.CurrentCopy
	foreign.InstallationID = "other-installation"
	registry.state.CurrentCopy = &foreign
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: foreign.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	if _, err := service.NodeUnpublishVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("foreign storage identity blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}
}

func TestNodeUnpublishFailsClosedWhenAuthorityChangesUnderStorageLock(t *testing.T) {
	binder := &fakeBinder{}
	service := configuredService(t, binder)
	registry := service.Volumes.(*fakeVolumeRegistry)
	copy := *registry.state.CurrentCopy
	registry.getStates = []volumeapi.State{
		registry.state,
		{UID: "rotated-volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy},
	}
	request := &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount"}

	_, err := service.NodeUnpublishVolume(context.Background(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("rotated volume authority code=%s err=%v", status.Code(err), err)
	}
	if binder.unpublished != request.TargetPath || registry.reconcileCalls.Load() != 0 {
		t.Fatalf("rotated volume authority blocked target teardown or changed publication state: binder=%#v reconciles=%d", binder, registry.reconcileCalls.Load())
	}
}

func TestNodeUnpublishRejectsUnsafeTarget(t *testing.T) {
	service := configuredService(t, &fakeBinder{})
	_, err := service.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods-evil/uid",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestNodeUnpublishReportsBinderFailure(t *testing.T) {
	service := configuredService(t, &fakeBinder{unpublishErr: errors.New("unmount failed")})
	_, err := service.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestNodeUnpublishRejectsUnconfiguredService(t *testing.T) {
	_, err := (&Service{}).NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestNodeCapabilitiesAdvertiseNoOptionalServices(t *testing.T) {
	response, err := (&Service{}).NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Capabilities) != 0 {
		t.Fatalf("unexpected capabilities: %#v", response.Capabilities)
	}
}

func configuredService(t *testing.T, binder Binder) *Service {
	t.Helper()
	hostRoot := t.TempDir()
	poolRoot := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(poolRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry := &fakeVolumeRegistry{state: volumeapi.State{
		UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy,
	}}
	return &Service{
		NodeName:   "worker-a",
		HostRoot:   hostRoot,
		Pools:      fakePoolRegistry{pool: volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}},
		TargetRoot: "/var/lib/kubelet/pods",
		Binder:     binder,
		Volumes:    registry,
	}
}

func validPublishRequest() *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         "shiftpv-0123456789abcdef0123456789abcdef",
		TargetPath:       "/var/lib/kubelet/pods/uid/volumes/csi/mount",
		VolumeContext:    map[string]string{controllercsi.NodeContextKey: "worker-a"},
		VolumeCapability: singleNodeWriterCapability(),
	}
}

func singleNodeWriterCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}
