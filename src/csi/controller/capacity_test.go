package controller

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type fakePoolCapacityRegistry struct {
	mu      *sync.RWMutex
	pool    volumeapi.Pool
	pools   []volumeapi.Pool
	volumes map[string]volumeapi.State
	moves   []volumeapi.Move
	err     error
}

func (f *fakePoolCapacityRegistry) ReadyPoolForNode(context.Context, string) (volumeapi.Pool, error) {
	return f.pool, f.err
}

func (f *fakePoolCapacityRegistry) ReadyPools(context.Context) ([]volumeapi.Pool, error) {
	if len(f.pools) != 0 {
		return f.pools, f.err
	}
	return []volumeapi.Pool{f.pool}, f.err
}

func (f *fakePoolCapacityRegistry) PoolForIdentity(_ context.Context, name, uid, nodeName string) (volumeapi.Pool, error) {
	for _, pool := range append(append([]volumeapi.Pool(nil), f.pools...), f.pool) {
		if pool.Name == name && pool.UID == uid && pool.NodeName == nodeName {
			return pool, f.err
		}
	}
	return volumeapi.Pool{}, volumeapi.ErrPoolNotFound
}

func (f *fakePoolCapacityRegistry) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	unlock := readLock(f.mu)
	defer unlock()
	volumes := make(map[string]volumeapi.State, len(f.volumes))
	for id, state := range f.volumes {
		volumes[id] = state
	}
	return volumes, f.err
}

func (f *fakePoolCapacityRegistry) ListMoves(context.Context) ([]volumeapi.Move, error) {
	unlock := readLock(f.mu)
	defer unlock()
	return append([]volumeapi.Move(nil), f.moves...), f.err
}

type fakePoolCapacityProbe struct {
	mu         sync.Mutex
	stats      poolcapacity.Filesystem
	err        error
	calls      int
	poolErrors map[string]error
}

func (f *fakePoolCapacityProbe) StatFS(context.Context, string) (poolcapacity.Filesystem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.stats, f.err
}

func (f *fakePoolCapacityProbe) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	if err := f.poolErrors[pool.UID]; err != nil {
		return poolcapacity.Filesystem{}, err
	}
	return f.StatFS(ctx, pool.NodeName)
}

func (f *fakePoolCapacityProbe) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type capacityTrackingVolumeRegistry struct {
	mu        *sync.RWMutex
	volumes   map[string]volumeapi.State
	poolNodes []string
}

func (r *capacityTrackingVolumeRegistry) Get(_ context.Context, id string) (volumeapi.State, error) {
	unlock := readLock(r.mu)
	defer unlock()
	state, ok := r.volumes[id]
	if !ok {
		return volumeapi.State{}, apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, id)
	}
	return state, nil
}

func (r *capacityTrackingVolumeRegistry) Delete(_ context.Context, id, uid string) error {
	unlock := writeLock(r.mu)
	defer unlock()
	state, ok := r.volumes[id]
	if ok && state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	delete(r.volumes, id)
	return nil
}

func (r *capacityTrackingVolumeRegistry) RemoveVolumeFinalizer(_ context.Context, id, uid string) error {
	unlock := readLock(r.mu)
	defer unlock()
	state, ok := r.volumes[id]
	if !ok || state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	return nil
}

func (r *capacityTrackingVolumeRegistry) PoolNodes(context.Context) ([]string, error) {
	unlock := readLock(r.mu)
	defer unlock()
	if len(r.poolNodes) > 0 {
		return append([]string(nil), r.poolNodes...), nil
	}
	return []string{"worker-a", "worker-b"}, nil
}

func (r *capacityTrackingVolumeRegistry) PoolNodesForGroup(ctx context.Context, _ string) ([]string, error) {
	return r.PoolNodes(ctx)
}

func (r *capacityTrackingVolumeRegistry) BeginCreate(ctx context.Context, volumeID, requestName, nodeName string, capacityBytes int64) (volumeapi.State, error) {
	return r.BeginCreateInPool(ctx, volumeID, requestName, nodeName, capacityBytes, "pool", "pool-uid")
}

func (r *capacityTrackingVolumeRegistry) BeginCreateInPool(ctx context.Context, volumeID, requestName, nodeName string, capacityBytes int64, poolName, poolUID string) (volumeapi.State, error) {
	unlock := writeLock(r.mu)
	defer unlock()
	if state, ok := r.volumes[volumeID]; ok {
		if err := validateCreateIntent(state, requestName, nodeName, capacityBytes); err != nil {
			return volumeapi.State{}, err
		}
		return state, nil
	}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: poolName, PoolUID: poolUID,
		VolumeID: volumeID, VolumeUID: "volume-uid-" + volumeID[len(volumeID)-6:], CopyID: "initial-" + volumeID[len(volumeID)-6:],
		NodeName: nodeName, Role: volume.RoleServing,
	}
	state := volumeapi.State{
		UID: copy.VolumeUID, RequestName: requestName, CapacityBytes: capacityBytes, InitialNode: nodeName,
		Phase: volumeapi.PhasePending, OwnerNode: nodeName, CurrentCopy: &copy,
	}
	r.volumes[volumeID] = state
	return state, nil
}

func (r *capacityTrackingVolumeRegistry) CompleteCreate(ctx context.Context, volumeID, uid string, copy volume.CopyIdentity) error {
	unlock := writeLock(r.mu)
	defer unlock()
	state, ok := r.volumes[volumeID]
	if !ok || state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	state.Phase = volumeapi.PhaseReady
	state.CurrentCopy = &copy
	r.volumes[volumeID] = state
	return nil
}

func (r *capacityTrackingVolumeRegistry) BeginDelete(_ context.Context, volumeID, uid string, copy volume.CopyIdentity) (volumeapi.State, error) {
	unlock := writeLock(r.mu)
	defer unlock()
	state, ok := r.volumes[volumeID]
	if !ok || state.UID != uid || state.CurrentCopy == nil || *state.CurrentCopy != copy {
		return volumeapi.State{}, volumeapi.ErrStateConflict
	}
	state.Phase = volumeapi.PhaseDeleting
	state.DeletionOperationID = "delete-" + uid
	r.volumes[volumeID] = state
	return state, nil
}

func TestCreateVolumeUsesPoolLimitAndFilesystemCapacity(t *testing.T) {
	for name, test := range map[string]struct {
		limit     string
		available int64
		wantCode  codes.Code
	}{
		"accepted":             {limit: "128Mi", available: 128 << 20, wantCode: codes.OK},
		"logical limit":        {limit: "32Mi", available: 128 << 20, wantCode: codes.ResourceExhausted},
		"filesystem available": {limit: "128Mi", available: 32 << 20, wantCode: codes.ResourceExhausted},
		"missing limit":        {available: 128 << 20, wantCode: codes.FailedPrecondition},
		"invalid limit":        {limit: "not-a-quantity", available: 128 << 20, wantCode: codes.FailedPrecondition},
	} {
		t.Run(name, func(t *testing.T) {
			probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: test.available}}
			service := capacityService(fake.NewClientset(), test.limit, nil, probe)
			_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("code = %s, want %s: %v", got, test.wantCode, err)
			}
		})
	}
}

func TestCreateVolumeSelectsIndependentPoolOnSameNode(t *testing.T) {
	const existingID = "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	volumes := map[string]volumeapi.State{
		existingID: {UID: "existing-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 64 << 20,
			CurrentCopy: capacityCopy("pool-a-uid", "worker-a")},
	}
	registry := &fakePoolCapacityRegistry{
		pools: []volumeapi.Pool{
			{Name: "pool-a", UID: "pool-a-uid", NodeName: "worker-a", CapacityLimit: "64Mi"},
			{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-a", CapacityLimit: "128Mi"},
		},
		volumes: volumes,
	}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: &capacityTrackingVolumeRegistry{volumes: volumes}, CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}},
	})
	request := validCreateRequest("worker-a")
	if _, err := service.CreateVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id, _ := volume.IDFromName(request.Name)
	state := volumes[id]
	if state.CurrentCopy == nil || state.CurrentCopy.PoolUID != "pool-b-uid" {
		t.Fatalf("new copy was not placed in the independent Pool: %#v", state.CurrentCopy)
	}
}

func TestCreateVolumeSelectsPoolGroup(t *testing.T) {
	volumes := map[string]volumeapi.State{}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: &capacityTrackingVolumeRegistry{volumes: volumes},
		CapacityPools: &fakePoolCapacityRegistry{pools: []volumeapi.Pool{
			{Name: "default-pool", UID: "default-uid", NodeName: "worker-a", CapacityLimit: "1Gi"},
			{Name: "fast-pool", UID: "fast-uid", NodeName: "worker-a", PoolGroup: "fast", CapacityLimit: "1Gi"},
		}},
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}},
	})
	request := validCreateRequest("worker-a")
	request.Parameters = map[string]string{PoolGroupKey: "fast"}
	if _, err := service.CreateVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id, _ := volume.IDFromName(request.Name)
	if copy := volumes[id].CurrentCopy; copy == nil || copy.PoolUID != "fast-uid" {
		t.Fatalf("Pool group selected wrong copy: %#v", copy)
	}
	request.Parameters[PoolGroupKey] = "default"
	if _, err := service.CreateVolume(context.Background(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("retry in another Pool group accepted: %v", err)
	}
}

func TestCreateVolumeRetriesAnyPoolThatCanFitScheduledConsumer(t *testing.T) {
	volumes := map[string]volumeapi.State{}
	service := configuredService(&Service{
		Client:    fake.NewClientset(scheduledScratchPVC("worker-a"), scratchConsumerPod("worker-a")),
		Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: &capacityTrackingVolumeRegistry{volumes: volumes},
		CapacityPools: &fakePoolCapacityRegistry{pools: []volumeapi.Pool{
			{Name: "too-small", UID: "small-uid", NodeName: "worker-a", CapacityLimit: "1Mi"},
			{Name: "temporarily-full", UID: "full-uid", NodeName: "worker-a", CapacityLimit: "1Gi"},
		}},
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30}},
	})
	request := validCreateRequest("worker-a")
	request.Parameters = map[string]string{PVCNameKey: "scratch", PVCNamespaceKey: "vmtest"}
	if _, err := service.CreateVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("temporarily full candidate must retain selected node: %v", err)
	}
}

func TestCreateVolumeUsesHealthyPoolWhenAnotherProbeFails(t *testing.T) {
	volumes := map[string]volumeapi.State{}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: &capacityTrackingVolumeRegistry{volumes: volumes},
		CapacityPools: &fakePoolCapacityRegistry{pools: []volumeapi.Pool{
			{Name: "failed", UID: "failed-uid", NodeName: "worker-a", CapacityLimit: "1Gi"},
			{Name: "healthy", UID: "healthy-uid", NodeName: "worker-a", CapacityLimit: "1Gi"},
		}},
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30},
			poolErrors: map[string]error{"failed-uid": retryableDirectoryError{errors.New("probe failed")}}},
	})
	request := validCreateRequest("worker-a")
	if _, err := service.CreateVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id, _ := volume.IDFromName(request.Name)
	if copy := volumes[id].CurrentCopy; copy == nil || copy.PoolUID != "healthy-uid" {
		t.Fatalf("healthy candidate was not chosen: %#v", copy)
	}
}

func TestCreateVolumeRetriesScheduledPodOwnedPVCWhenCapacityIsReleased(t *testing.T) {
	client := fake.NewClientset(scheduledScratchPVC("worker-a"), scratchConsumerPod("worker-a"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "vmtest"}})
	existingID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service := capacityService(client, "128Mi", map[string]volumeapi.State{
		existingID: {UID: "existing-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 80 << 20, CurrentCopy: capacityCopy("pool-uid", "worker-a")},
	}, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}})
	req := validCreateRequest("worker-a")
	req.Parameters = map[string]string{PVCNameKey: "scratch", PVCNamespaceKey: "vmtest"}
	if _, err := service.CreateVolume(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("scheduled consumer capacity denial = %v, want Unavailable", err)
	}
	registry := service.Volumes.(*capacityTrackingVolumeRegistry)
	registry.mu.Lock()
	delete(registry.volumes, existingID)
	registry.mu.Unlock()
	if _, err := service.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("retry after reservation release did not converge: %v", err)
	}
}

func TestCreateVolumeRetriesScheduledPodOwnedPVCWhenFilesystemSpaceReturns(t *testing.T) {
	client := fake.NewClientset(scheduledScratchPVC("worker-a"), scratchConsumerPod("worker-a"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "vmtest"}})
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 128 << 20, AvailableBytes: 32 << 20}}
	service := capacityService(client, "128Mi", nil, probe)
	req := validCreateRequest("worker-a")
	req.Parameters = map[string]string{PVCNameKey: "scratch", PVCNamespaceKey: "vmtest"}
	if _, err := service.CreateVolume(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("filesystem shortage for scheduled consumer = %v, want Unavailable", err)
	}
	probe.mu.Lock()
	probe.stats.AvailableBytes = 128 << 20
	probe.mu.Unlock()
	if _, err := service.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("retry after filesystem space returns did not converge: %v", err)
	}
}

func TestCreateVolumeKeepsReschedulingForUnfixedOrPermanentlyOversizedPVC(t *testing.T) {
	for name, test := range map[string]struct {
		change func(*corev1.PersistentVolumeClaim, *corev1.Pod)
		size   int64
		limit  string
		total  int64
	}{
		"no Pod owner":             {change: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.Pod) { pvc.OwnerReferences = nil }},
		"consumer not scheduled":   {change: func(_ *corev1.PersistentVolumeClaim, pod *corev1.Pod) { pod.Spec.NodeName = "" }},
		"consumer on another node": {change: func(_ *corev1.PersistentVolumeClaim, pod *corev1.Pod) { pod.Spec.NodeName = "worker-b" }},
		"stale Pod identity":       {change: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.Pod) { pvc.OwnerReferences[0].UID = "old-pod" }},
		"unrelated Pod volume": {change: func(_ *corev1.PersistentVolumeClaim, pod *corev1.Pod) {
			pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other"
		}},
		"wrong PVC identity": {change: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.Pod) { pvc.UID = "other" }},
		"selected node mismatch": {change: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.Pod) {
			pvc.Annotations[selectedNodeAnnotation] = "worker-b"
		}},
		"oversized request": {size: 129 << 20},
		"larger than filesystem after reservation denial": {size: 192 << 20, limit: "256Mi", total: 128 << 20},
	} {
		t.Run(name, func(t *testing.T) {
			pvc, pod := scheduledScratchPVC("worker-a"), scratchConsumerPod("worker-a")
			if test.change != nil {
				test.change(pvc, pod)
			}
			limit := test.limit
			if limit == "" {
				limit = "128Mi"
			}
			total := test.total
			if total == 0 {
				total = 1 << 30
			}
			service := capacityService(fake.NewClientset(pvc, pod), limit, map[string]volumeapi.State{
				"shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {UID: "existing-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 80 << 20, CurrentCopy: capacityCopy("pool-uid", "worker-a")},
			}, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: total, AvailableBytes: total}})
			req := validCreateRequest("worker-a")
			req.Parameters = map[string]string{PVCNameKey: "scratch", PVCNamespaceKey: "vmtest"}
			if test.size != 0 {
				req.CapacityRange.RequiredBytes = test.size
			}
			if _, err := service.CreateVolume(context.Background(), req); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("capacity denial = %v, want ResourceExhausted", err)
			}
		})
	}
}

func scheduledScratchPVC(node string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "scratch", Namespace: "vmtest", UID: types.UID("uid"),
		Annotations:     map[string]string{selectedNodeAnnotation: node},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "importer", UID: types.UID("pod-uid")}},
	}}
}

func scratchConsumerPod(node string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "importer", Namespace: "vmtest", UID: types.UID("pod-uid")},
		Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "scratch"},
		}}}},
	}
}

func TestCreateVolumeTreatsStatFSAsSnapshotWhenFilesystemChanges(t *testing.T) {
	client := fake.NewClientset()
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	operator := &fakeDirectoryOperator{createErr: retryableDirectoryError{&os.PathError{
		Op: "mkdir", Path: "/pool/volumes/test", Err: syscall.ENOSPC,
	}}}
	service := capacityService(client, "1Gi", nil, probe)
	service.Operator = operator
	request := validCreateRequest("worker-a")
	volumeID, err := volume.IDFromName(request.Name)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.CreateVolume(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("filesystem changed after statfs: code=%s err=%v", status.Code(err), err)
	}
	registry, ok := service.Volumes.(*capacityTrackingVolumeRegistry)
	if !ok || registry.volumes[volumeID].CapacityBytes != 64<<20 || registry.volumes[volumeID].RequestName != request.Name {
		t.Fatalf("retryable ENOSPC did not preserve volume intent: %#v", registry)
	}
	if operator.createCalls != 1 || probe.callCount() != 1 {
		t.Fatalf("create calls=%d statfs calls=%d", operator.createCalls, probe.callCount())
	}

	operator.createErr = nil
	response, err := service.CreateVolume(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetVolume().GetVolumeId() != volumeID || operator.createCalls != 2 {
		t.Fatalf("retry did not converge: response=%#v createCalls=%d", response, operator.createCalls)
	}
}

func TestCreateVolumeCountsReservationsAtCurrentVolumeOwner(t *testing.T) {
	existingID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	client := fake.NewClientset()
	registry := &fakePoolCapacityRegistry{
		pool: volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-b", MountPath: "/pool", CapacityLimit: "64Mi"},
		volumes: map[string]volumeapi.State{
			existingID: {UID: "volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-b", CapacityBytes: 64 << 20, CurrentCopy: capacityCopy("pool-b-uid", "worker-b")},
		},
	}
	service := configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
	})
	req := validCreateRequest("worker-b")
	req.Name = "new-pvc"
	_, err := service.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("moved reservation was not charged to current owner: %v", err)
	}
}

func TestCreateVolumeCountsCapacityApprovedMoveAtDestination(t *testing.T) {
	existingID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	client := fake.NewClientset()
	registry := &fakePoolCapacityRegistry{
		pool: volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-b", MountPath: "/pool", CapacityLimit: "64Mi"},
		volumes: map[string]volumeapi.State{
			existingID: {UID: "volume-uid", Phase: volumeapi.PhaseMoving, OwnerNode: "worker-a", ActiveMove: "move-a", CapacityBytes: 64 << 20, CurrentCopy: capacityCopy("source-uid", "worker-a")},
		},
		moves: []volumeapi.Move{{
			Name: "move-a", Spec: volumeapi.MoveSpec{VolumeID: existingID, SourceNode: "worker-a"},
			Status: volumeapi.MoveStatus{DestinationNode: "worker-b", DestinationPoolUID: "pool-b-uid", SourceCopy: capacityCopy("source-uid", "worker-a"), CapacityApproved: true, SourceBytes: 1},
		}},
	}
	service := configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
	})
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-b"))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("approved move was not charged to destination: %v", err)
	}
}

func TestCreateVolumeRetainsRecoveredMoveCapacityUntilSettled(t *testing.T) {
	existingID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	client := fake.NewClientset()
	registry := &fakePoolCapacityRegistry{
		pool: volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-b", MountPath: "/pool", CapacityLimit: "64Mi"},
		volumes: map[string]volumeapi.State{
			existingID: {UID: "volume-uid", Phase: volumeapi.PhaseMoving, OwnerNode: "worker-a", ActiveMove: "move-a", CapacityBytes: 64 << 20, CurrentCopy: capacityCopy("source-uid", "worker-a")},
		},
		moves: []volumeapi.Move{{
			Name: "move-a", Spec: volumeapi.MoveSpec{VolumeID: existingID, SourceNode: "worker-a"},
			Status: volumeapi.MoveStatus{
				Phase: "Blocked", DestinationNode: "worker-b", DestinationPoolUID: "pool-b-uid", SourceCopy: capacityCopy("source-uid", "worker-a"), CapacityApproved: true,
				SourceBytes: 1, RecoveryPhase: "Recovered",
			},
		}},
	}
	service := configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
	})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-b")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("recovered move released capacity before settlement: %v", err)
	}
}

func TestCreateVolumeIgnoresMoveAfterVolumeAndReservationDeletion(t *testing.T) {
	registry := &fakePoolCapacityRegistry{
		pool:    volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-b", MountPath: "/pool", CapacityLimit: "64Mi"},
		volumes: map[string]volumeapi.State{},
		moves: []volumeapi.Move{{
			Name: "move-deleted", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-deleted", SourceNode: "worker-a"},
			Status: volumeapi.MoveStatus{
				Phase: "Succeeded", CleanupPhase: "Completed", DestinationNode: "worker-b", CapacityApproved: true, SourceBytes: 1,
			},
		}},
	}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
	})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-b")); err != nil {
		t.Fatalf("deleted volume's Move blocked unrelated provisioning: %v", err)
	}
}

func TestCreateVolumeRejectsMoveWithReservationButNoVolume(t *testing.T) {
	volumeID := "shiftpv-orphaned-reservation"
	registry := &fakePoolCapacityRegistry{
		pool:    volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-b", MountPath: "/pool", CapacityLimit: "64Mi"},
		volumes: map[string]volumeapi.State{},
		moves: []volumeapi.Move{{
			Name: "move-incomplete", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "worker-a"},
			Status: volumeapi.MoveStatus{DestinationNode: "worker-b", CapacityApproved: true, SourceBytes: 1},
		}},
	}
	service := configuredService(&Service{
		Client:    fake.NewClientset(),
		Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, CapacityPools: registry,
		CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
	})
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-b"))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("incomplete move was not rejected: %v", err)
	}
}

func TestCreateVolumeSerializesPoolReservationAdmission(t *testing.T) {
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	service := capacityService(fake.NewClientset(), "64Mi", nil, probe)
	requests := []string{"pvc-a", "pvc-b"}
	results := make(chan codes.Code, len(requests))
	for _, name := range requests {
		name := name
		go func() {
			req := validCreateRequest("worker-a")
			req.Name = name
			_, err := service.CreateVolume(context.Background(), req)
			results <- status.Code(err)
		}()
	}
	counts := map[codes.Code]int{}
	for range requests {
		counts[<-results]++
	}
	if counts[codes.OK] != 1 || counts[codes.ResourceExhausted] != 1 {
		t.Fatalf("result codes = %v", counts)
	}
}

func TestCreateVolumeReusesExistingVolumeIntentWithoutNewProbe(t *testing.T) {
	req := validCreateRequest("worker-a")
	id, idErr := volume.IDFromName(req.Name)
	if idErr != nil {
		t.Fatal(idErr)
	}
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 0}}
	registry := &fakeVolumeRegistry{state: volumeapi.State{
		UID: "volume-uid", RequestName: req.Name, CapacityBytes: req.CapacityRange.RequiredBytes, InitialNode: "worker-a",
		Phase: volumeapi.PhasePending, OwnerNode: "worker-a", CurrentCopy: validCurrentCopy(id, "worker-a"),
	}, poolNodes: []string{"worker-a"}}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: registry,
		CapacityPools: &fakePoolCapacityRegistry{
			pool:    volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/pool", CapacityLimit: "1Mi"},
			volumes: map[string]volumeapi.State{id: registry.state},
		},
		CapacityProbe: probe,
	})

	response, err := service.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetVolume().GetVolumeId() != id || probe.callCount() != 0 {
		t.Fatalf("response=%#v probe calls=%d", response, probe.callCount())
	}
}

func TestCreateVolumeFailsClosedOnCapacityProbeError(t *testing.T) {
	probe := &fakePoolCapacityProbe{err: retryableDirectoryError{errors.New("statfs unavailable")}}
	client := fake.NewClientset()
	service := capacityService(client, "1Gi", nil, probe)

	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable: %v", status.Code(err), err)
	}
	registry, ok := service.Volumes.(*capacityTrackingVolumeRegistry)
	if !ok || len(registry.volumes) != 0 {
		t.Fatalf("probe failure created volume intent: %#v", registry)
	}
}

func TestCapacityProbeErrorPreservesContextCode(t *testing.T) {
	for name, test := range map[string]struct {
		err      error
		wantCode codes.Code
	}{
		"canceled": {err: context.Canceled, wantCode: codes.Canceled},
		"deadline": {err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			if got := status.Code(capacityProbeError("probe", test.err)); got != test.wantCode {
				t.Fatalf("code = %s, want %s", got, test.wantCode)
			}
		})
	}
}

func TestCreateVolumeRejectsUnavailablePoolBeforeCapacityProbe(t *testing.T) {
	for name, poolErr := range map[string]error{
		"invalid":   volumeapi.ErrPoolConfiguration,
		"missing":   volumeapi.ErrPoolNotFound,
		"not ready": volumeapi.ErrPoolNotReady,
	} {
		t.Run(name, func(t *testing.T) {
			probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
			service := configuredService(&Service{
				Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
				CapacityPools: &fakePoolCapacityRegistry{err: poolErr}, CapacityProbe: probe,
			})
			_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
			if status.Code(err) != codes.FailedPrecondition || probe.callCount() != 0 {
				t.Fatalf("code=%s probeCalls=%d err=%v", status.Code(err), probe.callCount(), err)
			}
		})
	}
}

func TestCreateVolumeRejectsServingCopyBeforeReservation(t *testing.T) {
	request := validCreateRequest("worker-a")
	volumeID, err := volume.IDFromName(request.Name)
	if err != nil {
		t.Fatal(err)
	}
	serving := volume.CopyIdentity{
		InstallationID: "installation-uid", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "old-volume-uid", CopyID: "old-copy",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
	client := fake.NewClientset()
	service := configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		CapacityPools: &fakePoolCapacityRegistry{
			pool: volumeapi.Pool{
				Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/pool", CapacityLimit: "1Gi",
				Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &serving, Present: true}}}},
			},
		},
		CapacityProbe: probe,
	})
	_, err = service.CreateVolume(context.Background(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition: %v", status.Code(err), err)
	}
	if probe.callCount() != 0 {
		t.Fatalf("capacity probe called despite serving copy conflict: %d", probe.callCount())
	}
}

func capacityService(client *fake.Clientset, limit string, volumes map[string]volumeapi.State, probe PoolCapacityProbe) *Service {
	if volumes == nil {
		volumes = map[string]volumeapi.State{}
	}
	guard := &sync.RWMutex{}
	registry := &fakePoolCapacityRegistry{
		mu:      guard,
		pool:    volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/pool", CapacityLimit: limit},
		volumes: volumes,
	}
	volumeRegistry := &capacityTrackingVolumeRegistry{mu: guard, volumes: volumes}
	return configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: volumeRegistry, CapacityPools: registry, CapacityProbe: probe,
	})
}

func capacityCopy(poolUID, nodeName string) *volume.CopyIdentity {
	return &volume.CopyIdentity{PoolUID: poolUID, NodeName: nodeName}
}

func readLock(mu *sync.RWMutex) func() {
	if mu == nil {
		return func() {}
	}
	mu.RLock()
	return mu.RUnlock
}

func writeLock(mu *sync.RWMutex) func() {
	if mu == nil {
		return func() {}
	}
	mu.Lock()
	return mu.Unlock
}

func validCurrentCopy(volumeID, nodeName string) *volume.CopyIdentity {
	return &volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: nodeName, Role: volume.RoleServing,
	}
}
