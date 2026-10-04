package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var cleanupListKinds = map[schema.GroupVersionResource]string{
	volumeapi.VolumeResource: "ShiftPVVolumeList",
	volumeapi.MoveResource:   "ShiftPVMoveList",
	volumeapi.PoolResource:   "ShiftPVPoolList",
}

type fakeVolumeRegistry struct {
	state     volumeapi.State
	poolNodes []string
}

type retryDeleteVolumeRegistry struct {
	state             volumeapi.State
	exists            bool
	deleteCalls       int
	deleteFirst       bool
	deletionRequested bool
	finalizersRemoved int
	events            []string
}

func (r *retryDeleteVolumeRegistry) BeginCreate(context.Context, string, string, string, int64) (volumeapi.State, error) {
	return r.state, nil
}

func (r *retryDeleteVolumeRegistry) BeginCreateInPool(ctx context.Context, id, request, node string, capacity int64, _, _ string) (volumeapi.State, error) {
	return r.BeginCreate(ctx, id, request, node, capacity)
}

func (r *retryDeleteVolumeRegistry) CompleteCreate(context.Context, string, string, volume.CopyIdentity) error {
	return nil
}

func (r *retryDeleteVolumeRegistry) Get(_ context.Context, id string) (volumeapi.State, error) {
	if !r.exists {
		return volumeapi.State{}, apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, id)
	}
	return r.state, nil
}

func (r *retryDeleteVolumeRegistry) Delete(_ context.Context, _ string, uid string) error {
	if r.exists && r.state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	r.events = append(r.events, "delete")
	r.deleteCalls++
	if r.deleteCalls == 1 {
		if r.deleteFirst {
			r.deletionRequested = true
		}
		return apierrors.NewTimeoutError("volume state delete response timed out", 1)
	}
	r.deletionRequested = true
	if r.finalizersRemoved > 0 {
		r.exists = false
	}
	return nil
}

func (r *retryDeleteVolumeRegistry) RemoveVolumeFinalizer(_ context.Context, _ string, uid string) error {
	if !r.exists {
		return nil
	}
	if r.state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	r.events = append(r.events, "remove-finalizer")
	r.finalizersRemoved++
	if r.deletionRequested {
		r.exists = false
	}
	return nil
}

func (r *retryDeleteVolumeRegistry) BeginDelete(_ context.Context, volumeID, uid string, copy volume.CopyIdentity) (volumeapi.State, error) {
	if !r.exists || r.state.UID != uid || copy.VolumeID != volumeID || r.state.CurrentCopy == nil || *r.state.CurrentCopy != copy ||
		r.state.ActiveMove != "" || len(r.state.PublishedNodes) != 0 ||
		(r.state.Phase != volumeapi.PhaseReady && r.state.Phase != volumeapi.PhaseDeleting) {
		return volumeapi.State{}, volumeapi.ErrStateConflict
	}
	r.state.Phase = volumeapi.PhaseDeleting
	r.state.DeletionOperationID = "delete-" + uid
	return r.state, nil
}

func (*retryDeleteVolumeRegistry) PoolNodes(context.Context) ([]string, error) { return nil, nil }
func (r *retryDeleteVolumeRegistry) PoolNodesForGroup(ctx context.Context, _ string) ([]string, error) {
	return r.PoolNodes(ctx)
}

func (f *fakeVolumeRegistry) Get(_ context.Context, id string) (volumeapi.State, error) {
	if f.state.UID == "" && f.state.Phase == "" && f.state.CurrentCopy == nil && f.state.CapacityBytes == 0 {
		return volumeapi.State{}, apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, id)
	}
	return f.state, nil
}
func (f *fakeVolumeRegistry) Delete(_ context.Context, _ string, uid string) error {
	if f.state.UID != "" && f.state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	return nil
}
func (f *fakeVolumeRegistry) RemoveVolumeFinalizer(_ context.Context, _ string, uid string) error {
	if f.state.UID != "" && f.state.UID != uid {
		return volumeapi.ErrStateConflict
	}
	return nil
}
func (f *fakeVolumeRegistry) PoolNodes(context.Context) ([]string, error) {
	if len(f.poolNodes) > 0 {
		return f.poolNodes, nil
	}
	return []string{f.state.OwnerNode}, nil
}

func (f *fakeVolumeRegistry) PoolNodesForGroup(ctx context.Context, _ string) ([]string, error) {
	return f.PoolNodes(ctx)
}

func (f *fakeVolumeRegistry) BeginCreate(_ context.Context, volumeID, requestName, nodeName string, capacityBytes int64) (volumeapi.State, error) {
	if f.state.CurrentCopy == nil {
		copy := volume.CopyIdentity{
			InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
			VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid",
			NodeName: nodeName, Role: volume.RoleServing,
		}
		f.state = volumeapi.State{
			UID: copy.VolumeUID, RequestName: requestName, CapacityBytes: capacityBytes, InitialNode: nodeName,
			Phase: volumeapi.PhasePending, OwnerNode: nodeName, CurrentCopy: &copy,
		}
	} else if err := validateCreateIntent(f.state, requestName, nodeName, capacityBytes); err != nil {
		return volumeapi.State{}, err
	}
	return f.state, nil
}

func (f *fakeVolumeRegistry) BeginCreateInPool(ctx context.Context, id, request, node string, capacity int64, _, _ string) (volumeapi.State, error) {
	return f.BeginCreate(ctx, id, request, node, capacity)
}

func (f *fakeVolumeRegistry) CompleteCreate(_ context.Context, _ string, _ string, copy volume.CopyIdentity) error {
	f.state.Phase = volumeapi.PhaseReady
	f.state.CurrentCopy = &copy
	return nil
}

func (f *fakeVolumeRegistry) BeginDelete(_ context.Context, volumeID, uid string, copy volume.CopyIdentity) (volumeapi.State, error) {
	if f.state.UID != uid || copy.VolumeID != volumeID || f.state.CurrentCopy == nil || *f.state.CurrentCopy != copy ||
		f.state.ActiveMove != "" || len(f.state.PublishedNodes) != 0 ||
		(f.state.Phase != volumeapi.PhaseReady && f.state.Phase != volumeapi.PhaseDeleting) {
		return volumeapi.State{}, volumeapi.ErrStateConflict
	}
	f.state.Phase = volumeapi.PhaseDeleting
	f.state.DeletionOperationID = "delete-" + uid
	return f.state, nil
}

type fakeDirectoryOperator struct {
	createdNode string
	createdID   string
	createCalls int
	createErr   error
}

type retryableDirectoryError struct{ error }

func (retryableDirectoryError) Retryable() bool { return true }

type durableCreateRegistry struct {
	fakeVolumeRegistry
	state         volumeapi.State
	events        *[]string
	beginErr      error
	completeErr   error
	completeCalls int
}

func (r *durableCreateRegistry) BeginCreate(context.Context, string, string, string, int64) (volumeapi.State, error) {
	*r.events = append(*r.events, "intent")
	return r.state, r.beginErr
}

func (r *durableCreateRegistry) BeginCreateInPool(ctx context.Context, id, request, node string, capacity int64, _, _ string) (volumeapi.State, error) {
	return r.BeginCreate(ctx, id, request, node, capacity)
}

func (r *durableCreateRegistry) CompleteCreate(context.Context, string, string, volume.CopyIdentity) error {
	*r.events = append(*r.events, "complete")
	r.completeCalls++
	return r.completeErr
}

type identityCreateOperator struct {
	events      *[]string
	createErr   error
	finalizeErr error
}

type receiptCleanupOperator struct{ calls int }

type verifyingCleanupOperator struct{ calls int }

type blockingCreateOperator struct {
	started chan struct{}
	release chan struct{}
}

func (o *blockingCreateOperator) CreateCopy(context.Context, volume.CopyIdentity) error {
	o.started <- struct{}{}
	<-o.release
	return nil
}

func (*blockingCreateOperator) FinalizeCreate(context.Context, volume.CopyIdentity) error { return nil }

type blockingCleanupOperator struct {
	started  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
	delegate receiptCleanupOperator
}

func (o *blockingCleanupOperator) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	if o.calls.Add(1) == 1 {
		o.started <- struct{}{}
		<-o.release
	}
	return o.delegate.Reclaim(ctx, cleanup, store)
}

func configuredService(service *Service) *Service {
	if service.Volumes == nil {
		service.Volumes = &fakeVolumeRegistry{poolNodes: []string{"worker-a", "worker-b"}}
	}
	if service.Cleanups == nil {
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds)
		service.Cleanups = &cleanupapi.Store{Client: client}
	}
	if service.CleanupOperator == nil {
		service.CleanupOperator = &receiptCleanupOperator{}
	}
	return service
}

func (o *receiptCleanupOperator) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	o.calls++
	if cleanup.Status.Phase == cleanupapi.PhaseVerifying || cleanup.Status.Phase == cleanupapi.PhaseCompleted {
		return cleanup, nil
	}
	executor := &cleanupapi.Executor{JobName: "job", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	if err := store.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	running, err := store.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true,
		LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if err := store.UpdateStatus(ctx, running, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	verifying, err := store.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	proof := &cleanupapi.AbsenceProof{
		RequestID: "absence-proof", PoolName: cleanup.Spec.Target.PoolName, PoolUID: cleanup.Spec.Target.PoolUID,
		RequiredGeneration: 1,
	}
	if err := store.UpdateStatus(ctx, verifying, cleanupapi.Status{
		Phase: cleanupapi.PhaseConfirmingAbsence, Executor: executor, Receipt: receipt, AbsenceProof: proof,
	}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	confirming, err := store.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	proof.Valid = true
	proof.Complete = true
	proof.Absent = true
	proof.ObservedGeneration = 1
	proof.ConfirmedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.UpdateStatus(ctx, confirming, cleanupapi.Status{
		Phase: cleanupapi.PhaseCompleted, Executor: executor, Receipt: receipt, AbsenceProof: proof,
		SettledAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	return store.Get(ctx, cleanup.Spec.Authority)
}

func (o *verifyingCleanupOperator) Reclaim(ctx context.Context, cleanup cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	o.calls++
	executor := &cleanupapi.Executor{JobName: "job", JobUID: "job-uid", PodUID: "pod-uid", NodeName: cleanup.Spec.Target.NodeName}
	if err := store.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	running, err := store.Get(ctx, cleanup.Spec.Authority)
	if err != nil {
		return cleanupapi.Cleanup{}, err
	}
	receipt := &cleanupapi.Receipt{
		OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true,
		LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if err := store.UpdateStatus(ctx, running, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	return store.Get(ctx, cleanup.Spec.Authority)
}

func cleanupParentVolume(volumeID string, copy volume.CopyIdentity) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVVolume",
		"metadata": map[string]any{
			"name": volumeID, "uid": copy.VolumeUID, "generation": int64(1),
			"finalizers": []any{volumeapi.VolumeProtectionFinalizer},
		},
		"spec": map[string]any{
			"volumeID": volumeID, "requestName": "pvc", "initialNode": copy.NodeName, "capacityBytes": int64(64 << 20),
		},
	}}
}

func cleanupPool(copy volume.CopyIdentity, present bool) *unstructured.Unstructured {
	identity, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVPool",
		"metadata": map[string]any{
			"name": copy.PoolName, "uid": copy.PoolUID, "generation": int64(2),
			"finalizers": []any{volumeapi.PoolProtectionFinalizer},
		},
		"spec": map[string]any{"nodeName": copy.NodeName, "scanEpoch": int64(1)},
		"status": map[string]any{
			"observedGeneration": int64(2),
			"inventory": map[string]any{
				"observedAt": time.Now().UTC().Format(time.RFC3339Nano), "valid": true, "truncated": false,
				"copies": []any{map[string]any{"marker": "copy", "identity": identity, "present": present}},
			},
		},
	}}
}

func TestDeleteVolumeUsesCleanupIntentAndReceiptBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &retryDeleteVolumeRegistry{state: volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy}, exists: true, deleteCalls: 1}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, cleanupParentVolume(volumeID, copy))
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	operator := &fakeDirectoryOperator{}
	reclaimer := &receiptCleanupOperator{}
	client := fake.NewClientset()
	service := &Service{
		Client: client, Namespace: "shiftpv-system", Operator: operator, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: reclaimer,
	}
	if _, err := service.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	if reclaimer.calls != 1 {
		t.Fatalf("cleanup calls=%d", reclaimer.calls)
	}
	if registry.state.Phase != volumeapi.PhaseDeleting || registry.state.DeletionOperationID != "delete-"+copy.VolumeUID {
		t.Fatalf("volume deletion was not fenced before cleanup: %#v", registry.state)
	}
	items, err := cleanups.List(ctx)
	if err != nil || len(items) != 1 || items[0].Status.Phase != cleanupapi.PhaseCompleted || items[0].Status.Receipt == nil {
		t.Fatalf("cleanups=%#v err=%v", items, err)
	}
	if registry.exists {
		t.Fatal("volume metadata remains after verified receipt")
	}
	if len(registry.events) != 2 || registry.events[0] != "delete" || registry.events[1] != "remove-finalizer" {
		t.Fatalf("settled deletion order=%v, want delete request before finalizer release", registry.events)
	}
}

func TestDeleteVolumeRetainsFinalizerAndCapacityUntilCausalAbsenceProof(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-1023456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &retryDeleteVolumeRegistry{
		state: volumeapi.State{
			UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName,
			CapacityBytes: 64 << 20, CurrentCopy: &copy,
		},
		exists: true, deleteCalls: 1,
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), cleanupListKinds, cleanupParentVolume(volumeID, copy), cleanupPool(copy, true),
	)
	dynamicClient.PrependReactor("update", "shiftpvpools", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if action.GetSubresource() == "" {
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	reclaimer := &verifyingCleanupOperator{}
	service := &Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: reclaimer,
	}
	request := &csi.DeleteVolumeRequest{VolumeId: volumeID}
	if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("receipt-only delete code=%s err=%v", status.Code(err), err)
	}
	if !registry.exists || registry.finalizersRemoved != 0 || registry.deleteCalls != 1 {
		t.Fatalf("receipt released metadata early: exists=%t finalizers=%d deletes=%d", registry.exists, registry.finalizersRemoved, registry.deleteCalls)
	}
	journal, err := cleanups.Get(ctx, cleanupapi.Authority{Kind: "ShiftPVVolume", Name: volumeID, UID: copy.VolumeUID})
	if err != nil || journal.Status.Phase != cleanupapi.PhaseConfirmingAbsence || journal.Status.AbsenceProof == nil {
		t.Fatalf("journal=%#v err=%v", journal, err)
	}
	pool, err := dynamicClient.Resource(volumeapi.PoolResource).Get(ctx, copy.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(pool.Object, []any{}, "status", "inventory", "copies"); err != nil {
		t.Fatal(err)
	}
	if _, err := dynamicClient.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.DeleteVolume(ctx, request); err != nil {
		t.Fatalf("fresh exact absence did not settle delete: %v", err)
	}
	if registry.exists || registry.finalizersRemoved != 1 || reclaimer.calls != 1 {
		t.Fatalf("settled delete state: exists=%t finalizers=%d effects=%d", registry.exists, registry.finalizersRemoved, reclaimer.calls)
	}
}

func TestCreateVolumeIsBlockedByUnresolvedCleanupFence(t *testing.T) {
	request := validCreateRequest("worker-a")
	volumeID, err := volume.IDFromName(request.Name)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds)
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	target := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: volumeID,
		VolumeUID: "old-volume-uid", CopyID: "old-copy", NodeName: "worker-a", Role: volume.RoleServing,
	}
	dynamicClient = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, cleanupParentVolume(volumeID, target))
	cleanups = &cleanupapi.Store{Client: dynamicClient}
	if _, err := cleanups.Ensure(context.Background(), cleanupapi.Spec{
		OperationID: "delete-old-volume", Target: target, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: volumeID, UID: target.VolumeUID},
	}); err != nil {
		t.Fatal(err)
	}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Cleanups: cleanups,
	})
	if _, err := service.CreateVolume(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unresolved cleanup did not fence volume recreation: %v", err)
	}
}

func TestDeleteVolumeConvergesAfterAcceptedVolumeStateDeleteTimeout(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-1123456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &retryDeleteVolumeRegistry{
		state:  volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy},
		exists: true, deleteFirst: true,
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, cleanupParentVolume(volumeID, copy))
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	reclaimer := &receiptCleanupOperator{}
	client := fake.NewClientset()
	service := &Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: cleanups, CleanupOperator: reclaimer,
	}
	request := &csi.DeleteVolumeRequest{VolumeId: volumeID}
	if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("first delete code=%s err=%v", status.Code(err), err)
	}
	if !registry.exists || registry.finalizersRemoved != 0 || !registry.deletionRequested {
		t.Fatalf("accepted delete timeout lost protection: exists=%t finalizers=%d requested=%t", registry.exists, registry.finalizersRemoved, registry.deletionRequested)
	}
	if _, err := service.DeleteVolume(ctx, request); err != nil {
		t.Fatalf("retry did not converge: %v", err)
	}
	if registry.exists || reclaimer.calls != 1 {
		t.Fatalf("exists=%t cleanup calls=%d", registry.exists, reclaimer.calls)
	}
}

func TestDeleteVolumeIsIdempotentAfterMetadataIsGone(t *testing.T) {
	volumeID := "shiftpv-2123456789abcdef0123456789abcdef"
	registry := &retryDeleteVolumeRegistry{exists: false}
	reclaimer := &receiptCleanupOperator{}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: registry, CleanupOperator: reclaimer,
	})
	if _, err := service.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	if reclaimer.calls != 0 || registry.deleteCalls != 0 {
		t.Fatalf("completed delete repeated effects: cleanup=%d volumeDelete=%d", reclaimer.calls, registry.deleteCalls)
	}
}

func TestDeleteVolumePreservesReservationWithoutExactCopyState(t *testing.T) {
	volumeID := "shiftpv-3123456789abcdef0123456789abcdef"
	registry := &retryDeleteVolumeRegistry{
		state: volumeapi.State{UID: "volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a"}, exists: true,
	}
	client := fake.NewClientset()
	service := configuredService(&Service{
		Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
	})
	if _, err := service.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volumeID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing exact copy state was accepted: %v", err)
	}
}

func TestConcurrentCreateVolumeCallsSerializeExactCopyEffect(t *testing.T) {
	operator := &blockingCreateOperator{started: make(chan struct{}, 2), release: make(chan struct{})}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator})
	request := validCreateRequest("worker-a")
	done := make(chan error, 2)
	go func() {
		_, err := service.CreateVolume(context.Background(), request)
		done <- err
	}()
	<-operator.started
	go func() {
		_, err := service.CreateVolume(context.Background(), request)
		done <- err
	}()
	select {
	case <-operator.started:
		close(operator.release)
		t.Fatal("second exact copy effect crossed the volume lifecycle lock")
	case <-time.After(50 * time.Millisecond):
	}
	close(operator.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentDeleteVolumeCallsRunOneExactCleanup(t *testing.T) {
	volumeID := "shiftpv-4123456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &retryDeleteVolumeRegistry{
		state:  volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy},
		exists: true, deleteCalls: 1,
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, cleanupParentVolume(volumeID, copy))
	cleaner := &blockingCleanupOperator{started: make(chan struct{}, 1), release: make(chan struct{})}
	service := &Service{
		Client:    fake.NewClientset(),
		Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: &cleanupapi.Store{Client: dynamicClient}, CleanupOperator: cleaner,
	}
	request := &csi.DeleteVolumeRequest{VolumeId: volumeID}
	done := make(chan error, 2)
	go func() {
		_, err := service.DeleteVolume(context.Background(), request)
		done <- err
	}()
	<-cleaner.started
	go func() {
		_, err := service.DeleteVolume(context.Background(), request)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if cleaner.calls.Load() != 1 {
		close(cleaner.release)
		t.Fatalf("concurrent cleanup effects=%d", cleaner.calls.Load())
	}
	close(cleaner.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if cleaner.calls.Load() != 1 || registry.exists {
		t.Fatalf("cleanup calls=%d volume exists=%t", cleaner.calls.Load(), registry.exists)
	}
}

func (o *identityCreateOperator) CreateCopy(context.Context, volume.CopyIdentity) error {
	*o.events = append(*o.events, "effect")
	return o.createErr
}

func (o *identityCreateOperator) FinalizeCreate(context.Context, volume.CopyIdentity) error {
	*o.events = append(*o.events, "settle-helper")
	return o.finalizeErr
}

func TestCreateVolumeOrdersDurableIntentEffectAndCompletion(t *testing.T) {
	events := []string{}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &durableCreateRegistry{state: volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhasePending, OwnerNode: copy.NodeName, CurrentCopy: &copy}, events: &events}
	registry.poolNodes = []string{"worker-a"}
	operator := &identityCreateOperator{events: &events}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator, Volumes: registry})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(events, ","); got != "intent,effect,complete,settle-helper" {
		t.Fatalf("lifecycle order=%s", got)
	}
}

func TestCreateVolumeNeverRunsEffectWithoutDurableIntent(t *testing.T) {
	events := []string{}
	registry := &durableCreateRegistry{events: &events, beginErr: apierrors.NewTimeoutError("lost response", 1)}
	operator := &identityCreateOperator{events: &events}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator, Volumes: registry})
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("intent failure code=%s err=%v", status.Code(err), err)
	}
	if got := strings.Join(events, ","); got != "intent" {
		t.Fatalf("effect ran without intent: %s", got)
	}
}

func TestCreateVolumeDoesNotRunEffectAfterCopyConflict(t *testing.T) {
	events := []string{}
	registry := &durableCreateRegistry{events: &events, beginErr: volumeapi.ErrPoolCopyConflict}
	client := fake.NewClientset()
	service := configuredService(&Service{Client: client, Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry})
	req := validCreateRequest("worker-a")

	_, err := service.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("copy conflict code=%s err=%v", status.Code(err), err)
	}
	if got := strings.Join(events, ","); got != "intent" {
		t.Fatalf("unexpected lifecycle events: %s", got)
	}
}

func TestCreateVolumeSettlesHelperOnlyAfterReadyIsDurable(t *testing.T) {
	events := []string{}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &durableCreateRegistry{
		state:  volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhasePending, OwnerNode: copy.NodeName, CurrentCopy: &copy},
		events: &events, completeErr: apierrors.NewTimeoutError("ready response lost", 1),
	}
	registry.poolNodes = []string{"worker-a"}
	operator := &identityCreateOperator{events: &events}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator, Volumes: registry})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); status.Code(err) != codes.Unavailable {
		t.Fatalf("ready failure code=%s err=%v", status.Code(err), err)
	}
	if got := strings.Join(events, ","); got != "intent,effect,complete" {
		t.Fatalf("helper was settled before Ready became durable: %s", got)
	}
}

func TestCreateVolumeRequiresHelperSettlementBeforeSuccess(t *testing.T) {
	events := []string{}
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
	registry := &durableCreateRegistry{
		state:  volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhasePending, OwnerNode: copy.NodeName, CurrentCopy: &copy},
		events: &events,
	}
	registry.poolNodes = []string{"worker-a"}
	operator := &identityCreateOperator{events: &events, finalizeErr: retryableDirectoryError{errors.New("helper still terminating")}}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator, Volumes: registry})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); status.Code(err) != codes.Unavailable {
		t.Fatalf("settlement failure code=%s err=%v", status.Code(err), err)
	}
	if got := strings.Join(events, ","); got != "intent,effect,complete,settle-helper" {
		t.Fatalf("unexpected lifecycle order: %s", got)
	}
}

func (f *fakeDirectoryOperator) CreateCopy(_ context.Context, identity volume.CopyIdentity) error {
	f.createCalls++
	f.createdNode = identity.NodeName
	f.createdID = identity.VolumeID
	return f.createErr
}

func (f *fakeDirectoryOperator) FinalizeCreate(context.Context, volume.CopyIdentity) error {
	return nil
}

func TestCreateVolumeIsIdempotent(t *testing.T) {
	operator := &fakeDirectoryOperator{}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator})
	req := validCreateRequest("worker-a")

	first, err := service.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Volume.VolumeId != second.Volume.VolumeId {
		t.Fatalf("volume ID changed: %q != %q", first.Volume.VolumeId, second.Volume.VolumeId)
	}
	if operator.createdNode != "worker-a" || operator.createdID != first.Volume.VolumeId {
		t.Fatalf("unexpected directory operation: node=%q id=%q", operator.createdNode, operator.createdID)
	}
}

func TestCreateVolumeRejectsProvisioningDuringUninstallQuiesce(t *testing.T) {
	operator := &fakeDirectoryOperator{}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator, ProvisioningGate: rejectingProvisioningGate{}})
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateVolume code = %s, want Unavailable: %v", status.Code(err), err)
	}
	if operator.createCalls != 0 {
		t.Fatal("quiesced CreateVolume reached the directory operator")
	}
}

func TestCreateVolumeTopologyFollowsMobilityOptIn(t *testing.T) {
	for name, test := range map[string]struct {
		namespace *corev1.Namespace
		wantNodes []string
	}{
		"opted in": {
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "mobile", Labels: map[string]string{MobilityAdmissionLabel: mobilityEnabledValue}}},
			wantNodes: []string{"worker-a", "worker-b"},
		},
		"not opted in": {
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "local"}},
			wantNodes: []string{"worker-a"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			namespace := test.namespace
			wantNodes := test.wantNodes
			registry := &fakeVolumeRegistry{state: volumeapi.State{OwnerNode: "worker-a"}, poolNodes: []string{"worker-a", "worker-b"}}
			service := configuredService(&Service{Client: fake.NewClientset(namespace), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry})
			req := validCreateRequest("worker-a")
			req.Parameters[PVCNameKey] = "claim"
			req.Parameters[PVCNamespaceKey] = namespace.Name
			req.Parameters[PVNameKey] = "pv-test"
			response, err := service.CreateVolume(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got := topologyNodes(response.Volume.AccessibleTopology); !equalStrings(got, wantNodes) {
				t.Fatalf("accessible topology = %v, want %v", got, wantNodes)
			}
		})
	}
}

func TestCreateVolumeWithoutProvisionerMetadataStaysOwnerLocal(t *testing.T) {
	registry := &fakeVolumeRegistry{state: volumeapi.State{OwnerNode: "worker-a"}, poolNodes: []string{"worker-a", "worker-b"}}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}, Volumes: registry})
	response, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := topologyNodes(response.Volume.AccessibleTopology); !equalStrings(got, []string{"worker-a"}) {
		t.Fatalf("accessible topology = %v, want owner only", got)
	}
}

func TestCreateVolumeRejectsChangedSelectedNode(t *testing.T) {
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}})
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); err != nil {
		t.Fatal(err)
	}
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-b"))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestCreateVolumeAcceptsLegacyCapacityEnforcementParameter(t *testing.T) {
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}})
	req := validCreateRequest("worker-a")
	req.Parameters[CapacityEnforcementKey] = capacityEnforcementNone
	if _, err := service.CreateVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestCreateVolumeRejectsInvalidRequests(t *testing.T) {
	tests := map[string]func(*csi.CreateVolumeRequest){
		"missing name":     func(req *csi.CreateVolumeRequest) { req.Name = "" },
		"missing capacity": func(req *csi.CreateVolumeRequest) { req.CapacityRange = nil },
		"zero capacity": func(req *csi.CreateVolumeRequest) {
			req.CapacityRange.RequiredBytes = 0
		},
		"capacity above limit": func(req *csi.CreateVolumeRequest) {
			req.CapacityRange.LimitBytes = req.CapacityRange.RequiredBytes - 1
		},
		"missing capabilities": func(req *csi.CreateVolumeRequest) {
			req.VolumeCapabilities = nil
		},
		"raw block": func(req *csi.CreateVolumeRequest) {
			req.VolumeCapabilities[0].AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
		},
		"unsupported access mode": func(req *csi.CreateVolumeRequest) {
			req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
		},
		"missing topology": func(req *csi.CreateVolumeRequest) {
			req.AccessibilityRequirements = nil
		},
		"topology without ShiftPV key": func(req *csi.CreateVolumeRequest) {
			req.AccessibilityRequirements.Preferred[0].Segments = map[string]string{"other": "worker-a"}
		},
		"unknown parameter": func(req *csi.CreateVolumeRequest) {
			req.Parameters["unknown"] = "value"
		},
		"unsupported enforcement": func(req *csi.CreateVolumeRequest) {
			req.Parameters[CapacityEnforcementKey] = "hard"
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			req := validCreateRequest("worker-a")
			mutate(req)
			service := &Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{}}
			_, err := service.CreateVolume(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
		})
	}
}

func TestCreateVolumeRejectsUnconfiguredController(t *testing.T) {
	service := &Service{}
	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestCreateVolumeReportsDirectoryFailure(t *testing.T) {
	operator := &fakeDirectoryOperator{createErr: errors.New("mkdir failed")}
	client := fake.NewClientset()
	service := configuredService(&Service{Client: client, Namespace: "shiftpv-system", Operator: operator})
	req := validCreateRequest("worker-a")

	_, err := service.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
	registry := service.Volumes.(*fakeVolumeRegistry)
	if registry.state.RequestName != req.Name || registry.state.InitialNode != "worker-a" {
		t.Fatalf("volume intent was not preserved for retry: %#v", registry.state)
	}
}

func TestCreateVolumeMapsRetryableDirectoryFailureToUnavailable(t *testing.T) {
	operator := &fakeDirectoryOperator{createErr: retryableDirectoryError{errors.New("filesystem unavailable")}}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator})

	_, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestCreateVolumePreservesDeadlineExceededCode(t *testing.T) {
	events := []string{}
	service := configuredService(&Service{
		Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
		Volumes: &durableCreateRegistry{events: &events, beginErr: context.DeadlineExceeded},
	})

	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestKubernetesAPIErrorPreservesRetryAndContextCodes(t *testing.T) {
	tests := map[string]struct {
		err  error
		code codes.Code
	}{
		"canceled":          {err: context.Canceled, code: codes.Canceled},
		"deadline exceeded": {err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
		"timeout":           {err: apierrors.NewTimeoutError("timed out", 1), code: codes.Unavailable},
		"throttled":         {err: apierrors.NewTooManyRequests("slow down", 1), code: codes.Unavailable},
		"unavailable":       {err: apierrors.NewServiceUnavailable("offline"), code: codes.Unavailable},
		"other":             {err: errors.New("invalid response"), code: codes.Internal},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := status.Code(kubernetesAPIError("call Kubernetes", test.err)); got != test.code {
				t.Fatalf("expected %s, got %s", test.code, got)
			}
		})
	}
}

func TestDeleteVolumeRejectsUnconfiguredController(t *testing.T) {
	id, err := volume.IDFromName("pvc-uid")
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&Service{}).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestControllerCapabilitiesAdvertiseCreateDeleteOnly(t *testing.T) {
	response, err := (&Service{}).ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Capabilities) != 1 || response.Capabilities[0].GetRpc().GetType() != csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME {
		t.Fatalf("unexpected capabilities: %#v", response.Capabilities)
	}
}

func TestValidateVolumeCapabilitiesReturnsMessageForUnsupportedMode(t *testing.T) {
	id, err := volume.IDFromName("pvc-uid")
	if err != nil {
		t.Fatal(err)
	}
	capability := validCreateRequest("worker-a").VolumeCapabilities[0]
	capability.AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	response, err := (&Service{}).ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           id,
		VolumeCapabilities: []*csi.VolumeCapability{capability},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Confirmed != nil || response.Message == "" {
		t.Fatalf("expected unsupported response message, got %#v", response)
	}
}

type rejectingProvisioningGate struct{}

func (rejectingProvisioningGate) Enter() (func(), error) {
	return nil, errors.New("quiescing")
}

func validCreateRequest(node string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:          "pvc-uid",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 64 << 20},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		Parameters: map[string]string{},
		AccessibilityRequirements: &csi.TopologyRequirement{Preferred: []*csi.Topology{{
			Segments: map[string]string{TopologyKey: node},
		}}},
	}
}

func topologyNodes(topologies []*csi.Topology) []string {
	result := make([]string, 0, len(topologies))
	for _, topology := range topologies {
		result = append(result, topology.Segments[TopologyKey])
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
