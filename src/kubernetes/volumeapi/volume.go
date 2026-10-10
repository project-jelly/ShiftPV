package volumeapi

import (
	"context"
	"fmt"
	"slices"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
	"github.com/project-jelly/ShiftPV/src/volume/deletion"
)

type State struct {
	UID                 string
	Finalizers          []string
	RequestName         string
	CapacityBytes       int64
	InitialNode         string
	Phase               string
	OwnerNode           string
	ActiveMove          string
	PublishedNodes      []string
	CreationOperationID string
	DeletionOperationID string
	CurrentCopy         *volume.CopyIdentity
	CreationExecutor    *NodeExecutor
	CreationReceipt     *CreationReceipt
}

func CreationOperationID(volumeUID string) (string, error) {
	operationID := "create-" + volumeUID
	if !volume.ValidIdentityToken(volumeUID) || !volume.ValidIdentityToken(operationID) {
		return "", fmt.Errorf("invalid volume creation operation identity")
	}
	return operationID, nil
}

// BeginCreate persists the exact copy identity before node-local filesystem work.
func (r *Registry) BeginCreate(ctx context.Context, volumeID, requestName, ownerNode string, capacityBytes int64) (State, error) {
	return r.beginCreate(ctx, volumeID, requestName, ownerNode, capacityBytes, "", "")
}

// BeginCreateInPool anchors a new copy to the Pool selected by capacity
// admission. A retry must name the same Pool incarnation as the durable copy.
func (r *Registry) BeginCreateInPool(ctx context.Context, volumeID, requestName, ownerNode string, capacityBytes int64, poolName, poolUID string) (State, error) {
	if poolName == "" || poolUID == "" {
		return State{}, ErrPoolConfiguration
	}
	return r.beginCreate(ctx, volumeID, requestName, ownerNode, capacityBytes, poolName, poolUID)
}

func (r *Registry) beginCreate(ctx context.Context, volumeID, requestName, ownerNode string, capacityBytes int64, poolName, poolUID string) (State, error) {
	if err := r.validate(); err != nil {
		return State{}, err
	}
	if requestName == "" || ownerNode == "" || capacityBytes <= 0 {
		return State{}, ErrStateConflict
	}
	resource := r.Client.Resource(VolumeResource)
	object, err := resource.Get(ctx, volumeID, metav1.GetOptions{})
	create := apierrors.IsNotFound(err)
	if err == nil {
		state, stateErr := stateFrom(object)
		if stateErr != nil {
			return State{}, stateErr
		}
		if state.Phase != "" {
			return r.resumeCreate(ctx, object, state, volumeID, requestName, ownerNode, capacityBytes, poolName, poolUID)
		}
	} else if !apierrors.IsNotFound(err) {
		return State{}, fmt.Errorf("read ShiftPVVolume creation intent: %w", err)
	}

	installationID, pool, err := r.freshCreationPlacement(ctx, volumeID, ownerNode, poolName, poolUID)
	if err != nil {
		return State{}, err
	}
	if create {
		object, err = resource.Create(ctx, newVolumeObject(volumeID, requestName, ownerNode, capacityBytes), metav1.CreateOptions{})
	}
	if err != nil {
		return State{}, fmt.Errorf("begin ShiftPVVolume creation: %w", err)
	}
	copy, operationID, err := initialServingCopy(object, pool, installationID, volumeID, ownerNode)
	if err != nil {
		return State{}, err
	}
	state, err := stateFrom(object)
	if err != nil {
		return State{}, err
	}
	if state.Phase == "" {
		state, err = r.recordCreationIntent(ctx, volumeID, State{
			UID:                 string(object.GetUID()),
			RequestName:         requestName,
			CapacityBytes:       capacityBytes,
			InitialNode:         ownerNode,
			Phase:               PhasePending,
			OwnerNode:           ownerNode,
			CreationOperationID: operationID,
			CurrentCopy:         &copy,
		})
		if err != nil {
			return State{}, err
		}
	}
	return r.resumeCreate(ctx, object, state, volumeID, requestName, ownerNode, capacityBytes, poolName, poolUID)
}

// freshCreationPlacement resolves the installation and the Ready Pool that a
// first serving copy may be placed into.
func (r *Registry) freshCreationPlacement(ctx context.Context, volumeID, ownerNode, poolName, poolUID string) (string, Pool, error) {
	installationID, err := r.InstallationID(ctx)
	if err != nil {
		return "", Pool{}, err
	}
	var pool Pool
	if poolName == "" {
		pool, err = r.ReadyPoolForNode(ctx, ownerNode)
	} else {
		pool, err = r.ReadyPoolForIdentity(ctx, poolName, poolUID, ownerNode)
	}
	if err != nil {
		return "", Pool{}, err
	}
	if PoolHasServingVolume(pool, volumeID) {
		return "", Pool{}, fmt.Errorf("%w: Pool %q already contains volume %q", ErrPoolCopyConflict, pool.Name, volumeID)
	}
	return installationID, pool, nil
}

func newVolumeObject(volumeID, requestName, ownerNode string, capacityBytes int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       "ShiftPVVolume",
		"metadata":   map[string]any{"name": volumeID, "finalizers": []any{VolumeProtectionFinalizer}},
		"spec":       map[string]any{"volumeID": volumeID, "requestName": requestName, "initialNode": ownerNode, "capacityBytes": capacityBytes},
	}}
}

// initialServingCopy builds the exact identity the first serving copy carries,
// together with the durable creation operation derived from the object UID.
func initialServingCopy(object *unstructured.Unstructured, pool Pool, installationID, volumeID, ownerNode string) (volume.CopyIdentity, string, error) {
	if object.GetUID() == "" || pool.UID == "" {
		return volume.CopyIdentity{}, "", fmt.Errorf("%w: Kubernetes object identity is missing", ErrStateConflict)
	}
	operationID, err := CreationOperationID(string(object.GetUID()))
	if err != nil {
		return volume.CopyIdentity{}, "", fmt.Errorf("%w: %v", ErrStateConflict, err)
	}
	copy := volume.CopyIdentity{
		InstallationID: installationID,
		PoolName:       pool.Name,
		PoolUID:        pool.UID,
		VolumeID:       volumeID,
		VolumeUID:      string(object.GetUID()),
		CopyID:         "initial-" + string(object.GetUID()),
		NodeName:       ownerNode,
		Role:           volume.RoleServing,
	}
	if err := copy.Validate(); err != nil {
		return volume.CopyIdentity{}, "", fmt.Errorf("build creation identity: %w", err)
	}
	return copy, operationID, nil
}

// recordCreationIntent publishes the creation intent exactly once. A concurrent
// writer that already recorded a phase wins, and its state is read back.
func (r *Registry) recordCreationIntent(ctx context.Context, volumeID string, next State) (State, error) {
	if err := r.mutateState(ctx, volumeID, func(current State) (State, error) {
		if current.UID != next.UID {
			return State{}, fmt.Errorf("%w: ShiftPVVolume %q UID changed from %q to %q", ErrStateConflict, volumeID, next.UID, current.UID)
		}
		if current.Phase != "" {
			return current, nil
		}
		return next, nil
	}); err != nil {
		return State{}, err
	}
	return r.Get(ctx, volumeID)
}

func (r *Registry) resumeCreate(ctx context.Context, object *unstructured.Unstructured, state State, volumeID, requestName, ownerNode string, capacityBytes int64, poolName, poolUID string) (State, error) {
	if !sameCreationIntent(object, state, volumeID, requestName, ownerNode, capacityBytes) {
		return State{}, fmt.Errorf("%w: volume creation identity changed", ErrStateConflict)
	}
	operationID, err := CreationOperationID(state.UID)
	if err != nil || state.CreationOperationID != operationID {
		return State{}, fmt.Errorf("%w: volume creation operation changed", ErrStateConflict)
	}
	installationID, err := r.InstallationID(ctx)
	if err != nil {
		return State{}, err
	}
	if poolName != "" && (state.CurrentCopy.PoolName != poolName || state.CurrentCopy.PoolUID != poolUID) {
		return State{}, fmt.Errorf("%w: volume creation Pool differs from selected Pool", ErrStateConflict)
	}
	pool, err := r.protectedCreationPool(ctx, *state.CurrentCopy)
	if err != nil {
		return State{}, err
	}
	if state.CurrentCopy.InstallationID != installationID || state.CurrentCopy.PoolName != pool.Name || state.CurrentCopy.PoolUID != pool.UID {
		return State{}, fmt.Errorf("%w: volume creation Pool identity changed", ErrStateConflict)
	}
	return state, nil
}

// sameCreationIntent reports whether the live state still describes the exact
// creation this call was asked to resume.
func sameCreationIntent(object *unstructured.Unstructured, state State, volumeID, requestName, ownerNode string, capacityBytes int64) bool {
	if object == nil || object.GetUID() == "" || state.UID != string(object.GetUID()) {
		return false
	}
	if state.OwnerNode != ownerNode || state.RequestName != requestName || state.InitialNode != ownerNode || state.CapacityBytes != capacityBytes {
		return false
	}
	if state.Phase != PhasePending && state.Phase != PhaseNodeCreating && state.Phase != PhaseReady {
		return false
	}
	return servingCopyFor(state.CurrentCopy, volumeID, state.UID, ownerNode)
}

func servingCopyFor(copy *volume.CopyIdentity, volumeID, volumeUID, nodeName string) bool {
	return copy != nil && copy.Validate() == nil && copy.VolumeID == volumeID &&
		copy.VolumeUID == volumeUID && copy.NodeName == nodeName && copy.Role == volume.RoleServing
}

// protectedCreationPool reads the owner's Pool and requires the live protection
// finalizer, so an in-flight creation cannot outlive its Pool.
func (r *Registry) protectedCreationPool(ctx context.Context, copy volume.CopyIdentity) (Pool, error) {
	pool, err := r.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return Pool{}, err
	}
	if pool.DeletionTimestamp != nil || !slices.Contains(pool.Finalizers, PoolProtectionFinalizer) {
		return Pool{}, fmt.Errorf("%w: creation Pool protection is unavailable", ErrPoolNotReady)
	}
	return pool, nil
}

func (r *Registry) CompleteCreate(ctx context.Context, volumeID, uid string, copy volume.CopyIdentity) error {
	operationID, err := CreationOperationID(uid)
	if err != nil {
		return ErrStateConflict
	}
	return r.mutateState(ctx, volumeID, func(current State) (State, error) {
		if uid == "" || current.UID != uid || current.CurrentCopy == nil || *current.CurrentCopy != copy ||
			current.CreationOperationID != operationID || current.OwnerNode != copy.NodeName {
			return State{}, ErrStateConflict
		}
		if current.Phase == PhaseReady {
			return current, nil
		}
		if current.Phase != PhasePending && current.Phase != PhaseNodeCreating {
			return State{}, ErrStateConflict
		}
		if current.Phase == PhaseNodeCreating && !ValidCreationReceipt(current) {
			return State{}, ErrStateConflict
		}
		current.Phase = PhaseReady
		return current, nil
	})
}

// BeginDelete fences new publications before an approved filesystem cleanup
// can be created. The deletion operation is durable and idempotent across
// controller restarts.
func (r *Registry) BeginDelete(ctx context.Context, volumeID, uid string, copy volume.CopyIdentity) (State, error) {
	operationID := "delete-" + uid
	if uid == "" || copy.Validate() != nil || copy.VolumeID != volumeID || copy.VolumeUID != uid {
		return State{}, ErrStateConflict
	}
	err := r.mutateState(ctx, volumeID, func(current State) (State, error) {
		return fenceForDeletion(current, uid, operationID, copy)
	})
	if err != nil {
		return State{}, err
	}
	state, err := r.Get(ctx, volumeID)
	if err != nil {
		return State{}, err
	}
	if state.Phase != PhaseDeleting || state.DeletionOperationID != operationID || state.UID != uid || state.CurrentCopy == nil || *state.CurrentCopy != copy {
		return State{}, ErrStateConflict
	}
	return state, nil
}

// fenceForDeletion admits exactly one deletion operation per live copy and is
// idempotent only for the operation that already owns the fence.
func fenceForDeletion(current State, uid, operationID string, copy volume.CopyIdentity) (State, error) {
	decision := deletion.Decide(deletion.Observation{
		UID: current.UID, OwnerNode: current.OwnerNode, Phase: current.Phase,
		ActiveMove: current.ActiveMove, OperationID: current.DeletionOperationID,
		CurrentCopy: current.CurrentCopy, PublishedNodes: current.PublishedNodes,
	}, uid, operationID, copy)
	switch decision {
	case deletion.WaitForUnpublish:
		return State{}, fmt.Errorf("%w: %w", ErrStateConflict, ErrVolumePublished)
	case deletion.Begin:
		current.Phase = PhaseDeleting
		current.DeletionOperationID = operationID
	case deletion.Resume:
	default:
		return State{}, ErrStateConflict
	}
	return current, nil
}

func (r *Registry) Get(ctx context.Context, volumeID string) (State, error) {
	object, err := r.getVolume(ctx, volumeID)
	if err != nil {
		return State{}, err
	}
	return stateFrom(object)
}

func (r *Registry) ListVolumes(ctx context.Context) (map[string]State, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	list, err := r.Client.Resource(VolumeResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ShiftPVVolume: %w", err)
	}
	result := make(map[string]State, len(list.Items))
	for index := range list.Items {
		state, stateErr := stateFrom(&list.Items[index])
		if stateErr != nil {
			return nil, fmt.Errorf("decode ShiftPVVolume %q: %w", list.Items[index].GetName(), stateErr)
		}
		result[list.Items[index].GetName()] = state
	}
	return result, nil
}

func (r *Registry) Delete(ctx context.Context, volumeID, uid string) error {
	if err := r.validate(); err != nil {
		return err
	}
	if uid == "" {
		return fmt.Errorf("ShiftPVVolume UID is required for deletion")
	}
	precondition := types.UID(uid)
	err := r.Client.Resource(VolumeResource).Delete(ctx, volumeID, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &precondition}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ShiftPVVolume: %w", err)
	}
	return nil
}

func (r *Registry) RemoveVolumeFinalizer(ctx context.Context, volumeID, uid string) error {
	return r.updateObjectFinalizer(ctx, VolumeResource, volumeID, uid, VolumeProtectionFinalizer, false)
}

func (r *Registry) CompareAndSetState(ctx context.Context, volumeID, expectedPhase, expectedActiveMove, expectedOwner string, next State) error {
	return r.mutateState(ctx, volumeID, func(current State) (State, error) {
		if next.UID == "" || current.UID != next.UID || current.Phase != expectedPhase || current.ActiveMove != expectedActiveMove || current.OwnerNode != expectedOwner {
			return State{}, fmt.Errorf("%w: volume %q is phase=%q activeMove=%q owner=%q", ErrStateConflict, volumeID, current.Phase, current.ActiveMove, current.OwnerNode)
		}
		// Node publication is independently maintained by the CSI node service.
		// A controller's earlier observation must not erase a concurrent publish/unpublish.
		next.PublishedNodes = current.PublishedNodes
		if next.Phase == PhaseReady {
			for _, node := range current.PublishedNodes {
				if node != next.OwnerNode {
					return State{}, fmt.Errorf("%w: node %q is still published", ErrStateConflict, node)
				}
			}
		}
		return next, nil
	})
}

func (r *Registry) BeginPublish(ctx context.Context, volumeID, nodeName string, copy volume.CopyIdentity) error {
	return r.mutateState(ctx, volumeID, func(state State) (State, error) {
		if state.Phase != PhaseReady || state.OwnerNode != nodeName || state.CurrentCopy == nil || *state.CurrentCopy != copy {
			return State{}, fmt.Errorf("%w: volume is not publishable by this copy", ErrStateConflict)
		}
		nodes := make(map[string]struct{}, len(state.PublishedNodes)+1)
		for _, node := range state.PublishedNodes {
			nodes[node] = struct{}{}
		}
		nodes[nodeName] = struct{}{}
		state.PublishedNodes = state.PublishedNodes[:0]
		for node := range nodes {
			state.PublishedNodes = append(state.PublishedNodes, node)
		}
		sort.Strings(state.PublishedNodes)
		return state, nil
	})
}

// ReconcilePublished changes publication state only for the exact live copy.
// It is used after inspecting real mount references under the node-local lock.
func (r *Registry) ReconcilePublished(ctx context.Context, volumeID, nodeName string, copy volume.CopyIdentity, published bool) error {
	return r.mutateState(ctx, volumeID, func(state State) (State, error) {
		if copy.Validate() != nil || copy.Role != volume.RoleServing || copy.VolumeID != volumeID || copy.NodeName != nodeName ||
			state.UID != copy.VolumeUID || state.OwnerNode != nodeName || state.CurrentCopy == nil || *state.CurrentCopy != copy {
			return State{}, fmt.Errorf("%w: publication copy identity changed", ErrStateConflict)
		}
		if published && state.Phase != PhaseReady {
			return State{}, fmt.Errorf("%w: volume is not publishable", ErrStateConflict)
		}
		nodes := make(map[string]struct{}, len(state.PublishedNodes)+1)
		for _, node := range state.PublishedNodes {
			nodes[node] = struct{}{}
		}
		if published {
			nodes[nodeName] = struct{}{}
		} else {
			delete(nodes, nodeName)
		}
		state.PublishedNodes = state.PublishedNodes[:0]
		for node := range nodes {
			state.PublishedNodes = append(state.PublishedNodes, node)
		}
		sort.Strings(state.PublishedNodes)
		return state, nil
	})
}

func (r *Registry) mutateState(ctx context.Context, volumeID string, mutate func(State) (State, error)) error {
	return r.mutateObject(ctx, objectMutation{
		resource:   VolumeResource,
		kind:       "ShiftPVVolume",
		name:       volumeID,
		backoff:    retry.DefaultBackoff,
		readError:  "get ShiftPVVolume for status update",
		writeError: "update ShiftPVVolume status",
		status:     true,
		apply: func(object *unstructured.Unstructured) error {
			current, err := stateFrom(object)
			if err != nil {
				return err
			}
			next, err := mutate(current)
			if err != nil {
				return err
			}
			if next.CreationOperationID == "" {
				next.CreationOperationID = current.CreationOperationID
			}
			if next.DeletionOperationID == "" {
				next.DeletionOperationID = current.DeletionOperationID
			}
			if next.CurrentCopy == nil {
				next.CurrentCopy = current.CurrentCopy
			}
			if next.CreationExecutor == nil {
				next.CreationExecutor = current.CreationExecutor
			}
			if next.CreationReceipt == nil {
				next.CreationReceipt = current.CreationReceipt
			}
			setState(object, next)
			return nil
		},
	})
}

func (r *Registry) getVolume(ctx context.Context, volumeID string) (*unstructured.Unstructured, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	object, err := r.Client.Resource(VolumeResource).Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get ShiftPVVolume: %w", err)
	}
	return object, nil
}

func stateFrom(object *unstructured.Unstructured) (State, error) {
	requestName, _, _ := unstructured.NestedString(object.Object, "spec", "requestName")
	initialNode, _, _ := unstructured.NestedString(object.Object, "spec", "initialNode")
	capacityBytes, _, _ := unstructured.NestedInt64(object.Object, "spec", "capacityBytes")
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	ownerNode, _, _ := unstructured.NestedString(object.Object, "status", "ownerNode")
	activeMove, _, _ := unstructured.NestedString(object.Object, "status", "activeMove")
	publishedNodes, _, err := unstructured.NestedStringSlice(object.Object, "status", "publishedNodes")
	if err != nil {
		return State{}, fmt.Errorf("decode publishedNodes: %w", err)
	}
	creationOperationID, _, _ := unstructured.NestedString(object.Object, "status", "creationOperationID")
	deletionOperationID, _, _ := unstructured.NestedString(object.Object, "status", "deletionOperationID")
	var executor *NodeExecutor
	var receipt *CreationReceipt
	if err := decodeStatusField(object, "creationExecutor", &executor); err != nil {
		return State{}, err
	}
	if err := decodeStatusField(object, "creationReceipt", &receipt); err != nil {
		return State{}, err
	}
	var currentCopy *volume.CopyIdentity
	if data, found, nestedErr := unstructured.NestedMap(object.Object, "status", "currentCopy"); nestedErr != nil {
		return State{}, nestedErr
	} else if found {
		var copy volume.CopyIdentity
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(data, &copy); err != nil {
			return State{}, fmt.Errorf("decode current copy: %w", err)
		}
		if err := copy.Validate(); err != nil {
			return State{}, err
		}
		currentCopy = &copy
	}
	return State{
		UID: string(object.GetUID()), Finalizers: append([]string(nil), object.GetFinalizers()...),
		RequestName: requestName, CapacityBytes: capacityBytes, InitialNode: initialNode,
		Phase: phase, OwnerNode: ownerNode, ActiveMove: activeMove,
		CreationExecutor: executor, CreationReceipt: receipt,
		PublishedNodes: publishedNodes, CreationOperationID: creationOperationID, DeletionOperationID: deletionOperationID, CurrentCopy: currentCopy,
	}, nil
}

func setState(object *unstructured.Unstructured, state State) {
	previous, _ := object.Object["status"].(map[string]any)
	cleanup := previous["cleanup"]
	next := map[string]any{
		"phase": state.Phase, "ownerNode": state.OwnerNode, "activeMove": state.ActiveMove,
		"publishedNodes": stringSliceToAny(state.PublishedNodes),
	}
	object.Object["status"] = next
	status := next
	if cleanup != nil {
		status["cleanup"] = cleanup
	}
	if state.CreationOperationID != "" {
		status["creationOperationID"] = state.CreationOperationID
	}
	if state.DeletionOperationID != "" {
		status["deletionOperationID"] = state.DeletionOperationID
	}
	setStatusField(object, "creationExecutor", state.CreationExecutor)
	setStatusField(object, "creationReceipt", state.CreationReceipt)
	if state.CurrentCopy != nil {
		if encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(state.CurrentCopy); err == nil {
			status["currentCopy"] = encoded
		}
	}
}
