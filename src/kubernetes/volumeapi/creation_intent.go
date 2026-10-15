package volumeapi

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// PoolReference is the immutable reservation recorded with the Volume spec,
// before the API-assigned Volume UID can be used to initialize currentCopy.
type PoolReference struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// Initializing distinguishes an unstarted creation from damaged execution state.
// Legacy objects may lack InitialPool and must pass capacity admission again.
func (s State) Initializing() bool {
	return volume.ValidIdentityToken(s.UID) && s.RequestName != "" && s.InitialNode != "" && s.CapacityBytes > 0 &&
		slices.Contains(s.Finalizers, VolumeProtectionFinalizer) && !s.Deleting && !s.HasCleanup &&
		s.Phase == "" && s.OwnerNode == "" && s.ActiveMove == "" && len(s.PublishedNodes) == 0 &&
		s.CurrentCopy == nil && s.CreationOperationID == "" && s.DeletionOperationID == "" &&
		s.CreationExecutor == nil && s.CreationReceipt == nil
}

func initialPoolFrom(object *unstructured.Unstructured) (*PoolReference, error) {
	data, found, err := unstructured.NestedMap(object.Object, "spec", "initialPool")
	if err != nil || !found {
		return nil, err
	}
	name, _, _ := unstructured.NestedString(data, "name")
	uid, _, _ := unstructured.NestedString(data, "uid")
	if !volume.ValidIdentityToken(name) || !volume.ValidIdentityToken(uid) {
		return nil, fmt.Errorf("%w: invalid initial Pool reservation", ErrStateConflict)
	}
	return &PoolReference{Name: name, UID: uid}, nil
}

func (r *Registry) initializeCreate(ctx context.Context, object *unstructured.Unstructured, state State, id, poolName, poolUID string) (State, error) {
	if state.InitialPool == nil {
		// The caller must hold capacity admission for this legacy, unstarted
		// request. Binding its reservation is durable before any status/effect.
		_, pool, err := r.freshCreationPlacement(ctx, id, state.InitialNode, poolName, poolUID)
		if err != nil {
			return State{}, err
		}
		if err := r.bindInitialPool(ctx, id, state, pool); err != nil {
			return State{}, err
		}
		return r.beginCreate(ctx, id, state.RequestName, state.InitialNode, state.CapacityBytes, pool.Name, pool.UID)
	}
	if poolName != "" && (state.InitialPool.Name != poolName || state.InitialPool.UID != poolUID) {
		return State{}, fmt.Errorf("%w: initial Pool reservation changed", ErrStateConflict)
	}
	pool, err := r.PoolForIdentity(ctx, state.InitialPool.Name, state.InitialPool.UID, state.InitialNode)
	if err != nil {
		return State{}, err
	}
	if pool.DeletionTimestamp != nil || !slices.Contains(pool.Finalizers, PoolProtectionFinalizer) {
		return State{}, fmt.Errorf("%w: creation Pool protection is unavailable", ErrPoolNotReady)
	}
	installationID, err := r.InstallationID(ctx)
	if err != nil {
		return State{}, err
	}
	return r.completeInitialization(ctx, object, state, pool, installationID, id)
}

func (r *Registry) bindInitialPool(ctx context.Context, id string, expected State, pool Pool) error {
	err := r.mutateObject(ctx, objectMutation{
		resource: VolumeResource, kind: "ShiftPVVolume", name: id, backoff: retry.DefaultBackoff,
		readError: "read initial Pool reservation", writeError: "record initial Pool reservation",
		apply: func(object *unstructured.Unstructured) error {
			current, err := stateFrom(object)
			if err != nil {
				return err
			}
			if current.UID != expected.UID || !current.Initializing() || current.RequestName != expected.RequestName ||
				current.InitialNode != expected.InitialNode || current.CapacityBytes != expected.CapacityBytes {
				return ErrStateConflict
			}
			if current.InitialPool != nil && *current.InitialPool != (PoolReference{Name: pool.Name, UID: pool.UID}) {
				return ErrStateConflict
			}
			return unstructured.SetNestedMap(object.Object, map[string]any{"name": pool.Name, "uid": pool.UID}, "spec", "initialPool")
		},
	})
	if err != nil {
		return err
	}
	bound, err := r.Get(ctx, id)
	if err != nil {
		return err
	}
	if bound.UID != expected.UID || bound.InitialPool == nil || *bound.InitialPool != (PoolReference{Name: pool.Name, UID: pool.UID}) {
		return fmt.Errorf("%w: initial Pool reservation was not stored; upgrade the Volume CRD", ErrStateConflict)
	}
	return nil
}

func (r *Registry) completeInitialization(ctx context.Context, object *unstructured.Unstructured, state State, pool Pool, installationID, id string) (State, error) {
	if !state.Initializing() || object.GetDeletionTimestamp() != nil {
		return State{}, ErrStateConflict
	}
	copy, operationID, err := initialServingCopy(object, pool, installationID, id, state.InitialNode)
	if err != nil {
		return State{}, err
	}
	state.Phase, state.OwnerNode = PhasePending, state.InitialNode
	state.CreationOperationID, state.CurrentCopy = operationID, &copy
	state, err = r.recordCreationIntent(ctx, id, state)
	if err != nil {
		return State{}, err
	}
	return r.resumeCreate(ctx, object, state, id, state.RequestName, state.InitialNode, state.CapacityBytes, pool.Name, pool.UID)
}
