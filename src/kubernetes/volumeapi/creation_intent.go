package volumeapi

import (
	"context"
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// PoolReference is the immutable reservation recorded with the Volume spec,
// before the API-assigned Volume UID can be used to initialize currentCopy.
type PoolReference struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

var ErrVolumeFinalizersPending = errors.New("other Volume finalizers are still present")

// Initializing distinguishes an unstarted creation from damaged execution state.
// Legacy objects may lack InitialPool and must pass capacity admission again.
func (s State) Initializing() bool {
	return !s.Deleting && s.UnstartedCreation()
}

// HasInitialReservation is an accounting fact, not permission to initialize.
// A DELETE before the first status write must retain its admitted Pool's hold
// until finalization removes the object. No execution evidence may be present.
func (s State) HasInitialReservation() bool {
	return s.InitialPool != nil && volume.ValidIdentityToken(s.InitialPool.Name) &&
		volume.ValidIdentityToken(s.InitialPool.UID) && s.UnstartedCreation()
}

// UnstartedCreation has never authorized a copy or executor. Its protection
// remains installed until cancellation removes the object and its reservation.
func (s State) UnstartedCreation() bool {
	return volume.ValidIdentityToken(s.UID) && s.RequestName != "" && s.InitialNode != "" && s.CapacityBytes > 0 &&
		slices.Contains(s.Finalizers, VolumeProtectionFinalizer) && !s.HasCleanup &&
		s.Phase == "" && s.OwnerNode == "" && s.ActiveMove == "" && len(s.PublishedNodes) == 0 &&
		s.CurrentCopy == nil && s.CreationOperationID == "" && s.DeletionOperationID == "" &&
		s.CreationExecutor == nil && s.CreationReceipt == nil
}

// CancelUnstartedCreation fences initialization with an exact UID/RV DELETE.
// No filesystem effect was approved, so no disk receipt is invented. Once the
// timestamp is stored, initialization cannot win a later status CAS.
func (r *Registry) CancelUnstartedCreation(ctx context.Context, id, uid string) error {
	object, err := r.getVolume(ctx, id)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	state, err := stateFrom(object)
	if err != nil {
		return err
	}
	if uid == "" || state.UID != uid || !state.UnstartedCreation() {
		return ErrStateConflict
	}
	if !state.Deleting {
		if object.GetResourceVersion() == "" {
			return ErrStateConflict
		}
		identity, revision := types.UID(uid), object.GetResourceVersion()
		if err := r.Client.Resource(VolumeResource).Delete(ctx, id, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &identity, ResourceVersion: &revision}}); err != nil {
			return err
		}
	}
	return r.finalizeUnstartedCreation(ctx, id, uid)
}

func (r *Registry) finalizeUnstartedCreation(ctx context.Context, id, uid string) error {
	err := r.mutateObject(ctx, objectMutation{
		resource: VolumeResource, kind: "ShiftPVVolume", name: id, uid: uid, backoff: retry.DefaultBackoff,
		readError: "read unstarted creation cancellation", writeError: "release unstarted creation protection",
		apply: func(object *unstructured.Unstructured) error {
			current, err := stateFrom(object)
			if err != nil {
				return err
			}
			if !current.Deleting || !current.UnstartedCreation() {
				return ErrStateConflict
			}
			// Keep a well-formed capacity owner while a foreign controller's
			// finalizer delays object removal. A concurrent addition loses RV CAS.
			if len(current.Finalizers) != 1 {
				return ErrVolumeFinalizersPending
			}
			object.SetFinalizers(nil)
			return nil
		},
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
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
