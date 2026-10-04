// Package publication owns the node-local publish and unpublish effects that
// run under the per-volume storage lock. It re-verifies the copy identity it
// was handed, records the publication intent and performs the bind mount or
// the publication reconcile, and returns errors that still carry the
// ownership and volumeapi sentinels its caller classifies.
package publication

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var (
	// ErrIdentityUnavailable reports that the copy identity backing the
	// publication can no longer be verified, so publication state must be
	// left untouched.
	ErrIdentityUnavailable = errors.New("publication identity is no longer verifiable")
	// ErrObservationRetry reports that the publication observation was
	// inconclusive and the operation must be retried.
	ErrObservationRetry = errors.New("publication observation must be retried")
)

// Binder performs the node-local mount effects for one target path. Unpublish
// is driven by the CSI adapter before the ownership lock; Publisher itself
// only publishes and inspects.
type Binder interface {
	Publish(source, target string) error
	Unpublish(volumeID, target string) error
	HasPublishedTarget(source, targetRoot string) (bool, error)
}

// VolumeRegistry is the durable publication state the node effects reconcile.
type VolumeRegistry interface {
	Get(context.Context, string) (volumeapi.State, error)
	BeginPublish(context.Context, string, string, volume.CopyIdentity) error
	ReconcilePublished(context.Context, string, string, volume.CopyIdentity, bool) error
}

// PoolResolver returns the exact Pool named by a copy and its host-visible root.
type PoolResolver func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error)

// Publisher runs publish and unpublish effects for one node.
type Publisher struct {
	NodeName   string
	TargetRoot string
	Binder     Binder
	Volumes    VolumeRegistry
	Pools      PoolResolver
}

// PublishRequest is one already validated publish command.
type PublishRequest struct {
	VolumeID   string
	TargetPath string
	Source     string
	PoolRoot   string
	Copy       volume.CopyIdentity
}

// UnpublishRequest is one already validated unpublish command. StateUID is the
// volume UID observed before the lock was taken.
type UnpublishRequest struct {
	VolumeID string
	Source   string
	PoolRoot string
	StateUID string
	Copy     volume.CopyIdentity
}

// Publish records the publication intent and binds the source to the target
// under the storage lock.
func (p Publisher) Publish(ctx context.Context, req PublishRequest) error {
	return ownership.WithLock(ctx, req.PoolRoot, poolIdentity(req.Copy), req.VolumeID, func(store *ownership.Store) error {
		freshPool, freshPoolRoot, poolErr := p.Pools(ctx, req.Copy)
		if poolErr != nil {
			return fmt.Errorf("refresh node pool before publish: %w", poolErr)
		}
		if freshPoolRoot != req.PoolRoot || !CopyMatchesPool(req.Copy, freshPool) {
			return fmt.Errorf("registered node pool changed before publish: %w", volumeapi.ErrStateConflict)
		}
		fresh, getErr := p.Volumes.Get(ctx, req.VolumeID)
		if getErr != nil || fresh.CurrentCopy == nil || *fresh.CurrentCopy != req.Copy || fresh.Phase != volumeapi.PhaseReady || fresh.OwnerNode != p.NodeName {
			return fmt.Errorf("volume publish authority changed: %w", errors.Join(getErr, volumeapi.ErrStateConflict))
		}
		if verifyErr := store.VerifyServing(req.Copy); verifyErr != nil {
			return verifyErr
		}
		if publishErr := p.Volumes.BeginPublish(ctx, req.VolumeID, p.NodeName, req.Copy); publishErr != nil {
			return publishErr
		}
		if publishErr := p.Binder.Publish(req.Source, req.TargetPath); publishErr != nil {
			stillPublished, inspectErr := p.Binder.HasPublishedTarget(req.Source, p.TargetRoot)
			if inspectErr != nil {
				return errors.Join(publishErr, inspectErr)
			}
			if reconcileErr := p.Volumes.ReconcilePublished(ctx, req.VolumeID, p.NodeName, req.Copy, stillPublished); reconcileErr != nil {
				return errors.Join(publishErr, reconcileErr)
			}
			return publishErr
		}
		return nil
	})
}

// Unpublish reconciles the durable publication state after the target was
// already unmounted. It reports whether the storage lock was entered, which
// the caller needs to tell an unreached reconcile from a failed one.
func (p Publisher) Unpublish(ctx context.Context, req UnpublishRequest) (bool, error) {
	enteredLock := false
	err := ownership.WithLock(ctx, req.PoolRoot, poolIdentity(req.Copy), req.VolumeID, func(*ownership.Store) error {
		enteredLock = true
		freshPool, freshPoolRoot, poolErr := p.Pools(ctx, req.Copy)
		if poolErr != nil {
			if PoolIdentityUnavailable(poolErr) {
				return ErrIdentityUnavailable
			}
			return fmt.Errorf("%w: refresh node pool after unpublish: %v", ErrObservationRetry, poolErr)
		}
		if freshPoolRoot != req.PoolRoot || !CopyMatchesPool(req.Copy, freshPool) {
			return ErrIdentityUnavailable
		}
		fresh, getErr := p.Volumes.Get(ctx, req.VolumeID)
		if getErr == nil && fresh.CurrentCopy != nil && fresh.CurrentCopy.Role == volume.RoleServing && fresh.CurrentCopy.NodeName != p.NodeName {
			return nil
		}
		if getErr != nil || fresh.UID != req.StateUID || fresh.CurrentCopy == nil || *fresh.CurrentCopy != req.Copy || fresh.OwnerNode != p.NodeName {
			return fmt.Errorf("volume unpublish authority changed: %w", errors.Join(getErr, volumeapi.ErrStateConflict))
		}
		stillPublished, inspectErr := p.Binder.HasPublishedTarget(req.Source, p.TargetRoot)
		if inspectErr != nil {
			return inspectErr
		}
		return p.Volumes.ReconcilePublished(ctx, req.VolumeID, p.NodeName, req.Copy, stillPublished)
	})
	return enteredLock, err
}

// CopyMatchesPool reports whether the copy identity still names the Pool.
func CopyMatchesPool(copy volume.CopyIdentity, pool volumeapi.Pool) bool {
	return copy.PoolName == pool.Name && copy.PoolUID == pool.UID && copy.NodeName == pool.NodeName
}

// PoolIdentityUnavailable reports whether the Pool read failed in a way that
// leaves the node without a verifiable Pool identity.
func PoolIdentityUnavailable(err error) bool {
	return apierrors.IsNotFound(err) || errors.Is(err, volumeapi.ErrPoolNotFound) || errors.Is(err, volumeapi.ErrPoolConfiguration)
}

func poolIdentity(copy volume.CopyIdentity) ownership.PoolIdentity {
	return ownership.PoolIdentity{InstallationID: copy.InstallationID, PoolUID: copy.PoolUID}
}
