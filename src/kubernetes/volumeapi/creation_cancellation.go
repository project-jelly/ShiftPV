package volumeapi

import (
	"context"
	"slices"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// CancellableCreation is an approved initial copy that has not completed. The
// CSI caller must separately exclude legacy helper responsibility before CAS.
func (s State) CancellableCreation() bool {
	if s.HasCleanup || s.DeletionOperationID != "" || !s.initialCreationIdentity() {
		return false
	}
	return s.Phase == PhasePending && s.CreationExecutor == nil && s.CreationReceipt == nil ||
		s.Phase == PhaseNodeCreating && s.CreationExecutor != nil
}

// CancellingCreation is the durable fence shared by cleanup admission and the
// filesystem callback. Creation history is retained; it is never made Ready.
func (s State) CancellingCreation() bool {
	return s.Phase == PhaseDeleting && s.initialCreationIdentity() &&
		volume.IsCreationCancellation(*s.CurrentCopy, s.DeletionOperationID)
}

func (s State) initialCreationIdentity() bool {
	if s.UID == "" || s.CapacityBytes <= 0 || s.RequestName == "" || s.InitialNode == "" || s.OwnerNode != s.InitialNode ||
		s.CurrentCopy == nil || s.CurrentCopy.VolumeUID != s.UID || s.CurrentCopy.NodeName != s.OwnerNode ||
		!volume.IsCreationCancellation(*s.CurrentCopy, volume.CreationCancellationPrefix+s.UID) ||
		s.CreationOperationID != "create-"+s.UID || s.ActiveMove != "" || len(s.PublishedNodes) != 0 ||
		!slices.Contains(s.Finalizers, VolumeProtectionFinalizer) {
		return false
	}
	return s.initialCreationHistory()
}

func (s State) initialCreationHistory() bool {
	if s.InitialPool != nil && (s.InitialPool.Name != s.CurrentCopy.PoolName || s.InitialPool.UID != s.CurrentCopy.PoolUID) {
		return false
	}
	if s.CreationExecutor == nil {
		return s.CreationReceipt == nil
	}
	return s.CreationExecutor.Valid() && s.CreationExecutor.NodeName == s.OwnerNode && (s.CreationReceipt == nil || ValidCreationReceipt(s))
}

func (r *Registry) BeginCreationCancellation(ctx context.Context, id, uid string, copy volume.CopyIdentity) (State, error) {
	if copy.VolumeID != id || copy.VolumeUID != uid || !volume.IsCreationCancellation(copy, volume.CreationCancellationPrefix+uid) {
		return State{}, ErrStateConflict
	}
	err := r.mutateState(ctx, id, func(current State) (State, error) {
		if current.UID != uid || current.CurrentCopy == nil || *current.CurrentCopy != copy {
			return State{}, ErrStateConflict
		}
		if current.CancellingCreation() {
			return current, nil
		}
		if !current.CancellableCreation() {
			return State{}, ErrStateConflict
		}
		current.Phase, current.DeletionOperationID = PhaseDeleting, volume.CreationCancellationPrefix+uid
		return current, nil
	})
	if err != nil {
		return State{}, err
	}
	current, err := r.Get(ctx, id)
	if err != nil {
		return State{}, err
	}
	if current.UID != uid || current.CurrentCopy == nil || *current.CurrentCopy != copy || !current.CancellingCreation() {
		return State{}, ErrStateConflict
	}
	return current, nil
}
