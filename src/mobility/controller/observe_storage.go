// Volume authority, Pool inventory, and source/destination node observations.
package controller

import (
	"context"
	"fmt"
	"path/filepath"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// poolIndex is the Pool half of one observation snapshot, indexed by node name.
// registered holds every validated ShiftPVPool; ready holds only those whose
// inventory is currently publishable.
type poolIndex struct {
	registered map[string]volumeapi.Pool
	ready      map[string]volumeapi.Pool
}

func (r *Reconciler) observeVolume(ctx context.Context, move volumeapi.Move, result *observation) (bool, error) {
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		if apierrors.IsNotFound(err) && completionAllowed(move, state, true) {
			result.VolumeMissing = true
			result.FSM.CompletionReady = true
			return true, nil
		}
		return false, err
	}
	result.Volume = state
	result.DestinationNode = move.Status.DestinationNode
	// Completing is persisted cleanup evidence. Finalization reads authority only;
	// expired Jobs or a deleted PVC must not restart disk work after unlock.
	if move.Status.Phase == string(fsm.PhaseCompleting) {
		result.FSM.CompletionReady = completionAllowed(move, state, false)
		return true, nil
	}
	result.FSM.OwnerCommitted = hasCommittedDestinationAuthority(move, state, false)
	return false, nil
}

func (r *Reconciler) observePools(ctx context.Context) (poolIndex, error) {
	var index poolIndex
	snapshot, err := r.Repository.ObservePools(ctx)
	if err != nil {
		return index, err
	}
	pools := snapshot.Registered
	index.registered = make(map[string]volumeapi.Pool, len(pools))
	for _, pool := range pools {
		if pool.NodeName == "" || !filepath.IsAbs(pool.MountPath) || filepath.Clean(pool.MountPath) == "/" {
			return index, fmt.Errorf("ShiftPVPool %q has invalid nodeName or mountPath", pool.Name)
		}
		if _, duplicate := index.registered[pool.NodeName]; duplicate {
			return index, fmt.Errorf("multiple ShiftPVPools are registered for node %q", pool.NodeName)
		}
		index.registered[pool.NodeName] = pool
	}
	readyPools := snapshot.Ready
	index.ready = make(map[string]volumeapi.Pool, len(readyPools))
	for _, pool := range readyPools {
		index.ready[pool.NodeName] = pool
	}
	return index, nil
}

func (r *Reconciler) observeSource(ctx context.Context, move volumeapi.Move, pools poolIndex, result *observation) (bool, error) {
	state := result.Volume
	sourceNode, err := r.Client.CoreV1().Nodes().Get(ctx, move.Spec.SourceNode, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("read source Node: %w", err)
	}
	sourcePool, sourceReady := pools.ready[move.Spec.SourceNode]
	sourceHealthy := err == nil && admission.NodeReady(sourceNode) && sourceReady && sourceCopyPresent(sourcePool, state.CurrentCopy)
	result.FSM.SourceHealthy = sourceHealthy
	result.SourceCordoned = sourceNode != nil && sourceNode.Spec.Unschedulable
	if !sourceHealthy && !result.FSM.OwnerCommitted {
		result.FSM.UnsafeReason = "SourceUnavailable"
	}
	// Discovery and Node updates are not atomic. A Move may be created from a
	// cordoned snapshot just after the source was uncordoned. Before any volume
	// lock or helper action, prefer the current Node observation and let the
	// reconciler remove that obsolete transaction even if its PVC is disappearing.
	result.PendingObsolete = move.Status.Phase == string(fsm.PhasePending) && sourceHealthy && !result.SourceCordoned &&
		state.Phase == volumeapi.PhaseReady && state.ActiveMove == "" && state.OwnerNode == move.Spec.SourceNode
	if result.PendingObsolete {
		result.FSM.PreflightDeferred = true
		result.FSM.UnsafeReason = "SourceNotCordoned"
		return true, nil
	}
	return false, nil
}

func (r *Reconciler) observeCandidates(ctx context.Context, move volumeapi.Move, pools poolIndex, result *observation) error {
	for nodeName := range pools.registered {
		if nodeName == move.Spec.SourceNode {
			continue
		}
		readyPool, ready := pools.ready[nodeName]
		if !ready || volumeapi.PoolHasConflictingServingVolume(readyPool, move.Spec.VolumeID, move.Status.DestinationCopy) {
			continue
		}
		node, nodeErr := r.Client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if nodeErr != nil {
			if apierrors.IsNotFound(nodeErr) {
				continue
			}
			return fmt.Errorf("read destination Node %q: %w", nodeName, nodeErr)
		}
		if admission.NodeReady(node) && !node.Spec.Unschedulable {
			result.CandidateNodes = append(result.CandidateNodes, nodeName)
		}
	}
	if len(move.Status.CandidateNodes) != 0 {
		// CandidateNodes is the immutable eligibility snapshot taken before
		// eviction. Keep it stable so a transient Pool outage after placement
		// pauses the transaction instead of being misclassified as a changed
		// scheduling constraint. The selected destination is checked against
		// current readiness later before any disk or authority action proceeds.
		result.CandidateNodes = append([]string(nil), move.Status.CandidateNodes...)
	}
	return nil
}

// observeDestination re-checks the selected destination against current Node and
// Pool readiness. repaired reports that readiness was accepted from a registered
// but not yet ready Pool inside this Move's own crash window.
func (r *Reconciler) observeDestination(ctx context.Context, move volumeapi.Move, pools poolIndex, result *observation) (bool, error) {
	if result.DestinationNode == "" {
		return false, nil
	}
	repaired := false
	readyPool, ready := pools.ready[result.DestinationNode]
	if !ready {
		registeredPool, exists := pools.registered[result.DestinationNode]
		staleAfter := r.PoolReadinessStaleAfter
		if staleAfter <= 0 {
			staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
		}
		if exists && volumeapi.PoolReadyForActiveMoveRepairAt(registeredPool, move, result.Volume, r.now(), staleAfter) {
			readyPool, ready, repaired = registeredPool, true, true
		}
	}
	copyConflict := ready && volumeapi.PoolHasConflictingServingVolume(readyPool, move.Spec.VolumeID, move.Status.DestinationCopy)
	destinationNode, destinationErr := r.Client.CoreV1().Nodes().Get(ctx, result.DestinationNode, metav1.GetOptions{})
	if destinationErr != nil && !apierrors.IsNotFound(destinationErr) {
		return repaired, fmt.Errorf("read selected destination Node %q: %w", result.DestinationNode, destinationErr)
	}
	result.FSM.DestinationUnavailable = destinationErr != nil || !ready || !admission.NodeReady(destinationNode) || copyConflict
	if copyConflict {
		result.FSM.UnsafeReason = "DestinationServingCopyPresent"
	}
	if move.Status.CapacityApproved && ready &&
		(move.Status.DestinationPoolUID == "" || readyPool.UID != move.Status.DestinationPoolUID) {
		result.FSM.DestinationBlocked = true
		result.FSM.UnsafeReason = "DestinationPoolIdentityChanged"
	}
	return repaired, nil
}

func sourceCopyPresent(pool volumeapi.Pool, copy *volume.CopyIdentity) bool {
	if copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing ||
		copy.PoolName != pool.Name || copy.PoolUID != pool.UID || copy.NodeName != pool.NodeName || pool.Status.Inventory == nil {
		return false
	}
	for _, observed := range pool.Status.Inventory.Copies {
		if observed.Identity != nil && *observed.Identity == *copy {
			return observed.Present && observed.Problem == ""
		}
	}
	return false
}
