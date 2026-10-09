// Volume authority, Pool inventory, and source/destination node observations.
package controller

import (
	"context"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// poolIndex is the Pool half of one observation snapshot, indexed by Pool UID.
// registered retains every Pool identity, including rejected registrations;
// ready holds only usable candidates. A configuration error must not erase a
// referenced Pool or prevent unrelated Pools from being observed.
type poolIndex struct {
	registered map[string]volumeapi.Pool
	ready      map[string]volumeapi.Pool
}

func moveSourceCopy(move volumeapi.Move, state volumeapi.State) *volume.CopyIdentity {
	if move.Status.SourceCopy != nil {
		return move.Status.SourceCopy
	}
	if state.CurrentCopy != nil && state.CurrentCopy.NodeName == move.Spec.SourceNode {
		return state.CurrentCopy
	}
	return nil
}

func indexedCopyPool(pools map[string]volumeapi.Pool, copy *volume.CopyIdentity) (volumeapi.Pool, bool) {
	if copy == nil || copy.Validate() != nil {
		return volumeapi.Pool{}, false
	}
	pool, exists := pools[copy.PoolUID]
	return pool, exists && pool.Name == copy.PoolName && pool.NodeName == copy.NodeName
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
		if pool.UID == "" {
			return index, fmt.Errorf("ShiftPVPool %q has no UID", pool.Name)
		}
		if _, duplicate := index.registered[pool.UID]; duplicate {
			return index, fmt.Errorf("multiple ShiftPVPools share UID %q", pool.UID)
		}
		index.registered[pool.UID] = pool
	}
	readyPools := snapshot.Ready
	index.ready = make(map[string]volumeapi.Pool, len(readyPools))
	for _, pool := range readyPools {
		registered, exists := index.registered[pool.UID]
		if !exists || registered.Name != pool.Name || registered.NodeName != pool.NodeName {
			continue
		}
		if !pool.BackingConfigurationCheck().OK {
			continue
		}
		index.ready[pool.UID] = pool
	}
	return index, nil
}

func (r *Reconciler) observeSource(ctx context.Context, move volumeapi.Move, pools poolIndex, result *observation) (bool, error) {
	state := result.Volume
	sourceNode, err := r.Client.CoreV1().Nodes().Get(ctx, move.Spec.SourceNode, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("read source Node: %w", err)
	}
	sourcePool, sourceReady := indexedCopyPool(pools.ready, moveSourceCopy(move, state))
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
	sourcePool, sourceRegistered := indexedCopyPool(pools.registered, moveSourceCopy(move, result.Volume))
	nodes := make(map[string]bool)
	for poolUID, registeredPool := range pools.registered {
		nodeName := registeredPool.NodeName
		if nodeName == move.Spec.SourceNode {
			continue
		}
		if !sourceRegistered || registeredPool.PoolGroup != sourcePool.PoolGroup {
			continue
		}
		readyPool, ready := pools.ready[poolUID]
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
		if admission.NodeReady(node) && !node.Spec.Unschedulable && !nodes[nodeName] {
			result.CandidateNodes = append(result.CandidateNodes, nodeName)
			nodes[nodeName] = true
		}
	}
	sort.Strings(result.CandidateNodes)
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
	readyPool, ready := destinationObservationPool(move, pools, result)
	if !ready {
		registeredPool, exists := pools.registered[move.Status.DestinationPoolUID]
		staleAfter := r.PoolReadinessStaleAfter
		if staleAfter <= 0 {
			staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
		}
		if exists && registeredPool.NodeName == result.DestinationNode && pools.capacityIndependent(registeredPool) && volumeapi.PoolReadyForActiveMoveRepairAt(registeredPool, move, result.Volume, r.now(), staleAfter) {
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
	registered, exists := pools.registered[move.Status.DestinationPoolUID]
	if move.Status.CapacityApproved && (!exists || registered.NodeName != result.DestinationNode) {
		result.FSM.DestinationBlocked = true
		result.FSM.DestinationUnavailable = false
		result.FSM.UnsafeReason = "DestinationPoolIdentityChanged"
	}
	if destinationGroupMismatch(move, pools, result, readyPool, ready) {
		result.FSM.DestinationBlocked = true
		result.FSM.DestinationUnavailable = false
		result.FSM.UnsafeReason = "DestinationPoolGroupMismatch"
	}
	return repaired, nil
}

func destinationGroupMismatch(move volumeapi.Move, pools poolIndex, result *observation, selected volumeapi.Pool, ready bool) bool {
	source, exists := indexedCopyPool(pools.registered, moveSourceCopy(move, result.Volume))
	if !exists {
		return false
	}
	if ready {
		return selected.PoolGroup != source.PoolGroup
	}
	if move.Status.DestinationPoolUID != "" {
		selected, exists = pools.registered[move.Status.DestinationPoolUID]
		return exists && selected.PoolGroup != source.PoolGroup
	}
	found := false
	for _, pool := range pools.registered {
		if pool.NodeName == result.DestinationNode {
			found = true
			if pool.PoolGroup == source.PoolGroup {
				return false
			}
		}
	}
	return found
}

// Before capacity admission the selected node may have several eligible Pools.
// Once approved, observation follows only the pinned destination incarnation.
func destinationObservationPool(move volumeapi.Move, pools poolIndex, result *observation) (volumeapi.Pool, bool) {
	if uid := move.Status.DestinationPoolUID; uid != "" {
		pool, ready := pools.ready[uid]
		return pool, ready && pool.NodeName == result.DestinationNode
	}
	source, exists := indexedCopyPool(pools.registered, moveSourceCopy(move, result.Volume))
	if !exists {
		return volumeapi.Pool{}, false
	}
	for _, pool := range pools.ready {
		if pool.NodeName == result.DestinationNode && pool.PoolGroup == source.PoolGroup &&
			!volumeapi.PoolHasConflictingServingVolume(pool, move.Spec.VolumeID, move.Status.DestinationCopy) {
			return pool, true
		}
	}
	return volumeapi.Pool{}, false
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

// A repair may bypass only its own inventory problem, never missing or
// contradictory peer capacity evidence.
func (p poolIndex) capacityIndependent(pool volumeapi.Pool) bool {
	peers := make([]volumeapi.Pool, 0, len(p.registered))
	for _, registered := range p.registered {
		peers = append(peers, registered)
	}
	independent, _ := volumeapi.PoolCapacityIndependent(pool, peers)
	return independent
}
