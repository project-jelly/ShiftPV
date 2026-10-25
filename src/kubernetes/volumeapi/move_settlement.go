package volumeapi

import (
	"fmt"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func validateSettlementTransition(current Move, next MoveStatus) error {
	if current.Status.SettlementRequested && !next.SettlementRequested {
		return fmt.Errorf("%w: settlement intent cannot be removed", ErrStateConflict)
	}
	if next.SettlementRequested && next.Phase != "Blocked" {
		return fmt.Errorf("%w: settlement requires a blocked Move", ErrStateConflict)
	}
	if next.SettlementRequested && !current.Status.SettlementRequested &&
		!current.Status.CapacityApproved && !hasMoveDestinationIntent(current.Status) {
		return fmt.Errorf("%w: unapproved preparation must use cancellation", ErrStateConflict)
	}
	return nil
}

// MoveRetiresUnpublishedSource is the non-service alternative to destination
// publication. The controller persisted retirement after quiescing executors
// and verifying the exact committed owner. Fresh owner inventory is still
// required separately before source cleanup may execute.
func MoveRetiresUnpublishedSource(move Move, state State) bool {
	if !move.Status.SettlementRequested || move.Status.Phase != "Blocked" || move.Status.RecoveryPhase != "Retiring" ||
		move.Status.RecoveryOwner != move.Status.DestinationNode || move.Status.RollbackRequiredGeneration <= 0 ||
		ValidatePreparedMovePlan(move) != nil || move.Status.IncomingCopy == nil ||
		move.Status.DestinationNode == move.Spec.SourceNode || move.Status.DestinationPoolUID == move.Status.SourceCopy.PoolUID {
		return false
	}
	anchor := MoveDestinationAnchor{
		InstallationID: move.Status.SourceCopy.InstallationID, VolumeUID: move.Status.SourceCopy.VolumeUID,
		PoolName: move.Status.IncomingCopy.PoolName, PoolUID: move.Status.DestinationPoolUID, NodeName: move.Status.DestinationNode,
	}
	if ValidateMoveTransactionIdentities(move, anchor) != nil {
		return false
	}
	copy := move.Status.DestinationCopy
	return copy != nil && !state.Deleting && !state.HasCleanup && state.Phase == PhaseBlocked && state.ActiveMove == move.Name && state.UID == copy.VolumeUID &&
		state.OwnerNode == copy.NodeName && state.CurrentCopy != nil && *state.CurrentCopy == *copy && len(state.PublishedNodes) == 0
}

// PoolHasUnpublishedCopy requires a current post-retirement inventory of the
// one exact serving copy, with no conflicting present copy of this Volume.
func PoolHasUnpublishedCopy(pool Pool, target *volume.CopyIdentity, requiredGeneration int64) bool {
	if !settlementInventoryCurrent(pool, target, requiredGeneration) {
		return false
	}
	matches := 0
	for _, observed := range pool.Status.Inventory.Copies {
		if observed.Identity == nil || observed.Identity.VolumeID != target.VolumeID {
			continue
		}
		if *observed.Identity != *target {
			if observed.Present || observed.Problem != "" {
				return false
			}
			continue
		}
		if !observed.Present || observed.Published || observed.Problem != "" {
			return false
		}
		matches++
	}
	return matches == 1
}

func settlementInventoryCurrent(pool Pool, target *volume.CopyIdentity, requiredGeneration int64) bool {
	if target == nil || target.Validate() != nil || target.Role != volume.RoleServing || requiredGeneration <= 0 ||
		pool.Generation < requiredGeneration || pool.Status.ObservedGeneration != pool.Generation ||
		target.PoolName != pool.Name || target.PoolUID != pool.UID || target.NodeName != pool.NodeName {
		return false
	}
	inventory := pool.Status.Inventory
	return inventory != nil && inventory.Valid && !inventory.Truncated && inventory.Message == ""
}
