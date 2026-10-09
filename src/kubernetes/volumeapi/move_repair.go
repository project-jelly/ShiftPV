package volumeapi

import (
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// PoolReadyForActiveMoveRepairAt admits only the two filesystem states that a
// bound Move can repair after a helper stops between a directory rename/create
// and its placement marker. It is intentionally narrower than ordinary Pool
// readiness and must not be used for provisioning or destination discovery.
func PoolReadyForActiveMoveRepairAt(pool Pool, move Move, state State, now time.Time, staleAfter time.Duration) bool {
	inventory := repairableInventoryAt(pool, now, staleAfter)
	if inventory == nil {
		return false
	}
	if !moveRepairBindsDestination(pool, move) || !moveRepairBindsSourceCopy(move, state) {
		return false
	}
	incoming, destination := move.Status.IncomingCopy, move.Status.DestinationCopy
	if incoming == nil || destination == nil {
		return false
	}
	expectedMarker, admitted := moveRepairMarker(pool, move, state, *incoming, *destination)
	if !admitted {
		return false
	}
	return inventoryHoldsOnlyRepairableProblem(inventory, expectedMarker)
}

// repairableInventoryAt returns the observation a repair may read, or nil unless
// the Pool is Ready and reports a fresh, complete copy-observation problem.
func repairableInventoryAt(pool Pool, now time.Time, staleAfter time.Duration) *PoolInventory {
	if !pool.BackingConfigurationCheck().OK {
		return nil
	}
	if ready, _ := pool.ReadyAt(now, staleAfter); !ready {
		return nil
	}
	inventory := pool.Status.Inventory
	if inventory == nil || inventory.Valid || inventory.Truncated || inventory.Message != "CopyObservationProblem" ||
		inventory.ObservedAt.IsZero() || now.Before(inventory.ObservedAt.Time) || now.Sub(inventory.ObservedAt.Time) > staleAfter {
		return nil
	}
	return inventory
}

// moveRepairBindsDestination reports whether the Move is a named, identified
// transaction whose destination is the Pool being read.
func moveRepairBindsDestination(pool Pool, move Move) bool {
	return move.Name != "" && volume.ValidIdentityToken(move.UID) &&
		move.Status.DestinationNode == pool.NodeName && move.Status.DestinationPoolUID == pool.UID
}

// moveRepairBindsSourceCopy reports whether the volume is still moving under
// this exact Move and still carries the exact serving copy the Move recorded.
func moveRepairBindsSourceCopy(move Move, state State) bool {
	if state.UID == "" || state.Phase != PhaseMoving || state.ActiveMove != move.Name ||
		state.OwnerNode != move.Spec.SourceNode || state.CurrentCopy == nil || move.Status.SourceCopy == nil {
		return false
	}
	return state.CurrentCopy.Validate() == nil && state.CurrentCopy.Role == volume.RoleServing &&
		state.CurrentCopy.VolumeID == move.Spec.VolumeID && state.CurrentCopy.VolumeUID == state.UID &&
		state.CurrentCopy.NodeName == move.Spec.SourceNode && *state.CurrentCopy == *move.Status.SourceCopy
}

// moveRepairMarker returns the single filesystem marker the Move's current phase
// may leave unrecorded, and whether that phase is repairable at all.
func moveRepairMarker(pool Pool, move Move, state State, incoming, destination volume.CopyIdentity) (string, bool) {
	// Repair reads the live destination Pool, so every destination term is
	// pinned to it. The installation is not observable here; anchoring it to the
	// incoming copy still rejects a transaction split across two installations.
	if ValidateMoveTransactionIdentities(move, MoveDestinationAnchor{
		InstallationID: incoming.InstallationID, PoolName: pool.Name, PoolUID: pool.UID,
		NodeName: pool.NodeName, VolumeUID: state.UID,
	}) != nil {
		return "", false
	}
	switch move.Status.Phase {
	case "Copying":
		return "path:.shiftpv/incoming/" + incoming.CopyID, true
	case "Promoting":
		return "path:volumes/" + destination.VolumeID, true
	default:
		return "", false
	}
}

// inventoryHoldsOnlyRepairableProblem admits an observation whose every reported
// problem is the one unrecorded path expectedMarker names, and exactly one of it.
func inventoryHoldsOnlyRepairableProblem(inventory *PoolInventory, expectedMarker string) bool {
	matched := 0
	for _, observed := range inventory.Copies {
		if observed.Problem == "" {
			continue
		}
		if observed.Marker != expectedMarker || observed.Identity != nil || !observed.Present || observed.Published || observed.Problem != "UnrecordedPath" {
			return false
		}
		matched++
	}
	return matched == 1
}
