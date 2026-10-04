package capacity

import (
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// ReservedBytesForPool counts durable volume and Move holds for one exact Pool
// incarnation. It uses copy Pool UIDs, since nodes may own multiple capacity
// units and a node-wide total cannot be subtracted from one Pool's limit.
func ReservedBytesForPool(volumes map[string]volumeapi.State, moves []volumeapi.Move, poolUID string) (int64, error) {
	if poolUID == "" {
		return 0, fmt.Errorf("Pool UID is required")
	}
	total, err := ownedBytesForPool(volumes, poolUID)
	if err != nil {
		return 0, err
	}
	return moveHeldBytesForPool(total, volumes, moves, poolUID)
}

func ownedBytesForPool(volumes map[string]volumeapi.State, poolUID string) (int64, error) {
	var total int64
	for volumeID, state := range volumes {
		if state.OwnerNode == "" || state.CapacityBytes <= 0 || state.CurrentCopy == nil ||
			state.CurrentCopy.PoolUID == "" || state.CurrentCopy.NodeName != state.OwnerNode {
			return 0, fmt.Errorf("volume %q has incomplete capacity owner identity", volumeID)
		}
		if state.CurrentCopy.PoolUID == poolUID {
			var err error
			total, err = add(total, state.CapacityBytes)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func moveHeldBytesForPool(total int64, volumes map[string]volumeapi.State, moves []volumeapi.Move, poolUID string) (int64, error) {
	for _, move := range moves {
		if volumeapi.MoveCleanupSettled(move) || !move.Status.CapacityApproved {
			continue
		}
		state, exists := volumes[move.Spec.VolumeID]
		if !exists {
			return 0, fmt.Errorf("move %q has no volume state", move.Name)
		}
		if state.ActiveMove != move.Name || state.CurrentCopy == nil ||
			move.Status.SourceCopy == nil || move.Status.SourceCopy.PoolUID == "" ||
			move.Status.DestinationPoolUID == "" {
			return 0, fmt.Errorf("move %q has incomplete capacity hold identity", move.Name)
		}
		heldPoolUID := move.Status.DestinationPoolUID
		if state.CurrentCopy.PoolUID == heldPoolUID {
			// Owner CAS committed: retain the source copy's hold until cleanup.
			heldPoolUID = move.Status.SourceCopy.PoolUID
		}
		if heldPoolUID == poolUID {
			var err error
			total, err = add(total, state.CapacityBytes)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}
