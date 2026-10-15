package capacity

import (
	"fmt"
	"math"

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
		ownerPool, err := ownerPoolUID(volumeID, state)
		if err != nil {
			return 0, err
		}
		if ownerPool == poolUID {
			var err error
			total, err = add(total, state.CapacityBytes)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func ownerPoolUID(volumeID string, state volumeapi.State) (string, error) {
	if state.HasInitialReservation() {
		return state.InitialPool.UID, nil
	}
	if state.Initializing() && state.InitialPool == nil {
		// Legacy spec-only objects never authorized execution. Each must pass
		// admission before acquiring a durable InitialPool reservation.
		return "", nil
	}
	if state.OwnerNode == "" || state.CapacityBytes <= 0 || state.CurrentCopy == nil ||
		state.CurrentCopy.PoolUID == "" || state.CurrentCopy.NodeName != state.OwnerNode {
		return "", fmt.Errorf("volume %q has incomplete capacity owner identity", volumeID)
	}
	return state.CurrentCopy.PoolUID, nil
}

func moveHeldBytesForPool(total int64, volumes map[string]volumeapi.State, moves []volumeapi.Move, poolUID string) (int64, error) {
	for _, move := range moves {
		heldPoolUID, reserved, err := moveReservation(volumes, move)
		if err != nil {
			return 0, err
		}
		if heldPoolUID == poolUID {
			total, err = add(total, reserved)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func moveReservation(volumes map[string]volumeapi.State, move volumeapi.Move) (string, int64, error) {
	// Preparation is a durable logical hold even if a terminal phase was
	// recorded. Only explicitly removing it releases the reservation.
	if move.Status.Preparation != nil {
		return preparationReservation(volumes, move)
	}
	if volumeapi.MoveCleanupSettled(move) || !move.Status.CapacityApproved {
		return "", 0, nil
	}
	state, exists := volumes[move.Spec.VolumeID]
	if !exists {
		return "", 0, fmt.Errorf("move %q has no volume state", move.Name)
	}
	if state.ActiveMove != move.Name || state.CurrentCopy == nil ||
		move.Status.SourceCopy == nil || move.Status.SourceCopy.PoolUID == "" ||
		move.Status.DestinationPoolUID == "" {
		return "", 0, fmt.Errorf("move %q has incomplete capacity hold identity", move.Name)
	}
	heldPoolUID := move.Status.DestinationPoolUID
	if state.CurrentCopy.PoolUID == heldPoolUID {
		// Owner CAS committed: retain the source copy's hold until cleanup.
		heldPoolUID = move.Status.SourceCopy.PoolUID
	}
	return heldPoolUID, state.CapacityBytes, nil
}

func add(left, right int64) (int64, error) {
	if right <= 0 {
		return 0, fmt.Errorf("capacity must be positive")
	}
	if left > math.MaxInt64-right {
		return 0, fmt.Errorf("Pool reservation total overflows int64")
	}
	return left + right, nil
}
