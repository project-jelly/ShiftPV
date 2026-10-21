package capacity

import (
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func preparationReservation(volumes map[string]volumeapi.State, move volumeapi.Move) (string, int64, error) {
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return "", 0, fmt.Errorf("move %q has invalid preparation: %w", move.Name, err)
	}
	state, exists := volumes[move.Spec.VolumeID]
	if !exists {
		return "", 0, fmt.Errorf("move %q preparation has no volume state", move.Name)
	}
	source, preparation := move.Status.SourceCopy, move.Status.Preparation
	if state.UID != source.VolumeUID || state.ActiveMove != move.Name ||
		state.OwnerNode != move.Spec.SourceNode || state.CurrentCopy == nil || *state.CurrentCopy != *source {
		return "", 0, fmt.Errorf("move %q preparation does not match the active volume owner", move.Name)
	}
	if state.Deleting || (state.Phase != volumeapi.PhaseReady && state.Phase != volumeapi.PhaseMoving && state.Phase != volumeapi.PhaseBlocked) ||
		state.CapacityBytes != preparation.ReservedBytes {
		return "", 0, fmt.Errorf("move %q preparation does not match the volume capacity or lifecycle", move.Name)
	}
	return preparation.Pool.UID, preparation.ReservedBytes, nil
}
