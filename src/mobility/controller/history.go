package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/history"
)

func movesForVolume(moves []volumeapi.Move, volumeID string) []volumeapi.Move {
	var relevant []volumeapi.Move
	for _, move := range moves {
		if move.Spec.VolumeID == volumeID {
			relevant = append(relevant, move)
		}
	}
	return relevant
}

// Coarse scan predicates only route work. Before admitting a new transaction,
// releasing a held consumer or retiring legacy history, verify all remaining
// responsibilities using fresh, complete observations in the Volume lane.
func (r *Reconciler) validateSettledMoves(ctx context.Context, moves []volumeapi.Move) error {
	if len(moves) == 0 {
		return nil
	}
	if r.Cleanups == nil || r.HistoryInspector == nil {
		return fmt.Errorf("Move history observation is not configured")
	}
	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return fmt.Errorf("observe Volume references before settling history: %w", err)
	}
	references := make(map[string]string)
	for id, state := range volumes {
		if state.ActiveMove != "" {
			references[state.ActiveMove] = id
		}
	}
	journals := make(map[string]cleanupapi.Cleanup)
	for _, move := range moves {
		if id, referenced := references[move.Name]; referenced {
			return fmt.Errorf("retain Move %s still referenced by Volume %s", move.Name, id)
		}
		var current *volumeapi.State
		if state, found := volumes[move.Spec.VolumeID]; found {
			current = &state
		}
		journal, err := r.settledMoveJournal(ctx, move, current)
		if err != nil {
			return err
		}
		if journal != nil {
			journals[move.UID] = *journal
		}
	}
	return r.HistoryInspector.MovesSettled(ctx, moves, journals)
}

func (r *Reconciler) settledMoveJournal(ctx context.Context, move volumeapi.Move, current *volumeapi.State) (*cleanupapi.Cleanup, error) {
	stored, err := r.Cleanups.GetHistory(ctx, cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID})
	var journal *cleanupapi.Cleanup
	if err == nil {
		journal = &stored
	} else if !errors.Is(err, cleanupapi.ErrNoJournal) {
		return nil, fmt.Errorf("observe Move %s cleanup history: %w", move.Name, err)
	}
	if err := history.ValidateSettled(move, current, journal); err != nil {
		return nil, fmt.Errorf("retain unsettled Move %s: %w", move.Name, err)
	}
	return journal, nil
}
