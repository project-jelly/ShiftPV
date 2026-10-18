// Package reclaim settles an exact cleanup journal before its owner is retired.
package reclaim

import (
	"context"
	"fmt"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
)

type Store interface {
	cleanupapi.Journal
	Ensure(context.Context, cleanupapi.Spec) (cleanupapi.Cleanup, error)
	ReconcileAbsence(context.Context, cleanupapi.Cleanup) (cleanupapi.Cleanup, bool, error)
	WaitForAbsence(context.Context, cleanupapi.Cleanup, time.Duration) (cleanupapi.Cleanup, bool, error)
}

// EffectError lets a transport adapter distinguish execution failures from API
// observation failures without importing its response policy into this package.
type EffectError struct{ Err error }

func (e *EffectError) Error() string { return "reclaim identified volume copy: " + e.Err.Error() }
func (e *EffectError) Unwrap() error { return e.Err }

type Settler struct {
	Store       Store
	Reclaim     func(context.Context, cleanupapi.Cleanup, cleanupapi.Journal) (cleanupapi.Cleanup, error)
	AbsenceWait time.Duration
}

// Advance performs one settlement observation. A background caller injects a
// non-waiting executor step and zero AbsenceWait; CSI may use bounded waiting.
func (s *Settler) Advance(ctx context.Context, spec cleanupapi.Spec) (bool, error) {
	if s == nil || s.Store == nil || s.Reclaim == nil {
		return false, fmt.Errorf("cleanup settlement is incompletely configured")
	}
	current, err := s.Store.Ensure(ctx, spec)
	if err != nil {
		return false, fmt.Errorf("record volume cleanup intent: %w", err)
	}
	if requiresExecutorObservation(current) {
		current, err = s.Reclaim(ctx, current, s.Store)
		if err != nil {
			return false, &EffectError{Err: err}
		}
	}
	if current.Spec != spec || current.UID != spec.Authority.UID {
		return false, fmt.Errorf("%w: cleanup observation changed authority or target", cleanupapi.ErrConflict)
	}
	switch current.Status.Phase {
	case cleanupapi.PhasePending, cleanupapi.PhaseRunning:
		return false, nil
	case cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence, cleanupapi.PhaseCompleted:
	case cleanupapi.PhaseNeedsReview:
		return false, fmt.Errorf("%w: cleanup %q requires review: %s", cleanupapi.ErrConflict, current.Name, current.Status.Message)
	default:
		return false, fmt.Errorf("%w: cleanup %q is phase=%q", cleanupapi.ErrConflict, current.Name, current.Status.Phase)
	}
	current, complete, err := s.Store.ReconcileAbsence(ctx, current)
	if err == nil && !complete && s.AbsenceWait > 0 {
		current, complete, err = s.Store.WaitForAbsence(ctx, current, s.AbsenceWait)
	}
	if err != nil {
		return false, fmt.Errorf("verify exact cleanup absence: %w", err)
	}
	return complete && current.Status.Phase == cleanupapi.PhaseCompleted, nil
}

func requiresExecutorObservation(current cleanupapi.Cleanup) bool {
	jobReceipt := current.Status.Executor != nil && current.Status.Executor.Kind == "" &&
		(current.Status.Phase == cleanupapi.PhaseVerifying || current.Status.Phase == cleanupapi.PhaseConfirmingAbsence || current.Status.Phase == cleanupapi.PhaseCompleted)
	return jobReceipt || current.Status.Phase == cleanupapi.PhasePending || current.Status.Phase == cleanupapi.PhaseRunning
}
