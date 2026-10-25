package reclaim

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type journal struct {
	current                 cleanupapi.Cleanup
	complete                bool
	absenceCalls, waitCalls int
	err                     error
}

func (s *journal) Get(context.Context, cleanupapi.Authority) (cleanupapi.Cleanup, error) {
	return s.current, nil
}
func (s *journal) UpdateStatus(context.Context, cleanupapi.Cleanup, cleanupapi.Status) error {
	return nil
}
func (s *journal) Ensure(context.Context, cleanupapi.Spec) (cleanupapi.Cleanup, error) {
	return s.current, nil
}
func (s *journal) ReconcileAbsence(context.Context, cleanupapi.Cleanup) (cleanupapi.Cleanup, bool, error) {
	s.absenceCalls++
	return s.current, s.complete, s.err
}
func (s *journal) WaitForAbsence(context.Context, cleanupapi.Cleanup, time.Duration) (cleanupapi.Cleanup, bool, error) {
	s.waitCalls++
	return s.current, s.complete, s.err
}

func TestAdvanceRequiresCompletedExactJournal(t *testing.T) {
	spec := cleanupapi.Spec{OperationID: "operation", Target: volume.CopyIdentity{VolumeUID: "volume-uid"}, Authority: cleanupapi.Authority{UID: "volume-uid"}}
	for _, phase := range []string{cleanupapi.PhasePending, cleanupapi.PhaseRunning, cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence, cleanupapi.PhaseCompleted, cleanupapi.PhaseNeedsReview, "unknown"} {
		t.Run(phase, func(t *testing.T) {
			store := &journal{current: cleanupapi.Cleanup{UID: "volume-uid", Spec: spec, Status: cleanupapi.Status{Phase: phase}}, complete: true}
			calls := 0
			s := Settler{Store: store, Reclaim: func(_ context.Context, current cleanupapi.Cleanup, _ cleanupapi.Journal) (cleanupapi.Cleanup, error) {
				calls++
				return current, nil
			}}
			complete, err := s.Advance(context.Background(), spec)
			if complete != (phase == cleanupapi.PhaseCompleted) {
				t.Fatalf("early completion in phase %q", phase)
			}
			if (err != nil) != (phase == cleanupapi.PhaseNeedsReview || phase == "unknown") {
				t.Fatal(err)
			}
			if (calls == 1) != (phase == cleanupapi.PhasePending || phase == cleanupapi.PhaseRunning) {
				t.Fatalf("effect calls=%d", calls)
			}
			if (store.absenceCalls == 1) != (phase == cleanupapi.PhaseVerifying || phase == cleanupapi.PhaseConfirmingAbsence || phase == cleanupapi.PhaseCompleted) {
				t.Fatalf("absence calls=%d", store.absenceCalls)
			}
			if store.waitCalls != 0 {
				t.Fatal("background path waited")
			}
		})
	}
}

func TestAdvancePreservesJobSettlementAndRejectsChangedTarget(t *testing.T) {
	spec := cleanupapi.Spec{Authority: cleanupapi.Authority{UID: "volume-uid"}}
	store := &journal{current: cleanupapi.Cleanup{UID: "volume-uid", Spec: spec, Status: cleanupapi.Status{Phase: cleanupapi.PhaseCompleted, Executor: &cleanupapi.Executor{JobUID: "legacy-job"}}}, complete: true}
	calls := 0
	s := Settler{Store: store, Reclaim: func(_ context.Context, current cleanupapi.Cleanup, _ cleanupapi.Journal) (cleanupapi.Cleanup, error) {
		calls++
		current.Spec.Target.CopyID = "foreign"
		return current, nil
	}}
	if done, err := s.Advance(context.Background(), spec); done || !errors.Is(err, cleanupapi.ErrConflict) || calls != 1 || store.absenceCalls != 0 {
		t.Fatalf("done=%v err=%v calls=%d", done, err, calls)
	}
}

func TestAdvancePreservesObservationAndEffectFailures(t *testing.T) {
	sentinel := errors.New("retryable observation failure")
	spec := cleanupapi.Spec{Authority: cleanupapi.Authority{UID: "volume-uid"}}
	store := &journal{current: cleanupapi.Cleanup{UID: "volume-uid", Spec: spec, Status: cleanupapi.Status{Phase: cleanupapi.PhaseVerifying}}, err: sentinel}
	s := Settler{Store: store, Reclaim: func(context.Context, cleanupapi.Cleanup, cleanupapi.Journal) (cleanupapi.Cleanup, error) {
		return cleanupapi.Cleanup{}, sentinel
	}}
	if done, err := s.Advance(context.Background(), spec); done || !errors.Is(err, sentinel) {
		t.Fatal(done, err)
	}
	store.current.Status.Phase = cleanupapi.PhasePending
	var effect *EffectError
	if done, err := s.Advance(context.Background(), spec); done || !errors.As(err, &effect) || !errors.Is(err, sentinel) {
		t.Fatal(done, err)
	}
}
