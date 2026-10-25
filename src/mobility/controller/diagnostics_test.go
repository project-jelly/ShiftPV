package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func TestMoveDiagnosticsTrackTransitionsWithoutPollingDrift(t *testing.T) {
	ctx := context.Background()
	first := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	now := first
	recorder := record.NewFakeRecorder(10)
	move := preparedFixtureMove(volumeapi.Move{Name: "move", UID: "uid", Spec: volumeapi.MoveSpec{VolumeID: preflightVolume, SourceNode: "source"}})
	repo := &memoryRepository{moves: []volumeapi.Move{move}}
	r := &Reconciler{Repository: repo, Now: func() time.Time { return now }, Recorder: recorder}

	previous := move.Status
	move.Status.Phase = string(fsm.PhasePending)
	move.Status.Reason = "DisruptionBudgetDenied"
	move.Status.Message = mobilityMessage(fsm.PhasePending, move.Status.Reason)
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		t.Fatal(err)
	}
	if move.Status.LastTransitionTime != first.Format(time.RFC3339Nano) || move.Status.LastProgressTime != first.Format(time.RFC3339Nano) {
		t.Fatalf("initial diagnostics=%+v", move.Status)
	}
	assertEventContains(t, recorder, "Normal DisruptionBudgetDenied automatic retry")

	now = second
	previous = move.Status
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		t.Fatal(err)
	}
	assertNoEvent(t, recorder)
	if move.Status.LastProgressTime != first.Format(time.RFC3339Nano) {
		t.Fatalf("polling changed progress time: %+v", move.Status)
	}

	move.Status.Reason = "NoCompatibleDestination"
	move.Status.Message = mobilityMessage(fsm.PhasePending, move.Status.Reason)
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		t.Fatal(err)
	}
	assertEventContains(t, recorder, "Normal NoCompatibleDestination automatic retry")
	if move.Status.LastProgressTime != first.Format(time.RFC3339Nano) || move.Status.LastTransitionTime != first.Format(time.RFC3339Nano) {
		t.Fatalf("diagnostic-only change looked like progress: %+v", move.Status)
	}

	previous = move.Status
	move.Status.Phase = string(fsm.PhasePreparing)
	move.Status.Reason = ""
	move.Status.Message = mobilityMessage(fsm.PhasePreparing, "")
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		t.Fatal(err)
	}
	if move.Status.LastTransitionTime != second.Format(time.RFC3339Nano) || move.Status.LastProgressTime != second.Format(time.RFC3339Nano) {
		t.Fatalf("phase transition did not advance timestamps: %+v", move.Status)
	}
	assertEventContains(t, recorder, "Normal MobilityPreparing automatic retry")
}

func TestMoveDiagnosticsRecordAndClearTransientObservationFailure(t *testing.T) {
	f, move := preparationAtCapacity(t)
	next := move.Status
	next.Phase = string(fsm.PhaseWaitingForUnpublish)
	move, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, next)
	if err != nil {
		t.Fatal(err)
	}
	source := f.state(t, 0)
	temporary := errors.New("API timeout")
	unavailable := true
	f.client.PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		if unavailable {
			return true, nil, temporary
		}
		return false, nil, nil
	})
	f.reconciler.Now = func() time.Time { return time.Date(2026, 9, 4, 2, 0, 0, 0, time.UTC) }
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); !errors.Is(err, temporary) {
		t.Fatalf("observation failure cause was hidden: %v", err)
	}
	failed := f.move(t, 0).Status
	if failed.Phase != string(fsm.PhaseWaitingForUnpublish) || failed.Reason != "ObservationFailed" || !strings.Contains(failed.Message, "API timeout") || failed.LastProgressTime == "" {
		t.Fatalf("failure diagnostic=%+v", failed)
	}
	if !reflect.DeepEqual(source, f.state(t, 0)) || !reflect.DeepEqual(move.Status.Preparation, failed.Preparation) {
		t.Fatal("observation error changed source authority or its hold")
	}
	unavailable = false
	f.reconciler.Now = func() time.Time { return time.Date(2026, 9, 4, 2, 1, 0, 0, time.UTC) }
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	recovered := f.move(t, 0).Status
	if recovered.Phase != string(fsm.PhaseWaitingForReplacement) || recovered.Reason != "" || strings.Contains(recovered.Message, "API timeout") {
		t.Fatalf("stale transient diagnostic=%+v", recovered)
	}
}

func TestMoveDiagnosticsRecordAndClearTransientActionFailure(t *testing.T) {
	f, move := preparationAtCapacity(t)
	singleLifecycleReplacement(t, f)
	// Convert the existing logical hold and reobserve its approved plan before
	// reaching the actual Copying effect; do not seed a partial legacy plan.
	for range 2 {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
	}
	move = f.move(t, 0)
	if move.Status.Phase != string(fsm.PhaseCopying) || !move.Status.CapacityApproved {
		t.Fatal("fixture did not reach approved Copying")
	}
	server, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).SourcePod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := f.client.CoreV1().Pods(server.Namespace).UpdateStatus(f.ctx, server, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	temporary := errors.New("API timeout")
	unavailable := true
	f.client.PrependReactor("create", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		if unavailable {
			return true, nil, temporary
		}
		return false, nil, nil
	})
	source := f.state(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); !errors.Is(err, temporary) {
		t.Fatalf("action failure cause was hidden: %v", err)
	}
	failed := f.move(t, 0).Status
	if failed.Phase != string(fsm.PhaseCopying) || failed.Reason != "ActionFailed" || !strings.Contains(failed.Message, "API timeout") || failed.CopyJobName != namesFor(move.Name).CopyJob {
		t.Fatalf("action failure diagnostic=%+v", failed)
	}
	if _, err := f.client.BatchV1().Jobs(f.reconciler.Namespace).Get(f.ctx, failed.CopyJobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed Job acquisition produced an executor: %v", err)
	}
	if !reflect.DeepEqual(source, f.state(t, 0)) || !failed.CapacityApproved || !reflect.DeepEqual(move.Status.IncomingCopy, failed.IncomingCopy) || !reflect.DeepEqual(move.Status.DestinationCopy, failed.DestinationCopy) {
		t.Fatal("failed executor acquisition changed approved authority")
	}
	unavailable = false
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	recovered := f.move(t, 0).Status
	if recovered.Phase != string(fsm.PhaseCopying) || recovered.Reason != "" || recovered.CopyJobName != namesFor(move.Name).CopyJob || strings.Contains(recovered.Message, "API timeout") {
		t.Fatalf("stale action diagnostic=%+v", recovered)
	}
	job, err := f.client.BatchV1().Jobs(f.reconciler.Namespace).Get(f.ctx, recovered.CopyJobName, metav1.GetOptions{})
	if err != nil || (metav1.GetControllerOf(job) == nil || string(metav1.GetControllerOf(job).UID) != move.UID || metav1.GetControllerOf(job).Name != move.Name) {
		t.Fatalf("retry did not acquire the exact copy Job: %v", err)
	}
}

func TestPreparedPreflightAPIFailurePreservesAuthorityBeforeFreshRetry(t *testing.T) {
	for _, phase := range []fsm.Phase{fsm.PhasePending, fsm.PhaseStartingDisruption} {
		t.Run(string(phase), func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			if phase == fsm.PhaseStartingDisruption {
				move := f.untilPrepared(t, 0)
				if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
					t.Fatal(err)
				}
			}
			move, source := f.move(t, 0), f.state(t, 0)
			if move.Status.Phase != string(phase) {
				t.Fatal("fixture phase mismatch")
			}
			temporary := errors.New("preflight API timeout")
			unavailable := true
			f.client.PrependReactor("get", "replicasets", func(ktesting.Action) (bool, runtime.Object, error) {
				if unavailable {
					return true, nil, temporary
				}
				return false, nil, nil
			})
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); !errors.Is(err, temporary) {
				t.Fatalf("original preflight cause was hidden: %v", err)
			}
			if !reflect.DeepEqual(move, f.move(t, 0)) || !reflect.DeepEqual(source, f.state(t, 0)) {
				t.Fatal("preparation observation failure changed plan, hold, source, status or resourceVersion")
			}
			assertNoEviction(t, f.client)
			unavailable = false
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			resumed := f.move(t, 0)
			wantPhase := string(fsm.PhasePreparing)
			if phase == fsm.PhaseStartingDisruption {
				wantPhase = string(fsm.PhaseWaitingForUnpublish)
			}
			if resumed.Status.Phase != wantPhase || resumed.Status.Reason != "" {
				t.Fatalf("fresh retry did not resume: %+v", resumed.Status)
			}
			if phase == fsm.PhasePending {
				assertNoEviction(t, f.client)
			}
			if phase == fsm.PhaseStartingDisruption && (!resumed.Status.EvictionRequested || !reflect.DeepEqual(move.Status.Preparation, resumed.Status.Preparation)) {
				t.Fatal("fresh retry lost its exact prepared eviction authority")
			}
		})
	}
}

// A failed eviction can follow the accepted Ready -> Moving fence. Preserve
// the stored StartingDisruption intent and retry from fresh source evidence;
// recording a diagnostic must not fabricate a phase transition after the error.
func TestPreparedEvictionFailureKeepsDurableDisruptionIntentForRetry(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move, source := f.move(t, 0), f.state(t, 0)
	temporary := errors.New("eviction API timeout")
	unavailable := true
	f.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if unavailable && action.GetSubresource() == "eviction" {
			return true, nil, temporary
		}
		return false, nil, nil
	})
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); !errors.Is(err, temporary) {
		t.Fatalf("eviction failure cause was hidden: %v", err)
	}
	if !reflect.DeepEqual(move, f.move(t, 0)) {
		t.Fatal("failed eviction changed the durable intent, hold, status or resourceVersion")
	}
	fenced := f.state(t, 0)
	if fenced.Phase != volumeapi.PhaseMoving {
		t.Fatal("eviction did not preserve the accepted publication fence")
	}
	fenced.Phase = source.Phase
	if !reflect.DeepEqual(source, fenced) {
		t.Fatal("failed eviction changed source ownership, copy, claim or publication evidence")
	}
	singleLifecycleNoDiskWork(t, f)
	unavailable = false
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	resumed := f.move(t, 0)
	if resumed.Status.Phase != string(fsm.PhaseWaitingForUnpublish) || !resumed.Status.EvictionRequested || !reflect.DeepEqual(move.Status.Preparation, resumed.Status.Preparation) || f.state(t, 0).Phase != volumeapi.PhaseMoving {
		t.Fatalf("fresh retry did not resume the same fenced disruption: %+v", resumed.Status)
	}
}

func assertEventContains(t *testing.T, recorder *record.FakeRecorder, want string) {
	t.Helper()
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, want) {
			t.Fatalf("event=%q want substring=%q", event, want)
		}
	default:
		t.Fatalf("missing event containing %q", want)
	}
}

func assertNoEvent(t *testing.T, recorder *record.FakeRecorder) {
	t.Helper()
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event=%q", event)
	default:
	}
}
