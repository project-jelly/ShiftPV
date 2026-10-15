package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type unavailableVolumeList struct {
	Repository
	err error
}

func (r unavailableVolumeList) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return nil, r.err
}

func TestMetadataScanDoesNotExecuteOrReadIndividualVolumeObjects(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", f.moveNames[0]); err != nil {
		t.Fatal(err)
	}
	pod := f.workloads[0].Consumer.DeepCopy()
	pod.Labels = map[string]string{"shiftpv.io/managed": "true"}
	pod.Spec.NodeName = ""
	pod.Status.Phase = corev1.PodPending
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	if _, err := f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.dynamic.ClearActions()
	f.client.ClearActions()
	hints, err := f.reconciler.scanVolumeWork(f.ctx, nil, newDiscoveryObservations(nil))
	if err != nil {
		t.Fatal(err)
	}
	if work := hints[f.volumeIDs[0]]; !work.discover || len(work.ownerPods) != 1 || len(work.moves) != 0 {
		t.Fatalf("discovery and held Pod not routed without creating a Move: %+v", hints)
	}
	for _, action := range append(f.dynamic.Actions(), f.client.Actions()...) {
		if action.GetVerb() != "list" {
			t.Fatalf("metadata scan performed individual observation/effect: %s %s", action.GetVerb(), action.GetResource())
		}
	}
}

func TestVolumeListFailureDoesNotSuppressExistingJournal(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	sentinel := errors.New("discovery Volume LIST unavailable")
	f.reconciler.Repository = unavailableVolumeList{Repository: f.registry, err: sentinel}
	hints, err := f.reconciler.scanVolumeWork(f.ctx, nil, newDiscoveryObservations(nil))
	if !errors.Is(err, sentinel) || len(hints[f.volumeIDs[0]].moves) != 1 {
		t.Fatalf("independent Move work lost: hints=%+v err=%v", hints, err)
	}
	if _, err := f.reconciler.advanceVolume(f.ctx, hints[f.volumeIDs[0]]); err != nil {
		t.Fatal(err)
	}
	if f.move(t, 0).Status.SourceCopy == nil {
		t.Fatal("existing journal could not save its plan while discovery failed")
	}
}

func TestQueuedMoveReferenceCannotExecuteReplacementOrForeignVolume(t *testing.T) {
	for _, scenario := range []string{"replacement UID", "different Volume lane", "deleted"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			before := f.move(t, 0)
			id := f.volumeIDs[0]
			switch scenario {
			case "replacement UID":
				object, err := f.dynamic.Tracker().Get(volumeapi.MoveResource, "", before.Name)
				if err != nil {
					t.Fatal(err)
				}
				changed := object.(*unstructured.Unstructured).DeepCopy()
				changed.SetUID(types.UID("replacement-move-uid"))
				if err := f.dynamic.Tracker().Update(volumeapi.MoveResource, changed, ""); err != nil {
					t.Fatal(err)
				}
			case "different Volume lane":
				id = "shiftpv-ffffffffffffffffffffffffffffffff"
			case "deleted":
				if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", before.Name); err != nil {
					t.Fatal(err)
				}
			}
			f.dynamic.ClearActions()
			f.client.ClearActions()
			if err := f.reconciler.advanceMoveReference(f.ctx, id, moveReference{before.Name, before.UID}); err != nil {
				t.Fatal(err)
			}
			for _, action := range append(f.dynamic.Actions(), f.client.Actions()...) {
				if action.GetVerb() != "get" {
					t.Fatalf("stale queued identity caused work: %s %s", action.GetVerb(), action.GetResource())
				}
			}
		})
	}
}

func TestOwnerPlacementRoutingHintCannotMutateAnotherVolumeLane(t *testing.T) {
	r, client, _, original := ownerPlacementFixture()
	pvs, err := client.CoreV1().PersistentVolumes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The routing snapshot predates a binding change. It can enqueue a hint,
	// but the current exact binding must still belong to that lane before Update.
	staleID := "shiftpv-ffffffffffffffffffffffffffffffff"
	for i := range pvs.Items {
		pvs.Items[i].Spec.CSI.VolumeHandle = staleID
	}
	hints := ownerPlacementHints([]*corev1.Pod{original}, pvs.Items)
	if len(hints) != 1 || hints[0].volumeID != staleID {
		t.Fatalf("missing stale routing fixture: %+v", hints)
	}
	client.ClearActions()
	if _, err := r.advanceVolume(context.Background(), hints[0]); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Pods(original.Namespace).Get(context.Background(), original.Name, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("wrong lane changed the held Pod: pod=%+v err=%v", got, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			t.Fatal("wrong Volume lane released a gate")
		}
	}
	if err := r.reconcileOwnerPlacementInVolume(context.Background(), ownerPlacementVolumeID, original); err != nil {
		t.Fatal(err)
	}
	got, err = client.CoreV1().Pods(original.Namespace).Get(context.Background(), original.Name, metav1.GetOptions{})
	if err != nil || hasPlacementHold(got) {
		t.Fatalf("correct Volume lane failed to recover Pod: pod=%+v err=%v", got, err)
	}
}

func TestHistoryFilterRetainsCancellationRepairAndDueGC(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	r := &Reconciler{Now: func() time.Time { return now }}
	settled := volumeapi.Move{Name: "move", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: "volume", ProtocolVersion: volumeapi.MoveProtocolPreparedV1}, Status: volumeapi.MoveStatus{Phase: "Cancelled", LastTransitionTime: now.Format(time.RFC3339Nano)}}
	for _, test := range []struct {
		name   string
		mutate func(*volumeapi.Move, map[string]volumeapi.State)
		known  bool
		want   bool
	}{
		{name: "settled future history", known: true},
		{name: "Volume gone but future history", known: true},
		{name: "active cancellation unlock", known: true, want: true, mutate: func(_ *volumeapi.Move, states map[string]volumeapi.State) {
			states["volume"] = volumeapi.State{ActiveMove: "move"}
		}},
		{name: "finalizer", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) {
			move.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
		}},
		{name: "working cleanup", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) { move.Status.CleanupPhase = "Verifying" }},
		{name: "logical hold", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) {
			move.Status.Preparation = &volumeapi.MovePreparation{}
		}},
		{name: "unknown protocol", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) { move.Spec.ProtocolVersion = "future" }},
		{name: "due GC without Volume", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) {
			move.Status.LastTransitionTime = now.Add(-DefaultMoveJournalRetention).Format(time.RFC3339Nano)
		}},
		{name: "invalid retention evidence", known: true, want: true, mutate: func(move *volumeapi.Move, _ map[string]volumeapi.State) { move.Status.LastTransitionTime = "" }},
		{name: "failed Volume LIST", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			move := settled
			states := map[string]volumeapi.State{}
			if test.mutate != nil {
				test.mutate(&move, states)
			}
			if got := r.moveNeedsWork(move, states, test.known); got != test.want {
				t.Fatalf("scheduled=%v want=%v: %+v", got, test.want, move)
			}
		})
	}
}
