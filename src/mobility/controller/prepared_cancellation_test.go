package controller

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

func TestPreparedCancellationResumesAfterCancelledResponseLossWithoutBinding(t *testing.T) {
	f := newCancellationFixture(t)
	w := f.workloads[0]
	for _, err := range []error{
		f.client.CoreV1().Pods(w.Consumer.Namespace).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}),
		f.client.CoreV1().PersistentVolumeClaims(w.Claim.Namespace).Delete(f.ctx, w.Claim.Name, metav1.DeleteOptions{}),
		f.client.CoreV1().PersistentVolumes().Delete(f.ctx, w.PV.Name, metav1.DeleteOptions{}),
		f.client.CoreV1().Nodes().Delete(f.ctx, "source", metav1.DeleteOptions{}),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	before := f.state(t, 0)
	lost := false
	f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
		phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
		if resource == volumeapi.MoveResource && phase == "Cancelled" && !lost {
			lost = true
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil { // DELETE placement
		t.Fatal(err)
	}
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); !errors.Is(err, errMoveStatusPersistence) {
		t.Fatalf("lost cancellation status response was not fenced: %v", err)
	}
	move := f.move(t, 0)
	if !lost || move.Status.Phase != "Cancelled" || move.Status.Preparation != nil || f.state(t, 0).ActiveMove != move.Name {
		t.Fatalf("hold release and Volume unlock were not separate durable steps: %+v", move.Status)
	}
	f.reconciler = newTestReconciler(f.client, f.registry)
	f.client.ClearActions()
	finishCancellation(t, f)
	after := f.state(t, 0)
	before.ActiveMove = ""
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("cancellation changed source authority/publication: before=%+v after=%+v", before, after)
	}
	for _, action := range f.client.Actions() {
		if action.GetResource().Resource != "pods" || action.GetVerb() != "get" || action.GetNamespace() != f.reconciler.Namespace {
			t.Fatalf("no-disk cancellation consulted workload availability: %#v", action)
		}
	}
}

func TestPreparedCancellationFencesDelayedMovingWrite(t *testing.T) {
	for _, deliverAfterUnlock := range []bool{false, true} {
		t.Run(map[bool]string{false: "after Cancelled", true: "after unlock"}[deliverAfterUnlock], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			move := f.untilPrepared(t, 0)
			previous := move.Status
			move.Status.Phase = "StartingDisruption"
			if err := f.reconciler.persistMoveStatus(f.ctx, &move, previous); err != nil {
				t.Fatal(err)
			}
			expected := f.state(t, 0)
			next := expected
			next.Phase = volumeapi.PhaseMoving
			var delayed *unstructured.Unstructured
			f.dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
				object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
				if phase == volumeapi.PhaseMoving && delayed == nil {
					delayed = object.DeepCopy()
					return true, nil, context.DeadlineExceeded
				}
				return false, nil, nil
			})
			if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, expected, next); !errors.Is(err, context.DeadlineExceeded) || delayed == nil {
				t.Fatalf("did not capture the actual timed-out Moving PUT: %v", err)
			}
			if err := f.reconciler.requestPreparationCancellation(f.ctx, &move, "SourceUncordoned"); err != nil {
				t.Fatal(err)
			}
			for range 2 { // placement absence, then durable Cancelled
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
			}
			if f.move(t, 0).Status.Phase != "Cancelled" || f.state(t, 0).ActiveMove != move.Name {
				t.Fatal("did not reach the crash boundary before unlock")
			}
			if deliverAfterUnlock {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, delayed); !apierrors.IsConflict(err) {
					t.Fatalf("late PUT revived the cancelled lock: %v", err)
				}
				if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, expected, next); !errors.Is(err, volumeapi.ErrStateConflict) {
					t.Fatalf("retry revived the cancelled lock: %v", err)
				}
			} else {
				if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, delayed); err != nil {
					t.Fatalf("late pre-unlock PUT should still match its Volume RV: %v", err)
				}
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				state := f.state(t, 0)
				if state.Phase != volumeapi.PhaseReady || state.ActiveMove != move.Name {
					t.Fatalf("Cancelled handler did not restore Moving before unlock: %+v", state)
				}
			}
			finishCancellation(t, f)
			if state := f.state(t, 0); state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || !reflect.DeepEqual(state.CurrentCopy, expected.CurrentCopy) {
				t.Fatalf("delayed PUT broke source restoration: %+v", state)
			}
		})
	}
}

func TestPreparedCancellationFencesDelayedInitialClaim(t *testing.T) {
	for _, arrival := range []string{"before cancellation claim", "after cancellation claim", "after journal deletion"} {
		t.Run(arrival, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil { // Persist the exact source plan.
				t.Fatal(err)
			}
			move := f.move(t, 0)
			if move.Status.SourceCopy == nil || f.state(t, 0).ActiveMove != "" {
				t.Fatal("source plan was not stored before the initial claim")
			}
			var delayed *unstructured.Unstructured
			f.dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
				object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				active, _, _ := unstructured.NestedString(object.Object, "status", "activeMove")
				if active == move.Name && delayed == nil {
					delayed = object.DeepCopy()
					return true, nil, context.DeadlineExceeded
				}
				return false, nil, nil
			})
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); !errors.Is(err, context.DeadlineExceeded) || delayed == nil {
				t.Fatalf("did not capture the actual initial claim PUT: %v", err)
			}
			if err := f.reconciler.requestPreparationCancellation(f.ctx, &move, "SourceUncordoned"); err != nil {
				t.Fatal(err)
			}
			if arrival == "before cancellation claim" {
				if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, delayed); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				state := f.state(t, 0)
				if arrival == "after cancellation claim" && (state.Phase != volumeapi.PhaseReady || state.ActiveMove != move.Name || f.move(t, 0).Status.Phase != "Canceling") {
					t.Fatal("cancellation must durably claim the Ready source before retiring its journal")
				}
				if arrival == "after journal deletion" {
					finishCancellation(t, f)
					if err := f.registry.DeleteMove(f.ctx, move.Name, move.UID); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, delayed); !apierrors.IsConflict(err) {
					t.Fatalf("delayed initial claim bypassed cancellation fence: %v", err)
				}
			}
			if arrival != "after journal deletion" {
				finishCancellation(t, f)
			}
			state := f.state(t, 0)
			if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || !reflect.DeepEqual(state.CurrentCopy, move.Status.SourceCopy) {
				t.Fatalf("cancellation left an orphan claim: %+v", state)
			}
			assertNoEviction(t, f.client)
		})
	}
}

type cancellationPublicationRepository struct {
	Repository
	beforeCAS func()
}

func (r *cancellationPublicationRepository) CompareAndSetSourceState(ctx context.Context, id string, expected, next volumeapi.State) error {
	if r.beforeCAS != nil {
		r.beforeCAS()
		r.beforeCAS = nil
	}
	return r.Repository.CompareAndSetSourceState(ctx, id, expected, next)
}

func TestPreparedCancellationPreservesConcurrentPublication(t *testing.T) {
	for _, restoring := range []bool{true, false} {
		t.Run(map[bool]string{true: "restore observes unpublish", false: "unlock observes publish"}[restoring], func(t *testing.T) {
			f := newCancellationFixture(t)
			want := []string{"source"}
			if restoring {
				state := f.state(t, 0)
				next := state
				next.Phase = volumeapi.PhaseMoving
				if err := f.registry.CompareAndSetSourceState(f.ctx, f.volumeIDs[0], state, next); err != nil {
					t.Fatal(err)
				}
				want = nil
			} else {
				for range 2 {
					if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
						t.Fatal(err)
					}
				}
				setCancellationPublications(t, f, nil)
			}
			wrapper := &cancellationPublicationRepository{Repository: f.registry}
			wrapper.beforeCAS = func() { setCancellationPublications(t, f, want) }
			f.reconciler.Repository = wrapper
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			got := f.state(t, 0).PublishedNodes
			if !slices.Equal(got, want) {
				t.Fatalf("stale cancellation snapshot overwrote publication: got=%v want=%v", got, want)
			}
			finishCancellation(t, f)
		})
	}
}

func TestPreparedCancellationWaitsForExactPlacementAbsence(t *testing.T) {
	for _, boundary := range []string{"recreated UID", "terminating", "delete response lost"} {
		t.Run(boundary, func(t *testing.T) {
			f := newCancellationFixture(t)
			move := f.move(t, 0)
			name := namesFor(move.Name).PlacementPod
			pods := f.client.CoreV1().Pods(f.reconciler.Namespace)
			pod, err := pods.Get(f.ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch boundary {
			case "recreated UID":
				if err := pods.Delete(f.ctx, name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				pod.UID, pod.ResourceVersion = "", ""
				if _, err := pods.Create(f.ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "terminating":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				if _, err := pods.Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "delete response lost":
				f.client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
					deletion := action.(ktesting.DeleteAction)
					precondition := deletion.GetDeleteOptions().Preconditions
					if precondition == nil || precondition.UID == nil || string(*precondition.UID) != move.Status.PlacementPodUID {
						t.Fatal("placement DELETE was not fenced to saved UID")
					}
					if err := f.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), action.GetNamespace(), deletion.GetName()); err != nil {
						t.Fatal(err)
					}
					return true, nil, io.ErrUnexpectedEOF
				})
			}
			f.client.ClearActions()
			err = f.reconciler.reconcilePersistedMove(f.ctx, move)
			if (err != nil) != (boundary != "terminating") {
				t.Fatalf("unexpected placement cancellation result: %v", err)
			}
			stored := f.move(t, 0)
			if stored.Status.Phase != "Canceling" || stored.Status.Preparation == nil || f.state(t, 0).ActiveMove != move.Name {
				t.Fatal("placement without observed absence released its holds")
			}
			if boundary == "recreated UID" {
				for _, action := range f.client.Actions() {
					if action.GetVerb() == "delete" {
						t.Fatal("deleted a same-name replacement placement")
					}
				}
				return
			}
			if boundary == "terminating" {
				if err := f.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name); err != nil {
					t.Fatal(err)
				}
			}
			finishCancellation(t, f)
		})
	}
}

func TestPreparedCancellationRejectsDiskAuthority(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*volumeapi.MoveStatus)
	}{
		{"approved", func(s *volumeapi.MoveStatus) { s.CapacityApproved = true }},
		{"incoming copy", func(s *volumeapi.MoveStatus) { s.IncomingCopy = s.SourceCopy }},
		{"destination copy", func(s *volumeapi.MoveStatus) { s.DestinationCopy = s.SourceCopy }},
		{"copy operation", func(s *volumeapi.MoveStatus) { s.CopyOperationID = "copy-op" }},
		{"promotion operation", func(s *volumeapi.MoveStatus) { s.PromotionOperationID = "promote-op" }},
		{"copy job", func(s *volumeapi.MoveStatus) { s.CopyJobName = "copy-job" }},
		{"promotion job", func(s *volumeapi.MoveStatus) { s.PromotionJobName = "promote-job" }},
		{"cleanup", func(s *volumeapi.MoveStatus) { s.CleanupPhase = "Completed" }},
		{"rollback", func(s *volumeapi.MoveStatus) { s.RollbackRequiredGeneration = 7 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCancellationFixture(t)
			move := f.move(t, 0)
			test.set(&move.Status)
			f.client.ClearActions()
			f.dynamic.ClearActions()
			if err := f.reconciler.requestPreparationCancellation(f.ctx, &move, "cancel"); err == nil {
				t.Fatal("execution authority was accepted for preparation cancellation")
			}
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err == nil {
				t.Fatal("execution authority entered cancellation effects")
			}
			if len(f.client.Actions()) != 0 || len(f.dynamic.Actions()) != 0 {
				t.Fatal("invalid cancellation touched API state")
			}
		})
	}
}

func TestPreparedCancellationBeforeReservation(t *testing.T) {
	for _, withPlacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "no plan", true: "unrecorded placement UID"}[withPlacement], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			move := f.move(t, 0)
			if withPlacement {
				for range 8 {
					if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
						t.Fatal(err)
					}
					move = f.move(t, 0)
					_, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
					if err == nil {
						break
					}
					if !apierrors.IsNotFound(err) {
						t.Fatal(err)
					}
				}
				pod, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
				if err != nil || pod.UID == "" || move.Status.PlacementPodUID != "" || move.Status.Preparation != nil {
					t.Fatalf("did not reach pre-reservation placement boundary: move=%+v err=%v", move.Status, err)
				}
			}
			if err := f.reconciler.requestPreparationCancellation(f.ctx, &move, "SourceUncordoned"); err != nil {
				t.Fatal(err)
			}
			finishCancellation(t, f)
			if _, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("pre-reservation placement survived cancellation: %v", err)
			}
		})
	}
}

func TestPreparedCancellationRetainsContradictorySource(t *testing.T) {
	for _, changed := range []string{"owner", "copy", "active Move", "volume UID", "missing plan with owned lock"} {
		t.Run(changed, func(t *testing.T) {
			f := newCancellationFixture(t)
			move := f.move(t, 0)
			object, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, move.Spec.VolumeID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch changed {
			case "owner":
				err = unstructured.SetNestedField(object.Object, "destination", "status", "ownerNode")
			case "copy":
				err = unstructured.SetNestedField(object.Object, "different-copy", "status", "currentCopy", "copyID")
			case "active Move":
				err = unstructured.SetNestedField(object.Object, "another-move", "status", "activeMove")
			case "volume UID":
				object.SetUID("recreated-volume-uid")
			case "missing plan with owned lock":
				move.Status.SourceCopy, move.Status.Preparation = nil, nil
			}
			if err != nil {
				t.Fatal(err)
			}
			// This represents a contradictory live observation, including a
			// replaced Volume incarnation, not a permitted prepared transition.
			if err := f.dynamic.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
				t.Fatal(err)
			}
			f.client.ClearActions()
			f.dynamic.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err == nil {
				t.Fatal("cancellation adopted contradictory source authority")
			}
			for _, actions := range [][]ktesting.Action{f.client.Actions(), f.dynamic.Actions()} {
				for _, action := range actions {
					if action.GetVerb() != "get" {
						t.Fatalf("contradictory cancellation mutated state: %#v", action)
					}
				}
			}
		})
	}
}

func TestPreparedCancelledJournalDoesNotRestoreLaterMove(t *testing.T) {
	f := newCancellationFixture(t)
	finishCancellation(t, f)
	object, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, f.volumeIDs[0], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(object.Object, "later-move", "status", "activeMove"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(object.Object, volumeapi.PhaseMoving, "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dynamic.Resource(volumeapi.VolumeResource).UpdateStatus(f.ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	before := f.state(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	if after := f.state(t, 0); !reflect.DeepEqual(before, after) {
		t.Fatalf("old cancelled Move changed later transaction: before=%+v after=%+v", before, after)
	}
}

func TestPreparedCancelledMoveAllowsSameHeldPodOwnerRecovery(t *testing.T) {
	f := newCancellationFixture(t)
	w := f.workloads[0]
	if err := f.client.CoreV1().Pods(w.Consumer.Namespace).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := w.Consumer.DeepCopy()
	pod.Name, pod.UID, pod.ResourceVersion, pod.Spec.NodeName = "held-replacement", "", "", ""
	pod.Status.Phase = corev1.PodPending
	pod.Labels = map[string]string{admission.ManagedLabel: "true"}
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}, {Name: "example.test/hold"}}
	pod, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Spec.Unschedulable = false
	if _, err := f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.reconcileOwnerPlacements(f.ctx); err != nil {
		t.Fatal(err)
	}
	held, _ := f.client.CoreV1().Pods(pod.Namespace).Get(f.ctx, pod.Name, metav1.GetOptions{})
	if !hasPlacementHold(held) {
		t.Fatal("owner placement bypassed an unsettled cancellation")
	}
	finishCancellation(t, f)
	if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := f.client.CoreV1().Pods(pod.Namespace).Get(f.ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if recovered.UID != pod.UID || hasPlacementHold(recovered) || recovered.Spec.NodeSelector[corev1.LabelHostname] != "source" ||
		!reflect.DeepEqual(recovered.Spec.SchedulingGates, []corev1.PodSchedulingGate{{Name: "example.test/hold"}}) {
		t.Fatalf("settled cancellation did not recover the same held Pod: %+v", recovered)
	}
}

func newCancellationFixture(t *testing.T) *preparationRegistryFixture {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	move := f.untilPrepared(t, 0)
	if err := f.reconciler.requestPreparationCancellation(f.ctx, &move, "SourceUncordoned"); err != nil {
		t.Fatal(err)
	}
	return f
}

func finishCancellation(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	for range 8 {
		move := f.move(t, 0)
		if move.Status.Phase == "Cancelled" && move.Status.Preparation == nil && f.state(t, 0).ActiveMove == "" && len(move.Finalizers) == 0 {
			return
		}
		if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("cancellation did not settle: move=%+v state=%+v", f.move(t, 0), f.state(t, 0))
}

func setCancellationPublications(t *testing.T, f *preparationRegistryFixture, nodes []string) {
	t.Helper()
	object, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, f.volumeIDs[0], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedStringSlice(object.Object, nodes, "status", "publishedNodes"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dynamic.Resource(volumeapi.VolumeResource).UpdateStatus(f.ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
