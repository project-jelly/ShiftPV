package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func installSourceUsageRunner(f *preparationRegistryFixture) {
	f.reconciler.SourceUsage = &helperpod.Runner{Client: f.client, Pools: f.registry, Namespace: "system", Image: "helper", Timeout: time.Hour}
}

func sourceUsagePodForTest(t *testing.T, f *preparationRegistryFixture, move volumeapi.Move) *corev1.Pod {
	t.Helper()
	pods, err := f.client.CoreV1().Pods("system").List(f.ctx, metav1.ListOptions{LabelSelector: helperpod.SourceUsagePurposeLabel + "=true"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		owner := metav1.GetControllerOf(&pods.Items[i])
		if owner != nil && string(owner.UID) == move.UID {
			return &pods.Items[i]
		}
	}
	t.Fatalf("no usage Pod for %s", move.Name)
	return nil
}

func completeSourceUsageForTest(t *testing.T, f *preparationRegistryFixture, move volumeapi.Move) *corev1.Pod {
	t.Helper()
	pod := sourceUsagePodForTest(t, f, move)
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "operation", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "2097152\n"}}}}
	pod, err := f.client.CoreV1().Pods("system").UpdateStatus(f.ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestSourceUsageResponseLossRetainsResultAndRestartsWithoutMeasurement(t *testing.T) {
	for _, protocol := range []string{volumeapi.MoveProtocolPreparedV1} {
		t.Run("protocol="+protocol, func(t *testing.T) {
			f, move := preparationAtCapacity(t)

			installSourceUsageRunner(f)
			before := move
			if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			if got := f.move(t, 0); !reflect.DeepEqual(got, before) {
				t.Fatalf("pending measurement wrote diagnostics or authority: %+v", got)
			}
			pod := completeSourceUsageForTest(t, f, move)
			lost := errors.New("usage stored, reply lost")
			writes := 0
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				bytes, _, _ := unstructured.NestedInt64(object.Object, "status", "sourceBytes")
				if resource == volumeapi.MoveResource && bytes > 0 {
					writes++
					return lost
				}
				return nil
			}
			if err := f.reconciler.reconcileMove(f.ctx, move); !errors.Is(err, errMoveStatusPersistence) || !errors.Is(err, lost) {
				t.Fatalf("ambiguous result: %v", err)
			}
			stored := f.move(t, 0)
			if writes != 1 || stored.Status.SourceBytes != 2097152 || stored.Status.CapacityApproved || stored.Status.Phase != before.Status.Phase {
				t.Fatalf("ambiguous write replayed: writes=%d move=%+v", writes, stored)
			}
			if current := sourceUsagePodForTest(t, f, move); current.UID != pod.UID {
				t.Fatal("result Pod lost before re-observation")
			}
			f.afterWrite = nil
			// New reconciler and Runner, retaining only API objects.
			restarted := *f.reconciler
			restarted.SourceUsage = &helperpod.Runner{Client: f.client, Namespace: "system", Image: "new-image"}
			f.client.ClearActions()
			if err := restarted.reconcileMove(f.ctx, stored); err != nil {
				t.Fatal(err)
			}
			if got := f.move(t, 0); !got.Status.CapacityApproved || got.Status.SourceBytes != 2097152 {
				t.Fatalf("durable result not reused: %+v", got)
			}
			for _, action := range f.client.Actions() {
				if action.Matches("create", "pods") {
					t.Fatal("restart remeasured a persisted result")
				}
			}
			pods, err := f.client.CoreV1().Pods("system").List(f.ctx, metav1.ListOptions{LabelSelector: helperpod.SourceUsagePurposeLabel + "=true"})
			if err != nil || len(pods.Items) != 0 {
				t.Fatalf("result not retired: pods=%d err=%v", len(pods.Items), err)
			}
		})
	}
}

func TestSourceUsageCancellationRestoresSourceBeforeHelperAbsence(t *testing.T) {
	f, move := preparationAtCapacity(t)
	installSourceUsageRunner(f)
	if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	pod := sourceUsagePodForTest(t, f, move)
	retain := true
	f.client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if retain && action.(ktesting.DeleteAction).GetName() == pod.Name {
			return true, nil, nil
		}
		return false, nil, nil
	})
	// An already admitted cancellation must settle even when its measurement
	// helper outlives the decision. This does not admit a fresh cancellation.
	move = f.move(t, 0)
	previous := move.Status
	move.Status.Phase = "Canceling"
	if err := f.reconciler.persistMoveStatus(f.ctx, &move, previous); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
	}
	state, cancelled := f.state(t, 0), f.move(t, 0)
	if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || cancelled.Status.Phase != "Cancelled" || cancelled.Status.Preparation != nil ||
		!slices.Contains(cancelled.Finalizers, volumeapi.MoveProtectionFinalizer) {
		t.Fatalf("helper absence blocked source restoration or released journal early: state=%+v move=%+v", state, cancelled)
	}
	retain = false
	if err := f.reconciler.reconcilePersistedMove(f.ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(f.move(t, 0).Finalizers, volumeapi.MoveProtectionFinalizer) {
		t.Fatal("settled helper retained Move finalizer")
	}
}

func TestSourceUsageHintRevisitsSettledHistoryAndFencesWrongIdentity(t *testing.T) {
	for _, variant := range []string{"owned", "relabeled", "other-UID"} {
		t.Run(variant, func(t *testing.T) {
			forged := variant == "other-UID"
			f, move := preparationAtCapacity(t)
			installSourceUsageRunner(f)
			if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			pod := sourceUsagePodForTest(t, f, move)
			if variant == "relabeled" {
				pod.Labels = map[string]string{"shiftpv.io/volume-id": "different-volume"}
				if _, err := f.client.CoreV1().Pods("system").Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if forged {
				pod.OwnerReferences[0].UID = "another-move-uid"
				if _, err := f.client.CoreV1().Pods("system").Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			// Settled history already retired placement and released its lock.
			if err := f.client.CoreV1().Pods("system").Delete(f.ctx, namesFor(move.Name).PlacementPod, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			node.Spec.Unschedulable = false
			if _, err = f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			// Simulate a terminal journal written by an older controller that
			// did not know this new helper. Its normal history scan skips it.
			object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			object.SetFinalizers(nil)
			unstructured.RemoveNestedField(object.Object, "status", "preparation")
			_ = unstructured.SetNestedField(object.Object, "Cancelled", "status", "phase")
			_ = unstructured.SetNestedField(object.Object, time.Now().UTC().Format(time.RFC3339Nano), "status", "lastTransitionTime")
			if _, err = f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
				t.Fatal(err)
			}
			object, err = f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, move.Spec.VolumeID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_ = unstructured.SetNestedField(object.Object, "Ready", "status", "phase")
			unstructured.RemoveNestedField(object.Object, "status", "activeMove")
			if _, err = f.commitDynamicUpdate(volumeapi.VolumeResource, object); err != nil {
				t.Fatal(err)
			}
			current := f.move(t, 0)
			if f.reconciler.moveNeedsWork(current, map[string]volumeapi.State{move.Spec.VolumeID: f.state(t, 0)}, true) {
				t.Fatal("fixture not skipped by history scan")
			}
			f.client.ClearActions()
			if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
				t.Fatal(err)
			}
			_, err = f.client.CoreV1().Pods("system").Get(f.ctx, pod.Name, metav1.GetOptions{})
			if forged && err != nil {
				t.Fatal("forged routing hint deleted another UID's helper")
			}
			if !forged && err == nil {
				t.Fatal("settled history stranded its readonly helper")
			}
		})
	}
}

func TestSourceUsagePendingPodsDoNotOccupyWorkers(t *testing.T) {
	f := newPreparationRegistryFixture(t, mobilityWorkers+2)
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(pool.Object, "20Gi", "spec", "capacity", "limit")
	if _, err = f.commitDynamicUpdate(volumeapi.PoolResource, pool); err != nil {
		t.Fatal(err)
	}
	pending := mobilityWorkers + 1
	before := make([]volumeapi.Move, pending)
	for index := range pending {
		move := f.untilPrepared(t, index)
		state := f.state(t, index)
		next := state
		next.Phase = volumeapi.PhaseMoving
		if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, state, next); err != nil {
			t.Fatal(err)
		}
		if err := f.registry.ReconcilePublished(f.ctx, move.Spec.VolumeID, "source", *state.CurrentCopy, false); err != nil {
			t.Fatal(err)
		}
		status := move.Status
		status.Phase, status.EvictionRequested = "WaitingForCapacity", true
		before[index], err = f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
		if err != nil {
			t.Fatal(err)
		}
	}
	installSourceUsageRunner(f) // one-hour helper budget cannot free a worker in this test
	f.reconciler.Interval = 10 * time.Millisecond
	stop := runDispatcherForTest(t, f.reconciler)
	waitDispatcherCondition(t, "pending usage Pods starved independent preparation", func() bool {
		pods, err := f.client.CoreV1().Pods("system").List(context.Background(), metav1.ListOptions{LabelSelector: helperpod.SourceUsagePurposeLabel + "=true"})
		return err == nil && len(pods.Items) == pending && f.move(t, pending).Status.SourceCopy != nil
	})
	stop()
	for index, expected := range before {
		if got := f.move(t, index); !reflect.DeepEqual(got, expected) {
			t.Fatalf("pending usage changed authority: before=%+v after=%+v", expected, got)
		}
	}
}

type sourceUsageAfterResult struct {
	SourceUsage
	after func()
}

func (s sourceUsageAfterResult) AdvanceSourceUsage(ctx context.Context, move volumeapi.Move, copy volume.CopyIdentity) (int64, bool, error) {
	bytes, done, err := s.SourceUsage.AdvanceSourceUsage(ctx, move, copy)
	if done && err == nil {
		s.after()
	}
	return bytes, done, err
}

func TestSourceUsageResultRechecksPublicationBeforeCAS(t *testing.T) {
	f, move := preparationAtCapacity(t)
	installSourceUsageRunner(f)
	if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	completeSourceUsageForTest(t, f, move)
	f.reconciler.SourceUsage = sourceUsageAfterResult{SourceUsage: f.reconciler.SourceUsage, after: func() {
		// Simulate an intervening API writer after observing a successful Pod.
		object, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, move.Spec.VolumeID, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedSlice(object.Object, []any{"source"}, "status", "publishedNodes")
		if _, err = f.commitDynamicUpdate(volumeapi.VolumeResource, object); err != nil {
			t.Fatal(err)
		}
	}}
	f.client.ClearActions()
	if err := f.reconciler.reconcileMove(f.ctx, move); !errors.Is(err, errMoveStatusPersistence) {
		t.Fatalf("changed publication accepted: %v", err)
	}
	if got := f.move(t, 0); !reflect.DeepEqual(got, move) {
		t.Fatalf("changed publication persisted measurement/diagnostics: %+v", got)
	}
	for _, action := range f.client.Actions() {
		if action.Matches("delete", "pods") {
			t.Fatal("rejected result lost before re-observation")
		}
	}
}
