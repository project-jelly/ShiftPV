package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestDisruptedPreparationResumesAfterDestinationRecovers(t *testing.T) {
	for _, fault := range []string{"NoSchedule taint", "NotReady", "cordon"} {
		for _, restart := range []bool{false, true} {
			t.Run(fault+map[bool]string{false: "", true: "/restart"}[restart], func(t *testing.T) {
				f, move, replacement := disruptedPreparationFixture(t)
				before := f.state(t, 0)
				node, err := f.client.CoreV1().Nodes().Get(f.ctx, "destination", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				original := node.DeepCopy()
				switch fault {
				case "NoSchedule taint":
					node.Spec.Taints = []corev1.Taint{{Key: "example.test/unavailable", Effect: corev1.TaintEffectNoSchedule}}
				case "NotReady":
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				case "cordon":
					node.Spec.Unschedulable = true
				}
				if _, err = f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				waiting := f.move(t, 0)
				if waiting.Status.Phase != move.Status.Phase || waiting.Status.CapacityApproved ||
					!reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(before, f.state(t, 0)) {
					t.Fatalf("transient fault abandoned or changed the disrupted transaction: move=%+v state=%+v", waiting.Status, f.state(t, 0))
				}
				assertPreparationReserved(t, f, preparationRequestedBytes)
				singleLifecycleNoDiskWork(t, f)
				if restart {
					restartDisruptedReconciler(f)
				}
				if _, err = f.client.CoreV1().Nodes().Update(f.ctx, original, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				assertDisruptedMoveResumes(t, f, move, replacement)
			})
		}
	}
}

func TestDisruptedPreparationResumesAfterReservedPoolRecovers(t *testing.T) {
	f, move, replacement := disruptedPreparationFixture(t)
	before := f.state(t, 0)
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := pool.DeepCopy()
	conditions, _, err := unstructured.NestedSlice(pool.Object, "status", "conditions")
	if err != nil || len(conditions) != 1 {
		t.Fatalf("unexpected readiness fixture: %+v, %v", conditions, err)
	}
	conditions[0].(map[string]any)["status"] = "False"
	if err = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	pool, err = f.dynamic.Resource(volumeapi.PoolResource).UpdateStatus(f.ctx, pool, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	waiting := f.move(t, 0)
	if waiting.Status.Phase != move.Status.Phase || !reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatalf("Pool unavailability abandoned its exact reservation: move=%+v state=%+v", waiting.Status, f.state(t, 0))
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)
	restartDisruptedReconciler(f)
	original.SetResourceVersion(pool.GetResourceVersion())
	if _, err = f.dynamic.Resource(volumeapi.PoolResource).UpdateStatus(f.ctx, original, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	assertDisruptedMoveResumes(t, f, move, replacement)
}

func TestDisruptedPreparationResumesAfterSourceRecovers(t *testing.T) {
	f, move, replacement := disruptedPreparationFixture(t)
	before := f.state(t, 0)
	node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := node.DeepCopy()
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	if _, err = f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	waiting := f.move(t, 0)
	if waiting.Status.Phase != move.Status.Phase || !reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatalf("temporary source unavailability abandoned authority: move=%+v state=%+v", waiting.Status, f.state(t, 0))
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)
	restartDisruptedReconciler(f)
	if _, err = f.client.CoreV1().Nodes().Update(f.ctx, original, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	assertDisruptedMoveResumes(t, f, move, replacement)
}

func TestDisruptionRecoversAcceptedEvictionWithLostResponse(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	w := f.workloads[0]
	applied := false
	f.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" || applied {
			return false, nil, nil
		}
		resource := corev1.SchemeGroupVersion.WithResource("pods")
		object, err := f.client.Tracker().Get(resource, w.Consumer.Namespace, w.Consumer.Name)
		if err != nil {
			return true, nil, err
		}
		pod := object.(*corev1.Pod).DeepCopy()
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		if err = f.client.Tracker().Update(resource, pod, pod.Namespace); err != nil {
			return true, nil, err
		}
		applied = true
		return true, nil, context.DeadlineExceeded
	})
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fixture did not lose the accepted eviction response: %v", err)
	}
	move := f.move(t, 0)
	if !applied || move.Status.Phase != "StartingDisruption" || move.Status.EvictionRequested {
		t.Fatalf("fixture did not preserve the ambiguous eviction boundary: %+v", move.Status)
	}
	// Observe the actual Terminating original, without clearing its deletion
	// timestamp or adding an unrelated destination fault.
	restartDisruptedReconciler(f)
	if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	if phase := f.move(t, 0).Status.Phase; phase == "Canceling" || phase == "Cancelled" || phase == "Blocked" {
		t.Fatalf("accepted eviction was mistaken for a pre-disruption change: %s", phase)
	}
	replacement := singleLifecycleReplacement(t, f)
	unpublishDisruptedSource(t, f)
	assertDisruptedMoveResumes(t, f, move, replacement)
}

func TestDisruptionWaitsForAdmissionBeforeEvictingOriginal(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	move, source := f.move(t, 0), f.state(t, 0)
	w := f.workloads[0]
	ns, err := f.client.CoreV1().Namespaces().Get(f.ctx, w.Namespace.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := ns.DeepCopy()
	delete(ns.Labels, admissionNamespaceLabel)
	if _, err = f.client.CoreV1().Namespaces().Update(f.ctx, ns, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	waiting := f.move(t, 0)
	if waiting.Status.Phase != "StartingDisruption" || waiting.Status.Reason != "AdmissionNotEnabled" || waiting.Status.EvictionRequested ||
		!reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(source, f.state(t, 0)) {
		t.Fatalf("unobserved consumer was treated as already evicted: move=%+v source=%+v", waiting.Status, f.state(t, 0))
	}
	pod, err := f.client.CoreV1().Pods(w.Consumer.Namespace).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
	if err != nil || pod.UID != w.Consumer.UID || pod.DeletionTimestamp != nil {
		t.Fatalf("admission wait disrupted the original consumer: pod=%+v error=%v", pod, err)
	}
	assertNoEviction(t, f.client)
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)
	restartDisruptedReconciler(f)
	if _, err = f.client.CoreV1().Namespaces().Update(f.ctx, original, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
		if f.move(t, 0).Status.EvictionRequested {
			break
		}
	}
	evicted := false
	for _, action := range f.client.Actions() {
		if action.GetSubresource() == "eviction" {
			evicted = true
		}
	}
	if !evicted || !f.move(t, 0).Status.EvictionRequested {
		t.Fatal("restored admission did not submit the original eviction")
	}
	replacement := singleLifecycleReplacement(t, f)
	unpublishDisruptedSource(t, f)
	assertDisruptedMoveResumes(t, f, move, replacement)
}

func TestDisruptionResumesWhenCompletedOriginalRemainsInAPI(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			f.untilPrepared(t, 0)
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			move := f.move(t, 0)
			w := f.workloads[0]
			pod, err := f.client.CoreV1().Pods(w.Consumer.Namespace).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase = phase
			if _, err = f.client.CoreV1().Pods(pod.Namespace).UpdateStatus(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if got := f.move(t, 0); got.Status.Phase != "WaitingForUnpublish" || !got.Status.EvictionRequested {
				t.Fatalf("completed original stalled disruption or bypassed unpublish: %+v", got.Status)
			}
			assertNoEviction(t, f.client)
			replacement := w.Consumer.DeepCopy()
			replacement.Name, replacement.UID, replacement.ResourceVersion = "held-replacement", "", ""
			replacement.Spec.NodeName = ""
			replacement.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
			replacement.Status = corev1.PodStatus{Phase: corev1.PodPending}
			replacement, err = f.client.CoreV1().Pods(replacement.Namespace).Create(f.ctx, replacement, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			unpublishDisruptedSource(t, f)
			assertDisruptedMoveResumes(t, f, move, replacement)
		})
	}
}

func TestDisruptedPreparationSettlesWhenOriginalClaimRetires(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	f.untilPrepared(t, 0)
	for range 2 {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
	}
	move, source := f.move(t, 0), f.state(t, 0)
	w := f.workloads[0]
	if err := f.client.CoreV1().PersistentVolumeClaims(w.Claim.Namespace).Delete(f.ctx, w.Claim.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	settled, state := f.move(t, 0), f.state(t, 0)
	if settled.UID != move.UID || settled.Status.Phase != "Cancelled" || state.ActiveMove != "" || state.Phase != volumeapi.PhaseReady ||
		state.OwnerNode != source.OwnerNode || !reflect.DeepEqual(state.CurrentCopy, source.CurrentCopy) {
		t.Fatalf("retired claim retained a no-disk transaction or changed authority: move=%+v state=%+v", settled.Status, state)
	}
	assertPreparationReserved(t, f, 0)
	singleLifecycleNoDiskWork(t, f)
}

func TestDeletingDisruptionRetainsAuthorityUntilOriginalClaimRetires(t *testing.T) {
	for _, retireFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "live then retired", true: "already retired"}[retireFirst], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			f.untilPrepared(t, 0)
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			move, source := f.move(t, 0), f.state(t, 0)
			object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			now := metav1.Now()
			object.SetDeletionTimestamp(&now)
			if _, err = f.dynamic.Resource(volumeapi.MoveResource).Update(f.ctx, object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if !retireFirst {
				for range 3 {
					if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
						t.Fatal(err)
					}
				}
				blocked, state := f.move(t, 0), f.state(t, 0)
				if blocked.Status.Phase != "Blocked" || blocked.Status.Reason != "MoveDeleting" || blocked.Status.CapacityApproved ||
					!reflect.DeepEqual(blocked.Status.Preparation, move.Status.Preparation) || state.Phase != volumeapi.PhaseBlocked ||
					state.ActiveMove != move.Name || !reflect.DeepEqual(state.CurrentCopy, source.CurrentCopy) {
					t.Fatalf("Move deletion released live workload authority: move=%+v state=%+v", blocked.Status, state)
				}
				assertPreparationReserved(t, f, preparationRequestedBytes)
			}
			w := f.workloads[0]
			if err = f.client.CoreV1().PersistentVolumeClaims(w.Claim.Namespace).Delete(f.ctx, w.Claim.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			for range 12 {
				if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				if f.move(t, 0).Status.Phase == "Cancelled" && f.state(t, 0).ActiveMove == "" {
					break
				}
			}
			settled, state := f.move(t, 0), f.state(t, 0)
			if settled.Status.Phase != "Cancelled" || state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || !reflect.DeepEqual(state.CurrentCopy, source.CurrentCopy) {
				t.Fatalf("Move deletion masked original-claim retirement: move=%+v state=%+v", settled.Status, state)
			}
			assertPreparationReserved(t, f, 0)
			assertNoEviction(t, f.client)
			singleLifecycleNoDiskWork(t, f)
		})
	}
}

func TestDisruptedPreparationPreservesAuthorityOnUnknownObservation(t *testing.T) {
	f, move, replacement := disruptedPreparationFixture(t)
	before := f.state(t, 0)
	unavailable := true
	f.client.PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		if unavailable && action.(ktesting.GetAction).GetName() == "destination" {
			return true, nil, apierrors.NewServiceUnavailable("destination observation unknown")
		}
		return false, nil, nil
	})
	if err := f.reconciler.ReconcileAll(f.ctx); err == nil {
		t.Fatal("unknown observation was swallowed as a scheduling verdict")
	}
	stored := f.move(t, 0)
	if stored.Status.Phase != move.Status.Phase || !reflect.DeepEqual(stored.Status.Preparation, move.Status.Preparation) ||
		!reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatalf("unknown observation changed authority: move=%+v state=%+v", stored.Status, f.state(t, 0))
	}
	singleLifecycleNoDiskWork(t, f)
	unavailable = false
	assertDisruptedMoveResumes(t, f, move, replacement)
}

func TestDisruptedPreparationRejectsRecreatedReservation(t *testing.T) {
	f, move, _ := disruptedPreparationFixture(t)
	names := namesFor(move.Name)
	pod, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, names.PlacementPod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.client.CoreV1().Pods(pod.Namespace).Delete(f.ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.UID, pod.ResourceVersion = "", ""
	if _, err = f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	stored, source := f.move(t, 0), f.state(t, 0)
	if stored.Status.Phase != "Blocked" || stored.Status.PlacementPodUID != move.Status.PlacementPodUID ||
		stored.Status.CapacityApproved || !reflect.DeepEqual(stored.Status.Preparation, move.Status.Preparation) || source.ActiveMove != move.Name || source.OwnerNode != "source" {
		t.Fatalf("reservation identity contradiction was cancelled or adopted: move=%+v source=%+v", stored.Status, source)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)
	// Once the exact original claim ends, service resumption is no longer
	// required. Even this blocked, unapproved transaction must be settleable.
	w := f.workloads[0]
	if err = f.client.CoreV1().PersistentVolumeClaims(w.Claim.Namespace).Delete(f.ctx, w.Claim.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if settled, state := f.move(t, 0), f.state(t, 0); settled.Status.Phase != "Cancelled" || state.ActiveMove != "" || state.Phase != volumeapi.PhaseReady {
		t.Fatalf("retired original claim could not settle the blocked preparation: move=%+v state=%+v", settled.Status, state)
	}
	assertPreparationReserved(t, f, 0)
}

func disruptedPreparationFixture(t *testing.T) (*preparationRegistryFixture, volumeapi.Move, *corev1.Pod) {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	f.untilPrepared(t, 0)
	for range 2 {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
	}
	move := f.move(t, 0)
	if !move.Status.EvictionRequested || move.Status.Phase != "WaitingForUnpublish" {
		t.Fatalf("fixture did not execute disruption: %+v", move.Status)
	}
	replacement := singleLifecycleReplacement(t, f)
	unpublishDisruptedSource(t, f)
	return f, move, replacement
}

func unpublishDisruptedSource(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	state := f.state(t, 0)
	if err := f.registry.ReconcilePublished(f.ctx, f.volumeIDs[0], "source", *state.CurrentCopy, false); err != nil {
		t.Fatal(err)
	}
}

func restartDisruptedReconciler(f *preparationRegistryFixture) {
	old := f.reconciler
	f.reconciler = newTestReconciler(f.client, f.registry, func(r *Reconciler) {
		r.CapacityProbe, r.SourceUsage = old.CapacityProbe, old.SourceUsage
		r.Cleanups, r.CleanupOperator = old.Cleanups, old.CleanupOperator
	})
}

func assertDisruptedMoveResumes(t *testing.T, f *preparationRegistryFixture, original volumeapi.Move, replacement *corev1.Pod) {
	t.Helper()
	for range 16 {
		if f.move(t, 0).Status.Phase == "Copying" {
			break
		}
		if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	move, state := f.move(t, 0), f.state(t, 0)
	if move.UID != original.UID || move.Status.Phase != "Copying" || !move.Status.CapacityApproved ||
		state.ActiveMove != move.Name || state.OwnerNode != "source" {
		t.Fatalf("healthy destination did not resume the same disrupted Move: move=%+v state=%+v", move.Status, state)
	}
	if moves, err := f.registry.ListMoves(f.ctx); err != nil || len(moves) != 1 {
		t.Fatalf("retry created another transaction: moves=%+v error=%v", moves, err)
	}
	owner, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
	if err != nil || !owner.Spec.Unschedulable {
		t.Fatalf("test or controller silently uncordoned source: node=%+v error=%v", owner, err)
	}
	pod, err := f.client.CoreV1().Pods(replacement.Namespace).Get(f.ctx, replacement.Name, metav1.GetOptions{})
	if err != nil || pod.UID != replacement.UID || !hasPlacementHold(pod) || pod.Spec.NodeSelector[corev1.LabelHostname] != "destination" {
		t.Fatalf("copy did not preserve the exact held replacement: pod=%+v error=%v", pod, err)
	}
}
