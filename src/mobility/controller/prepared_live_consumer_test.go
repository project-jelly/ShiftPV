package controller

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestPreparedDisruptionWaitsForLiveOriginalWithOneHeldConsumer(t *testing.T) {
	for _, rolling := range []bool{false, true} {
		t.Run(map[bool]string{false: "ReplicaSet", true: "Deployment rolling update"}[rolling], func(t *testing.T) {
			f, move, held := preparedDisruptionConsumerFixture(t, rolling)
			before := f.state(t, 0)
			f.client.ClearActions()
			for range 3 {
				if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			waiting := f.move(t, 0)
			if waiting.UID != move.UID || waiting.Status.Phase != "StartingDisruption" || waiting.Status.Reason != "MultipleConsumers" ||
				waiting.Status.EvictionRequested || waiting.Status.CapacityApproved || !reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) {
				t.Fatalf("live original and one held consumer did not defer the same disruption: before=%+v after=%+v", move.Status, waiting.Status)
			}
			if !reflect.DeepEqual(before, f.state(t, 0)) {
				t.Fatalf("waiting changed source authority: before=%+v after=%+v", before, f.state(t, 0))
			}
			assertNoEviction(t, f.client)
			assertPreparedDisruptionHolds(t, f, move, held)
			singleLifecycleNoDiskWork(t, f)

			// Durable disruption intent must survive a restart and resume this Move
			// once the additional consumer is gone.
			restartDisruptedReconciler(f)
			if err := f.client.CoreV1().Pods(held.Namespace).Delete(f.ctx, held.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			f.client.ClearActions()
			if err := f.reconciler.ReconcileAll(f.ctx); err != nil {
				t.Fatal(err)
			}
			assertPreparedDisruptionContinues(t, f, move)
			evictions := 0
			for _, action := range f.client.Actions() {
				if action.GetSubresource() == "eviction" {
					evictions++
				}
			}
			if evictions != 1 {
				t.Fatalf("removing the extra consumer did not submit exactly one original eviction: %d", evictions)
			}
			replacement := singleLifecycleReplacement(t, f)
			unpublishDisruptedSource(t, f)
			assertDisruptedMoveResumes(t, f, move, replacement)
		})
	}
}

func TestPreparedDisruptionContinuesWithOneHeldConsumerAfterOriginalDisrupted(t *testing.T) {
	for _, original := range []string{"absent", "terminating"} {
		t.Run(original, func(t *testing.T) {
			f, move, held := preparedDisruptionConsumerFixture(t, true)
			w := f.workloads[0]
			if original == "absent" {
				if err := f.client.CoreV1().Pods(w.Consumer.Namespace).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				pod, err := f.client.CoreV1().Pods(w.Consumer.Namespace).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				if _, err = f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			assertPreparedDisruptionContinues(t, f, move)
			assertNoEviction(t, f.client)
			assertPreparedDisruptionHolds(t, f, move, held)
			unpublishDisruptedSource(t, f)
			assertDisruptedMoveResumes(t, f, move, held)
		})
	}
}

func TestPreparedDisruptionBlocksTemplateWithMultiplePVCsBeforeEviction(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	move, before := f.move(t, 0), f.state(t, 0)
	if move.Status.Phase != "StartingDisruption" || move.Status.EvictionRequested {
		t.Fatalf("fixture did not reach durable disruption intent: %+v", move.Status)
	}
	w := f.workloads[0]
	rs, err := f.client.AppsV1().ReplicaSets(w.Namespace.Name).Get(f.ctx, w.ReplicaSet.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second := claimVolumes()[0]
	second.Name, second.PersistentVolumeClaim.ClaimName = "second", "second-claim"
	rs.Spec.Template.Spec.Volumes = append(rs.Spec.Template.Spec.Volumes, second)
	if _, err = f.client.AppsV1().ReplicaSets(rs.Namespace).Update(f.ctx, rs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()
	for range 3 {
		if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	waiting, state := f.move(t, 0), f.state(t, 0)
	if waiting.UID != move.UID || waiting.Status.Phase != "Blocked" || waiting.Status.Reason != "MultiplePVCsUnsupported" ||
		waiting.Status.EvictionRequested || waiting.Status.CapacityApproved || !reflect.DeepEqual(waiting.Status.Preparation, move.Status.Preparation) {
		t.Fatalf("unsupported replacement template did not block before disruption: move=%+v source=%+v", waiting.Status, state)
	}
	before.Phase = volumeapi.PhaseBlocked
	if !reflect.DeepEqual(before, state) {
		t.Fatalf("template-only change modified the live source authority: before=%+v after=%+v", before, state)
	}
	original, err := f.client.CoreV1().Pods(w.Consumer.Namespace).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
	if err != nil || original.DeletionTimestamp != nil || len(pvcNames(original.Spec)) != 1 {
		t.Fatalf("template-only change disrupted or modified the one-PVC original: pod=%+v error=%v", original, err)
	}
	assertNoEviction(t, f.client)
	assertPreparationReserved(t, f, preparationRequestedBytes)
	placement, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil || string(placement.UID) != move.Status.PlacementPodUID {
		t.Fatalf("template wait replaced the placement reservation: pod=%+v error=%v", placement, err)
	}
	singleLifecycleNoDiskWork(t, f)
}

// Exercise the production Registry and Reconciler. Both controllers desire one
// replica; a Deployment may nevertheless create a surge Pod in a new ReplicaSet.
// The extra consumer is created only after StartingDisruption is persisted so
// the ordinary preflight replica/consumer checks cannot mask this boundary.
func preparedDisruptionConsumerFixture(t *testing.T, rolling bool) (*preparationRegistryFixture, volumeapi.Move, *corev1.Pod) {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	w := f.workloads[0]
	rs, err := f.client.AppsV1().ReplicaSets(w.Namespace.Name).Get(f.ctx, w.ReplicaSet.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	rs.Spec.Replicas = &one
	if rolling {
		oldLabels := map[string]string{"app": "rolling", "pod-template-hash": "old"}
		rs.Labels = oldLabels
		rs.Spec.Selector = &metav1.LabelSelector{MatchLabels: oldLabels}
		rs.Spec.Template.Labels = oldLabels
		original := w.Consumer.DeepCopy()
		original.Labels = oldLabels
		if _, err = f.client.CoreV1().Pods(original.Namespace).Update(f.ctx, original, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		surge, zero := intstr.FromInt32(1), intstr.FromInt32(0)
		labels := map[string]string{"app": "rolling"}
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "rolling", Namespace: rs.Namespace, UID: "rolling-uid"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: *rs.Spec.Template.Spec.DeepCopy()},
				Strategy: appsv1.DeploymentStrategy{
					Type:          appsv1.RollingUpdateDeploymentStrategyType,
					RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &surge, MaxUnavailable: &zero},
				},
			},
		}
		if _, err = f.client.AppsV1().Deployments(rs.Namespace).Create(f.ctx, deployment, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		rs.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(deployment, appsv1.SchemeGroupVersion.WithKind("Deployment"))}
	}
	if _, err = f.client.AppsV1().ReplicaSets(rs.Namespace).Update(f.ctx, rs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	move := f.untilPrepared(t, 0)
	if err = f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	if move.Status.Phase != "StartingDisruption" || move.Status.EvictionRequested {
		t.Fatalf("fixture did not reach durable disruption intent: %+v", move.Status)
	}
	if rolling {
		deployment, getErr := f.client.AppsV1().Deployments(rs.Namespace).Get(f.ctx, "rolling", metav1.GetOptions{})
		if getErr != nil {
			t.Fatal(getErr)
		}
		deployment.Spec.Template.Spec.Containers[0].Image = "test-new-revision"
		if _, err = f.client.AppsV1().Deployments(rs.Namespace).Update(f.ctx, deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		rs = rs.DeepCopy()
		rs.Name, rs.UID, rs.ResourceVersion = "rolling-new-rs", "rolling-new-rs-uid", ""
		newLabels := map[string]string{"app": "rolling", "pod-template-hash": "new"}
		rs.Labels = newLabels
		rs.Spec.Selector = &metav1.LabelSelector{MatchLabels: newLabels}
		rs.Spec.Template = *deployment.Spec.Template.DeepCopy()
		rs.Spec.Template.Labels = newLabels
		if rs, err = f.client.AppsV1().ReplicaSets(rs.Namespace).Create(f.ctx, rs, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	held := w.Consumer.DeepCopy()
	held.Labels = rs.Spec.Template.Labels
	held.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
	held.Name, held.UID, held.ResourceVersion = "one-held-extra", "", ""
	held.Spec.NodeName = ""
	held.Spec.Containers = append([]corev1.Container(nil), rs.Spec.Template.Spec.Containers...)
	held.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	held.Status = corev1.PodStatus{Phase: corev1.PodPending}
	held, err = f.client.CoreV1().Pods(held.Namespace).Create(f.ctx, held, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return f, move, held
}

func assertPreparedDisruptionHolds(t *testing.T, f *preparationRegistryFixture, move volumeapi.Move, held *corev1.Pod) {
	t.Helper()
	assertPreparationReserved(t, f, preparationRequestedBytes)
	placement, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil || string(placement.UID) != move.Status.PlacementPodUID {
		t.Fatalf("disruption changed the placement reservation identity: pod=%+v error=%v", placement, err)
	}
	pod, err := f.client.CoreV1().Pods(held.Namespace).Get(f.ctx, held.Name, metav1.GetOptions{})
	if err != nil || pod.UID != held.UID || !hasPlacementHold(pod) || pod.Spec.NodeName != "" {
		t.Fatalf("disruption released or replaced the held consumer: pod=%+v error=%v", pod, err)
	}
}

func assertPreparedDisruptionContinues(t *testing.T, f *preparationRegistryFixture, before volumeapi.Move) {
	t.Helper()
	move, state := f.move(t, 0), f.state(t, 0)
	if move.UID != before.UID || move.Status.Phase != "WaitingForUnpublish" || !move.Status.EvictionRequested || move.Status.CapacityApproved ||
		!reflect.DeepEqual(move.Status.Preparation, before.Status.Preparation) || state.Phase != volumeapi.PhaseMoving || state.ActiveMove != before.Name {
		t.Fatalf("disrupted original did not continue the same prepared Move: move=%+v state=%+v", move.Status, state)
	}
	if moves, err := f.registry.ListMoves(f.ctx); err != nil || len(moves) != 1 {
		t.Fatalf("disruption created another transaction: moves=%+v error=%v", moves, err)
	}
}
