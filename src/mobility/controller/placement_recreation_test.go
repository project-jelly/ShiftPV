package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func TestFakeAssignsNewUIDAfterPodRecreation(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	assignJobUIDs(client)
	pods := client.CoreV1().Pods("system")
	first, err := pods.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "placement"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pods.Delete(ctx, first.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	second, err := pods.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: first.Name}}, metav1.CreateOptions{})
	if err != nil || first.UID == "" || second.UID == "" || first.UID == second.UID {
		t.Fatalf("recreation did not get a fresh identity: first=%v second=%v err=%v", first, second, err)
	}
}

func TestApprovedPlacementRecreationSurvivesExecutionPhases(t *testing.T) {
	for _, phase := range []fsm.Phase{fsm.PhaseCopying, fsm.PhasePromoting, fsm.PhaseCommitting} {
		t.Run(string(phase), func(t *testing.T) {
			r, repo, client := recoveryFixture(t, "source")
			ctx := context.Background()
			assignJobUIDs(client)
			move := repo.moves[0]
			move.Spec.Recovery = ""
			move.Status.Phase = string(phase)
			move.Status.CandidateNodes = []string{"destination"}
			move.Status.ConsumerUID = "evicted-consumer-uid"
			repo.moves[0] = move
			state := repo.volumes[move.Spec.VolumeID]
			state.Phase = volumeapi.PhaseMoving
			repo.volumes[move.Spec.VolumeID] = state
			replacement, err := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			replacement.Spec.NodeName = ""
			replacement.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
			if _, err := client.CoreV1().Pods(replacement.Namespace).Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			names := namesFor(move.Name)
			pods := client.CoreV1().Pods("system")
			old := r.placementPod(move, replacement, names)
			old.UID = "placement-uid"
			old.Spec.NodeName = "destination"
			if _, err := pods.Create(ctx, old, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := pods.Delete(ctx, old.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := r.ensurePlacement(ctx, &move, observation{Replacement: replacement}); err != nil {
				t.Fatal(err)
			}
			current, err := pods.Get(ctx, old.Name, metav1.GetOptions{})
			if err != nil || current.UID == old.UID || current.UID == "" {
				t.Fatalf("placement was not recreated: pod=%v err=%v", current, err)
			}
			current.Spec.NodeName = "destination"
			if _, err := pods.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			// Use the real observation and decision path, not a handcrafted verdict.
			if err := r.reconcilePersistedMove(ctx, move); err != nil {
				t.Fatal(err)
			}
			if got := repo.moves[0]; got.Status.Phase == string(fsm.PhaseBlocked) || got.Status.Reason == "PlacementReservationConflict" {
				t.Fatalf("same-Move recreation blocked execution: %+v", got.Status)
			}
			if phase == fsm.PhaseCommitting && repo.volumes[move.Spec.VolumeID].OwnerNode != "destination" {
				t.Fatalf("fresh reservation did not permit owner commit: %+v", repo.volumes[move.Spec.VolumeID])
			}
		})
	}
}

func TestApprovedRecoveryQuiescesRecreatedPlacement(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	ctx := context.Background()
	move := repo.moves[0]
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "source", recoveryQuiescing
	repo.moves[0] = move
	pod := r.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
	pod.UID = "new-placement-uid"
	pod.Spec.NodeName = "destination"
	if _, err := client.CoreV1().Pods("system").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcilePersistedMove(ctx, move); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("system").Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("recovery did not remove the recreated same-Move reservation: %v", err)
	}
	if repo.volumes[move.Spec.VolumeID].OwnerNode != "source" || !repo.moves[0].Status.CapacityApproved {
		t.Fatal("quiescing changed owner or released capacity before cleanup evidence")
	}
}

func TestCancellationPreservesForeignPlacement(t *testing.T) {
	for _, change := range []string{"Move UID", "role"} {
		t.Run(change, func(t *testing.T) {
			f := newCancellationFixture(t)
			move := f.move(t, 0)
			pods := f.client.CoreV1().Pods(f.reconciler.Namespace)
			pod, err := pods.Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if change == "Move UID" {
				pod.OwnerReferences[0].UID = "foreign-move"
			} else {
				pod.Labels["shiftpv.io/role"] = "copy"
			}
			if _, err := pods.Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err == nil {
				t.Fatal("cancellation accepted foreign placement ownership or role")
			}
			for _, action := range f.client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("cancellation deleted a foreign placement")
				}
			}
			if f.move(t, 0).Status.Preparation == nil || f.state(t, 0).ActiveMove != move.Name {
				t.Fatal("foreign placement conflict released the reservation")
			}
		})
	}
}

func TestPlacementUIDPolicyPreservesPreparationAndForeignOwnership(t *testing.T) {
	for _, approved := range []bool{false, true} {
		move := volumeapi.Move{Name: "move-test", UID: "move-uid", Status: volumeapi.MoveStatus{PlacementPodUID: "old", CapacityApproved: approved}}
		r := &Reconciler{Namespace: "system"}
		pod := r.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
		pod.UID = "new"
		if err := validatePlacementIdentity(pod, move, namesFor(move.Name)); (err == nil) != approved {
			t.Fatalf("approved=%t recreation verdict: %v", approved, err)
		}
		pod.OwnerReferences[0].UID = "foreign-move"
		if err := validatePlacementIdentity(pod, move, namesFor(move.Name)); err == nil {
			t.Fatalf("approved=%t accepted foreign Move ownership", approved)
		}
	}
}
