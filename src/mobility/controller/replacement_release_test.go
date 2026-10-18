package controller

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

func replacementReleaseFixture(t *testing.T, newName bool) (*Reconciler, *memoryRepository, *fake.Clientset, *corev1.Pod) {
	t.Helper()
	r, repo, client := recoveryFixture(t, "destination")
	ctx := context.Background()
	assignJobUIDs(client)
	move := repo.moves[0]
	move.Spec.Recovery = ""
	move.Status.Phase, move.Status.Reason = "ReleasingDestination", ""
	move.Status.CandidateNodes = []string{"destination"}
	move.Status.ReplacementName, move.Status.ReplacementUID = "consumer", "previous-replacement-uid"
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	state.Phase = volumeapi.PhaseReady
	repo.volumes[move.Spec.VolumeID] = state
	pods := client.CoreV1().Pods("workload")
	pod, err := pods.Get(ctx, "consumer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pods.Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.UID, pod.ResourceVersion = "", ""
	if newName {
		pod.Name = "new-consumer"
	}
	pod.Labels = map[string]string{admission.ManagedLabel: "true"}
	pod.Annotations = map[string]string{placementAnnotationKey: "held"}
	pod.Spec.NodeName, pod.Spec.NodeSelector = "", map[string]string{"disk": "fast"}
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}, {Name: "user.example/hold"}}
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	pod, err = pods.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return r, repo, client, pod
}

func TestRecreatedReplacementProgressesAfterOwnerCommit(t *testing.T) {
	for _, phase := range []string{"ReleasingDestination", "WaitingForDestinationPublish", "CleaningSource"} {
		for _, newName := range []bool{false, true} {
			t.Run(phase+"/"+map[bool]string{false: "same name new UID", true: "new name new UID"}[newName], func(t *testing.T) {
				r, repo, client, pod := replacementReleaseFixture(t, newName)
				repo.moves[0].Status.Phase = phase
				ctx := context.Background()
				before := repo.volumes[repo.moves[0].Spec.VolumeID]
				client.ClearActions()
				if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
					t.Fatal(err)
				}
				live, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if live.UID != pod.UID || hasPlacementHold(live) || live.Spec.NodeSelector[corev1.LabelHostname] != "destination" || live.Spec.NodeSelector["disk"] != "fast" {
					t.Fatalf("current replacement was not safely released: %+v", live)
				}
				if len(live.Spec.SchedulingGates) != 1 || live.Spec.SchedulingGates[0].Name != "user.example/hold" || live.Annotations[placementAnnotationKey] != "owner" {
					t.Fatalf("release changed user gates or failed to record owner placement: %+v", live)
				}
				move := repo.moves[0]
				want := phase
				if phase == "ReleasingDestination" {
					want = "WaitingForDestinationPublish"
				}
				if move.Status.Phase != want || move.Status.ReplacementName != pod.Name || move.Status.ReplacementUID != string(pod.UID) {
					t.Fatalf("replacement recreation did not advance the Move: %+v", move.Status)
				}
				if !reflect.DeepEqual(before, repo.volumes[move.Spec.VolumeID]) || !move.Status.CapacityApproved {
					t.Fatal("releasing the workload changed copy authority or settled capacity before publication")
				}
				updates := 0
				for _, action := range client.Actions() {
					if action.GetVerb() == "update" && action.GetResource().Resource == "pods" {
						updates++
					}
				}
				if updates != 1 {
					t.Fatalf("pin and release were not one Pod update: %d", updates)
				}
			})
		}
	}
}

func TestReplacementReleaseRechecksAfterConflict(t *testing.T) {
	for _, changed := range []string{"binding", "Pod", "owner"} {
		t.Run(changed, func(t *testing.T) {
			r, repo, client, pod := replacementReleaseFixture(t, false)
			ctx := context.Background()
			injected := false
			client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if injected {
					return false, nil, nil
				}
				injected = true
				switch changed {
				case "binding":
					claim := newMobilityFixture(repo.moves[0].Spec.VolumeID).Claim
					claim.UID = "new-claim-uid"
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), claim, claim.Namespace); err != nil {
						t.Fatal(err)
					}
				case "Pod":
					recreated := pod.DeepCopy()
					recreated.UID = "recreated-between-attempts"
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), recreated, recreated.Namespace); err != nil {
						t.Fatal(err)
					}
				case "owner":
					state := repo.volumes[repo.moves[0].Spec.VolumeID]
					state.OwnerNode = "source"
					repo.volumes[repo.moves[0].Spec.VolumeID] = state
				}
				return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, errors.New("injected conflict"))
			})
			move := repo.moves[0]
			if err := r.releasePlacement(ctx, &move, observation{Replacement: pod}); err == nil || !injected {
				t.Fatalf("changed %s was accepted after conflict: %v", changed, err)
			}
			live, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(live.Spec, pod.Spec) {
				t.Fatalf("retry changed the held Pod: %+v err=%v", live, err)
			}
		})
	}
}

func TestRecreatedReplacementReleaseRejectsChangedPrerequisites(t *testing.T) {
	for _, scenario := range []string{"observed UID", "unmanaged", "bare Pod", "claim name", "multiple claims", "scheduled", "selector", "binding UID", "owner", "terminating"} {
		t.Run(scenario, func(t *testing.T) {
			r, repo, client, pod := replacementReleaseFixture(t, false)
			ctx := context.Background()
			observed := pod.DeepCopy()
			switch scenario {
			case "observed UID":
				observed.UID = "stale-observation-uid"
			case "unmanaged":
				pod.Labels = nil
			case "bare Pod":
				pod.OwnerReferences = nil
			case "claim name":
				pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other-claim"
			case "multiple claims":
				pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "extra", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "extra"}}})
			case "scheduled":
				pod.Spec.NodeName = "source"
			case "selector":
				pod.Spec.NodeSelector[corev1.LabelHostname] = "source"
			case "binding UID":
				claim, err := client.CoreV1().PersistentVolumeClaims("workload").Get(ctx, "claim", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				claim.UID = "new-claim-uid"
				if _, err := client.CoreV1().PersistentVolumeClaims("workload").Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "owner":
				state := repo.volumes[repo.moves[0].Spec.VolumeID]
				state.OwnerNode = "source"
				repo.volumes[repo.moves[0].Spec.VolumeID] = state
			case "terminating":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}
			if _, err := client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			move := repo.moves[0]
			if err := r.releasePlacement(ctx, &move, observation{Replacement: observed}); err == nil {
				t.Fatal("release accepted changed prerequisites")
			}
			live, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(live.Spec, pod.Spec) {
				t.Fatalf("rejected release changed the Pod: %+v err=%v", live, err)
			}
		})
	}
}

func TestReplacementReleaseReadBackAfterLostUpdateResponse(t *testing.T) {
	r, repo, client, pod := replacementReleaseFixture(t, true)
	ctx := context.Background()
	lost, writes := false, 0
	client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		writes++
		if lost {
			return false, nil, nil
		}
		lost = true
		object := action.(ktesting.UpdateAction).GetObject().(*corev1.Pod)
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), object, object.Namespace); err != nil {
			t.Fatal(err)
		}
		return true, nil, io.ErrUnexpectedEOF
	})
	if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err == nil || !lost {
		t.Fatalf("did not inject a stored Pod update with lost response: %v", err)
	}
	restarted := newTestReconciler(client, repo, withCleanupsFrom(r))
	if err := restarted.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
		t.Fatal(err)
	}
	live, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || hasPlacementHold(live) || live.Spec.NodeSelector[corev1.LabelHostname] != "destination" || writes != 1 || repo.moves[0].Status.Phase != "WaitingForDestinationPublish" {
		t.Fatalf("stored release was not recovered by observation: pod=%+v writes=%d move=%+v err=%v", live, writes, repo.moves[0].Status, err)
	}
}
