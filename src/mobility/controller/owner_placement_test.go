package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

func TestOwnerPlacementRetainsGateWithoutAuthority(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*volumeapi.State, *corev1.Pod, *corev1.Node)
		wantErr bool
	}{
		{name: "cordoned owner", mutate: func(_ *volumeapi.State, _ *corev1.Pod, node *corev1.Node) { node.Spec.Unschedulable = true }},
		{name: "unready owner", mutate: func(_ *volumeapi.State, _ *corev1.Pod, node *corev1.Node) { node.Status.Conditions = nil }},
		{name: "deleting owner", mutate: func(_ *volumeapi.State, _ *corev1.Pod, node *corev1.Node) {
			now := metav1.Now()
			node.DeletionTimestamp = &now
		}},
		{name: "active Move with Ready volume", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.ActiveMove = "move-test" }},
		{name: "moving volume", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.Phase = volumeapi.PhaseMoving }},
		{name: "blocked volume", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.Phase = volumeapi.PhaseBlocked }},
		{name: "deleting volume", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.Deleting = true }},
		{name: "cleanup intent", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.HasCleanup = true }},
		{name: "missing copy", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.CurrentCopy = nil }, wantErr: true},
		{name: "foreign copy UID", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.CurrentCopy.VolumeUID = "foreign" }, wantErr: true},
		{name: "incoming copy", mutate: func(state *volumeapi.State, _ *corev1.Pod, _ *corev1.Node) { state.CurrentCopy.Role = "Incoming" }, wantErr: true},
		{name: "conflicting hostname", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) {
			pod.Spec.NodeSelector[corev1.LabelHostname] = "destination"
		}, wantErr: true},
		{name: "existing empty hostname", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) {
			pod.Spec.NodeSelector[corev1.LabelHostname] = ""
		}, wantErr: true},
		{name: "already scheduled", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) { pod.Spec.NodeName = "source" }},
		{name: "deleting Pod", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}},
		{name: "terminal Pod", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) { pod.Status.Phase = corev1.PodFailed }},
		{name: "unmanaged Pod", mutate: func(_ *volumeapi.State, pod *corev1.Pod, _ *corev1.Node) { pod.Labels = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, client, repository, pod := ownerPlacementFixture()
			state := repository.volumes[ownerPlacementVolumeID]
			node, err := client.CoreV1().Nodes().Get(context.Background(), "source", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&state, pod, node)
			repository.volumes[ownerPlacementVolumeID] = state
			if _, err := client.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			err = r.reconcileOwnerPlacements(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("reconcile error = %v, wantErr %v", err, test.wantErr)
			}
			got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, pod) {
				t.Fatalf("held Pod changed without authority: %#v", got)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "update" {
					t.Fatalf("unauthorized Pod update: %#v", action)
				}
			}
		})
	}
}

func TestOwnerPlacementWaitsForMoveSettlement(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   volumeapi.MoveStatus
		released bool
	}{
		{name: "pending", status: volumeapi.MoveStatus{Phase: "Pending"}},
		{name: "locking", status: volumeapi.MoveStatus{Phase: "Locking"}},
		{name: "blocked", status: volumeapi.MoveStatus{Phase: "Blocked"}},
		{name: "terminal cleanup pending", status: volumeapi.MoveStatus{Phase: "Succeeded", CleanupPhase: "Requested"}},
		{name: "settled history", status: volumeapi.MoveStatus{Phase: "Succeeded", CleanupPhase: "Completed"}, released: true},
		{name: "recovery holds remain", status: volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityApproved: true, CapacityReason: "RecoverySettled"}},
		{name: "settled recovery history", status: volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityReason: "RecoverySettled"}, released: true},
		{name: "recovered with working journal", status: volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityReason: "RecoverySettled", CleanupPhase: "Verifying"}},
		{name: "recovered with review journal", status: volumeapi.MoveStatus{Phase: "Blocked", RecoveryPhase: "Recovered", CapacityReason: "RecoverySettled", CleanupPhase: "NeedsReview"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, client, repository, pod := ownerPlacementFixture()
			repository.moves = []volumeapi.Move{{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: ownerPlacementVolumeID, SourceNode: "source"}, Status: test.status}}
			if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if hasPlacementHold(got) == test.released {
				t.Fatalf("gate held = %v, want released %v", hasPlacementHold(got), test.released)
			}
		})
	}
}

func TestOwnerPlacementRejectsRecreatedPod(t *testing.T) {
	r, client, _, pod := ownerPlacementFixture()
	replacement := pod.DeepCopy()
	replacement.UID = "new-pod-uid"
	client.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, replacement.DeepCopy(), nil
	})
	if err := r.reconcileOwnerPlacements(context.Background()); err == nil {
		t.Fatal("recreated Pod was accepted")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			t.Fatal("updated recreated Pod")
		}
	}
}

func TestOwnerPlacementAllowsOtherUnboundClaimToReachScheduler(t *testing.T) {
	r, client, _, pod := ownerPlacementFixture()
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "waiting", Namespace: pod.Namespace, UID: "waiting-uid"}}
	if _, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(context.Background(), claim, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "waiting", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name},
	}})
	if _, err := client.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "source" {
		t.Fatal("unbound claim cannot reach the scheduler after owner recovery")
	}
}

func TestOwnerPlacementRecoversUnsupportedWorkloadAfterUncordon(t *testing.T) {
	r, client, _, pod := ownerPlacementFixture()
	pod.OwnerReferences = nil
	pod.Spec.SchedulerName = "custom-scheduler"
	if _, err := client.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "source" || got.Spec.SchedulerName != pod.Spec.SchedulerName {
		t.Fatal("workload mobility restrictions prevented ordinary owner placement")
	}
}

type ownerPlacementMoveErrorRepository struct {
	Repository
	err error
}

func (r *ownerPlacementMoveErrorRepository) ListMoves(ctx context.Context) ([]volumeapi.Move, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Repository.ListMoves(ctx)
}

func TestOwnerPlacementRetriesMoveObservationFailure(t *testing.T) {
	r, client, repository, pod := ownerPlacementFixture()
	wrapped := &ownerPlacementMoveErrorRepository{Repository: repository, err: errors.New("temporary API failure")}
	r.Repository = wrapped
	if err := r.reconcileOwnerPlacements(context.Background()); err == nil {
		t.Fatal("Move observation failure was hidden")
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasPlacementHold(got) {
		t.Fatal("gate released after failed Move observation")
	}
	wrapped.err = nil
	if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err = client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasPlacementHold(got) {
		t.Fatal("gate did not recover after API recovery")
	}
}

func TestOwnerPlacementRecoversHeldPodWithoutMove(t *testing.T) {
	r, client, repository, pod := ownerPlacementFixture()
	wantState := repository.volumes[ownerPlacementVolumeID]
	wantAffinity := pod.Spec.Affinity.DeepCopy()
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasPlacementHold(got) || got.Spec.NodeSelector[corev1.LabelHostname] != "source" || got.UID != pod.UID {
		t.Fatalf("held Pod did not resume on its owner: %#v", got)
	}
	if got.Annotations[placementAnnotationKey] != "owner" || got.Spec.NodeSelector["disk"] != "fast" ||
		!reflect.DeepEqual(got.Spec.Affinity, wantAffinity) || len(got.Spec.SchedulingGates) != 1 || got.Spec.SchedulingGates[0].Name != "example.test/hold" {
		t.Fatalf("owner placement changed unrelated constraints: %#v", got)
	}
	if len(repository.moves) != 0 || !reflect.DeepEqual(repository.volumes[ownerPlacementVolumeID], wantState) {
		t.Fatal("placement recovery changed storage authority or created a Move")
	}
	client.ClearActions()
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "create" || action.GetVerb() == "delete" {
			t.Fatalf("settled recovery was not idempotent: %#v", action)
		}
	}
}

const ownerPlacementVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"

func ownerPlacementFixture() (*Reconciler, *fake.Clientset, *memoryRepository, *corev1.Pod) {
	fixture := newMobilityFixture(ownerPlacementVolumeID)
	fixture.SourceNode.Spec.Unschedulable = false
	fixture.PV.UID = "pv-uid"
	fixture.Consumer.Spec.NodeName = ""
	fixture.Consumer.Labels = map[string]string{admission.ManagedLabel: "true"}
	fixture.Consumer.Annotations = map[string]string{placementAnnotationKey: "held"}
	fixture.Consumer.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}, {Name: "example.test/hold"}}
	fixture.Consumer.Spec.NodeSelector = map[string]string{"disk": "fast"}
	fixture.Consumer.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1, Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}},
		}}},
	}}
	copy, _, _ := testCopyIdentities(ownerPlacementVolumeID, "source", "destination")
	repository := &memoryRepository{volumes: map[string]volumeapi.State{ownerPlacementVolumeID: {
		UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: "source", CurrentCopy: &copy,
	}}}
	client := fake.NewSimpleClientset(fixture.Objects()...)
	return newTestReconciler(client, repository), client, repository, fixture.Consumer.DeepCopy()
}
