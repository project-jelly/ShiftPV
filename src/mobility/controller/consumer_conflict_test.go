package controller

import (
	"context"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func approvedConsumerConflictFixture(t *testing.T, outcome batchv1.JobConditionType) (*preparationRegistryFixture, volumeapi.Move, *corev1.Pod) {
	t.Helper()
	f, move := preparationAtCapacity(t)
	if err := f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	next := move.Status
	next.Phase = string(fsm.PhaseCopying)
	var err error
	move, err = f.registry.CompareAndSetMoveStatus(f.ctx, move, next)
	if err != nil {
		t.Fatal(err)
	}
	first := singleLifecycleReplacement(t, f)
	second := first.DeepCopy()
	second.Name, second.UID, second.ResourceVersion = "second-held-replacement", "", ""
	second, err = f.client.CoreV1().Pods(second.Namespace).Create(f.ctx, second, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	names := namesFor(move.Name)
	if _, err := f.client.BatchV1().Jobs("system").Create(f.ctx, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: names.CopyJob},
		Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: outcome, Status: corev1.ConditionTrue}}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return f, move, second
}

func TestApprovedMultipleConsumersRetainExecutionObservationAndWait(t *testing.T) {
	f, move, second := approvedConsumerConflictFixture(t, batchv1.JobComplete)
	state := f.state(t, 0)
	for pass := 0; pass < 3; pass++ {
		observed, err := f.reconciler.observeLifecycle(f.ctx, move)
		if err != nil {
			t.Fatal(err)
		}
		if !observed.FSM.CopyComplete || !observed.FSM.PlacementExists || observed.Replacement != nil {
			t.Fatalf("ambiguous consumers discarded execution facts or chose a replacement: facts=%+v replacement=%v", observed.FSM, observed.Replacement != nil)
		}
		lifecycleFacts(move, &observed)
		decision, err := fsm.Decide(lifecycleState(move), observed.FSM)
		if err != nil || decision.Action != fsm.ActionWait || decision.Reason != "MultipleConsumers" {
			t.Fatalf("ambiguous execution did not explicitly wait: decision=%+v error=%v", decision, err)
		}
		if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
			t.Fatal(err)
		}
		move = f.move(t, 0)
		if move.Status.Reason != "MultipleConsumers" || !reflect.DeepEqual(state, f.state(t, 0)) {
			t.Fatal("waiting lost diagnosis or changed serving authority")
		}
		assertPreparationReserved(t, f, preparationRequestedBytes)
	}
	if err := f.client.CoreV1().Pods(second.Namespace).Delete(f.ctx, second.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	observed, err := f.reconciler.observeLifecycle(f.ctx, move)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleFacts(move, &observed)
	decision, err := fsm.Decide(lifecycleState(move), observed.FSM)
	if err != nil || decision.Action != fsm.ActionEnsurePromotion || decision.Reason != "" || decision.Next.Phase != fsm.PhasePromoting {
		t.Fatalf("single consumer did not resume the completed copy: decision=%+v error=%v observed=%+v", decision, err, observed.FSM)
	}
}

func TestApprovedConsumerConflictDoesNotHideCopyFailure(t *testing.T) {
	f, move, _ := approvedConsumerConflictFixture(t, batchv1.JobFailed)
	observed, err := f.reconciler.observeLifecycle(f.ctx, move)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleFacts(move, &observed)
	decision, err := fsm.Decide(lifecycleState(move), observed.FSM)
	if err != nil || decision.Action != fsm.ActionMarkBlocked || decision.Reason != "CopyFailed" {
		t.Fatalf("consumer conflict hid failed copy evidence: decision=%+v error=%v observation=%+v", decision, err, observed.FSM)
	}
}

func TestCommittedMultipleConsumersRequirePublicationProofForCleanup(t *testing.T) {
	r, repo, client, first := replacementReleaseFixture(t, true)
	ctx := context.Background()
	move := repo.moves[0]
	move.Status.Phase = string(fsm.PhaseWaitingForDestinationPublish)
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	state.PublishedNodes = []string{move.Status.DestinationNode}
	repo.volumes[move.Spec.VolumeID] = state
	second := first.DeepCopy()
	second.Name, second.UID, second.ResourceVersion = "second-committed-consumer", "", ""
	second.Spec.NodeName, second.Spec.SchedulingGates = move.Status.DestinationNode, nil
	second.Status = corev1.PodStatus{Phase: corev1.PodRunning}
	if _, err := client.CoreV1().Pods(second.Namespace).Create(ctx, second, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, published := range []bool{false, true} {
		for i := range repo.pools {
			if repo.pools[i].UID == state.CurrentCopy.PoolUID {
				repo.pools[i].Status.Inventory = &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.Now(), Copies: []volumeapi.CopyObservation{
					{Identity: state.CurrentCopy, Present: true, Published: published},
				}}
			}
		}
		observed, err := r.observeLifecycle(ctx, move)
		if err != nil {
			t.Fatal(err)
		}
		lifecycleFacts(move, &observed)
		if !observed.FSM.OwnerCommitted || !observed.FSM.ConsumersAmbiguous || observed.FSM.PublishedOnDestination != published || observed.Replacement != nil {
			t.Fatalf("committed observation lost authority/publication/ambiguity: %+v", observed.FSM)
		}
		decision, err := fsm.Decide(lifecycleState(move), observed.FSM)
		if !published {
			if err != nil || decision.Action != fsm.ActionWait || decision.Reason != "MultipleConsumers" {
				t.Fatalf("API publication alone bypassed physical proof: decision=%+v error=%v", decision, err)
			}
		} else if err != nil || decision.Action != fsm.ActionEnsureCleanup || decision.Next.Phase != fsm.PhaseCleaningSource {
			t.Fatalf("consumer count blocked committed cleanup: decision=%+v error=%v", decision, err)
		}
	}
}

func TestCommittedOwnerWithLaggingPhaseDoesNotNeedReplacementOrJob(t *testing.T) {
	r, repo, client, first := replacementReleaseFixture(t, true)
	ctx := context.Background()
	move := repo.moves[0]
	move.Status.Phase = string(fsm.PhasePromoting)
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	second := first.DeepCopy()
	second.Name, second.UID, second.ResourceVersion = "second-held-consumer", "", ""
	if _, err := client.CoreV1().Pods(second.Namespace).Create(ctx, second, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcilePersistedMove(ctx, move); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.Phase != string(fsm.PhaseCommitting) || !reflect.DeepEqual(state, repo.volumes[move.Spec.VolumeID]) {
		t.Fatal("lagging Move phase did not follow the exact committed owner without changing authority")
	}
	jobs, err := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("committed owner restarted a promotion Job: jobs=%+v error=%v", jobs, err)
	}
}
