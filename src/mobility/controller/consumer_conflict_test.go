package controller

import (
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func TestApprovedMultipleConsumersRetainExecutionObservationAndWait(t *testing.T) {
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
		Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
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
