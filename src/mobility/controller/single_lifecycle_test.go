package controller

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestSingleLifecycleApprovalNeedsFreshObservationBeforeCopy(t *testing.T) {
	f, move := preparationAtCapacity(t)
	replacement := singleLifecycleReplacement(t, f)
	before := f.state(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	approved := f.move(t, 0)
	if approved.Status.Phase != "WaitingForCapacity" || !approved.Status.CapacityApproved || approved.Status.Preparation != nil ||
		approved.Status.IncomingCopy == nil || approved.Status.DestinationCopy == nil {
		t.Fatalf("approval effect chose another phase or lost its exact disk plan: %+v", approved.Status)
	}
	if !reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatal("destination approval changed source authority or publication")
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)

	// Only the next stored observation sees approval and chooses Copying. The
	// source server is still unready, so this also proves phase choice does not
	// depend on the effect having finished all of its helper acquisition.
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	copying := f.move(t, 0)
	if copying.Status.Phase != "Copying" || copying.UID != move.UID ||
		!reflect.DeepEqual(copying.Status.IncomingCopy, approved.Status.IncomingCopy) ||
		!reflect.DeepEqual(copying.Status.DestinationCopy, approved.Status.DestinationCopy) {
		t.Fatalf("fresh approval did not resume the same transaction: %+v", copying)
	}
	server, err := f.client.CoreV1().Pods(f.reconciler.Namespace).Get(f.ctx, namesFor(move.Name).SourcePod, metav1.GetOptions{})
	if err != nil || !ownedByMove(server, move) {
		t.Fatalf("copy phase did not acquire its exact source server: %+v error=%v", server, err)
	}
	held, err := f.client.CoreV1().Pods(replacement.Namespace).Get(f.ctx, replacement.Name, metav1.GetOptions{})
	if err != nil || held.UID != replacement.UID || !hasPlacementHold(held) || held.Spec.NodeSelector[corev1.LabelHostname] != "destination" {
		t.Fatalf("copy did not preserve the exact held replacement: %+v error=%v", held, err)
	}
	if !reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatal("starting copy changed source authority")
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestSingleLifecycleWaitActionCanAdvancePersistedPhase(t *testing.T) {
	f, move := preparationAtCapacity(t)
	status := move.Status
	status.Phase = "WaitingForUnpublish"
	move, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
	if err != nil {
		t.Fatal(err)
	}
	before := f.state(t, 0)
	f.client.ClearActions()
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	stored := f.move(t, 0)
	if stored.Status.Phase != "WaitingForReplacement" || stored.Status.CapacityApproved ||
		!reflect.DeepEqual(stored.Status.Preparation, move.Status.Preparation) {
		t.Fatalf("no-effect decision was treated as a phase self-loop: %+v", stored.Status)
	}
	if !reflect.DeepEqual(before, f.state(t, 0)) {
		t.Fatal("observing source unpublication changed volume authority")
	}
	for _, action := range f.client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("Wait transition executed an effect: %#v", action)
		}
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	singleLifecycleNoDiskWork(t, f)
}

func TestSingleLifecycleSourceChangesBeforeApprovalPreserveServingCopy(t *testing.T) {
	for _, change := range []string{"uncordoned", "missing", "not ready"} {
		t.Run(change, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			singleLifecycleUntil(t, f, func(move volumeapi.Move, _ volumeapi.State) bool { return move.Status.Preparation != nil })
			move, source := f.move(t, 0), f.state(t, 0)
			if change == "missing" {
				if err := f.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("nodes"), "", "source"); err != nil {
					t.Fatal(err)
				}
			} else {
				node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if change == "uncordoned" {
					node.Spec.Unschedulable = false
				} else {
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				}
				if _, err = f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			canceling := f.move(t, 0)
			if canceling.Status.Phase != "Canceling" || canceling.Status.CapacityApproved ||
				!reflect.DeepEqual(canceling.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(source, f.state(t, 0)) {
				t.Fatalf("%s source change bypassed durable pre-disk cancellation: move=%+v source=%+v", change, canceling.Status, f.state(t, 0))
			}
			reason := canceling.Status.Reason
			if reason == "" {
				t.Fatal("source change did not record its cancellation reason")
			}
			assertPreparationReserved(t, f, preparationRequestedBytes)
			singleLifecycleUntil(t, f, func(move volumeapi.Move, state volumeapi.State) bool {
				return move.Status.Phase == "Cancelled" && state.ActiveMove == ""
			})
			source.ActiveMove = ""
			if !reflect.DeepEqual(source, f.state(t, 0)) {
				t.Fatal("pre-disk cancellation changed source copy or publication")
			}
			if got := f.move(t, 0).Status.Reason; got != reason {
				t.Fatalf("settlement erased the cancellation reason: got=%q want=%q", got, reason)
			}
			assertPreparationReserved(t, f, 0)
			assertNoEviction(t, f.client)
			singleLifecycleNoDiskWork(t, f)
		})
	}
}

func TestSingleLifecycleUnapprovedBlockedResumeOwnerPreservesLiveClaimAndSettlesRetiredWorkload(t *testing.T) {
	for _, test := range []struct {
		name    string
		retired bool
	}{
		{"Ready source with live binding", false},
		{"Moving source without binding or Node", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			singleLifecycleUntil(t, f, func(move volumeapi.Move, _ volumeapi.State) bool { return move.Status.Preparation != nil })
			move, source := f.move(t, 0), f.state(t, 0)
			if test.retired {
				next := source
				next.Phase = volumeapi.PhaseMoving
				if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, source, next); err != nil {
					t.Fatal(err)
				}
				source = f.state(t, 0)
				singleLifecycleRemoveWorkload(t, f)
			}
			status := move.Status
			status.Phase = "Blocked"
			move, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
			if err != nil {
				t.Fatal(err)
			}
			object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := unstructured.SetNestedField(object.Object, "ResumeOwner", "spec", "recovery"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.dynamic.Resource(volumeapi.MoveResource).Update(f.ctx, object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			move = f.move(t, 0) // Read the requested spec and its new resourceVersion.
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
				t.Fatalf("unapproved blocked decision required execution-recovery prerequisites: %v", err)
			}
			if !test.retired {
				for range 3 {
					blocked := f.move(t, 0)
					if blocked.UID != move.UID || blocked.Status.Phase != "Blocked" || blocked.Status.RecoveryPhase != "" || blocked.Status.CapacityApproved ||
						!reflect.DeepEqual(blocked.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(source, f.state(t, 0)) {
						t.Fatalf("ResumeOwner released a live blocked workload claim: move=%+v source=%+v", blocked.Status, f.state(t, 0))
					}
					assertPreparationReserved(t, f, preparationRequestedBytes)
					if err := f.reconciler.reconcilePersistedMove(f.ctx, blocked); err != nil {
						t.Fatal(err)
					}
				}
				assertNoEviction(t, f.client)
				singleLifecycleNoDiskWork(t, f)
				return
			}
			canceling := f.move(t, 0)
			if canceling.Status.Phase != "Canceling" || canceling.Status.Reason != "WorkloadRetired" || canceling.Status.RecoveryPhase != "" || canceling.Status.CapacityApproved ||
				!reflect.DeepEqual(canceling.Status.Preparation, move.Status.Preparation) || !reflect.DeepEqual(source, f.state(t, 0)) {
				t.Fatalf("retired workload bypassed cancellation settlement: move=%+v source=%+v", canceling.Status, f.state(t, 0))
			}
			assertPreparationReserved(t, f, preparationRequestedBytes)
			singleLifecycleUntil(t, f, func(move volumeapi.Move, state volumeapi.State) bool {
				return move.Status.Phase == "Cancelled" && state.ActiveMove == ""
			})
			source.Phase, source.ActiveMove = volumeapi.PhaseReady, ""
			settled := f.move(t, 0)
			if !reflect.DeepEqual(source, f.state(t, 0)) || settled.Status.Reason != "WorkloadRetired" || settled.Status.RecoveryPhase != "" || settled.Status.CapacityApproved ||
				settled.Status.IncomingCopy != nil || settled.Status.DestinationCopy != nil || settled.Status.CleanupPhase != "" {
				t.Fatalf("pre-disk settlement changed authority or invented execution evidence: move=%+v source=%+v", settled.Status, f.state(t, 0))
			}
			journals, err := f.reconciler.Cleanups.List(f.ctx)
			if err != nil || len(journals) != 0 {
				t.Fatalf("cancellation invented a cleanup journal or receipt: journals=%+v error=%v", journals, err)
			}
			assertPreparationReserved(t, f, 0)
			assertNoEviction(t, f.client)
			singleLifecycleNoDiskWork(t, f)
		})
	}
}

func singleLifecycleReplacement(t *testing.T, f *preparationRegistryFixture) *corev1.Pod {
	t.Helper()
	w := f.workloads[0]
	if err := f.client.CoreV1().Pods(w.Namespace.Name).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := w.Consumer.DeepCopy()
	pod.Name, pod.UID, pod.ResourceVersion = "held-replacement", "", ""
	pod.Spec.NodeName = ""
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	pod, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func singleLifecycleNoDiskWork(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	jobs, err := f.client.BatchV1().Jobs(f.reconciler.Namespace).List(f.ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("unexpected disk executor: jobs=%+v error=%v", jobs, err)
	}
	pods, err := f.client.CoreV1().Pods(f.reconciler.Namespace).List(f.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		if pod.Labels["shiftpv.io/role"] != placementRole {
			t.Fatalf("transfer helper acquired before disk approval was reobserved: %+v", pod)
		}
	}
	for _, resource := range []string{"secrets", "configmaps", "services"} {
		for _, action := range f.client.Actions() {
			if action.GetVerb() == "create" && action.GetResource().Resource == resource {
				t.Fatalf("transfer resource acquired before disk work: %#v", action)
			}
		}
	}
}

func ownedByMove(pod *corev1.Pod, move volumeapi.Move) bool {
	if pod == nil {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.APIVersion == "shiftpv.io/v1alpha1" && owner.Kind == "ShiftPVMove" &&
		owner.Name == move.Name && string(owner.UID) == move.UID
}
