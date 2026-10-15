package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func cleanupMoveFixture() (*Reconciler, *memoryRepository, *fake.Clientset) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, incoming, destination := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{
		Name: "move-cleanup", UID: "move-cleanup-uid",
		Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{
			Phase: string(fsm.PhaseWaitingForDestinationPublish), DestinationNode: "destination", DestinationPoolUID: destination.PoolUID,
			SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			CopyOperationID: "copy-move-cleanup-uid", PromotionOperationID: "promote-move-cleanup-uid",
		},
	}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{move.Spec.VolumeID: {
			UID: destination.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: "destination", ActiveMove: move.Name,
			PublishedNodes: []string{"destination"}, CurrentCopy: &destination,
		}},
		pools: []volumeapi.Pool{
			{Name: source.PoolName, UID: source.PoolUID, NodeName: "source", MountPath: "/source-pool"},
			{Name: destination.PoolName, UID: destination.PoolUID, NodeName: "destination", MountPath: "/destination-pool"},
		},
		moves: []volumeapi.Move{move},
	}
	client := fake.NewClientset(mobilityObjects(move.Spec.VolumeID)...)
	assignJobUIDs(client)
	return newTestReconciler(client, repository, withTestCleanups()), repository, client
}

type failingCleanupOperator struct{}

func (failingCleanupOperator) AdvanceReclaim(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error) {
	return cleanupapi.Cleanup{}, false, errors.New("cleanup API unavailable")
}

func settleCleanupFixture(t *testing.T, reconciler *Reconciler, repository *memoryRepository) {
	t.Helper()
	move := repository.moves[0]
	if err := reconciler.ensureCleanupContract(context.Background(), &move); err != nil {
		t.Fatal(err)
	}
	cleanup, err := reconciler.Cleanups.Get(context.Background(), cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID})
	if err != nil || cleanup.Status.Phase != cleanupapi.PhaseCompleted {
		t.Fatalf("cleanup did not settle: cleanup=%#v err=%v", cleanup, err)
	}
	// memoryRepository bypasses the registry decoder, which derives this
	// projection from the embedded journal on every real API read.
	repository.moves[0].Status.CleanupPhase = cleanup.Status.Phase
}

func TestMoveErrorsPreservePhaseAndActiveMove(t *testing.T) {
	for _, stage := range []string{"observation", "decision", "action"} {
		t.Run(stage, func(t *testing.T) {
			reconciler, repository, client := cleanupMoveFixture()
			wantReason := "ActionFailed"
			switch stage {
			case "observation":
				wantReason = "ObservationFailed"
				client.PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("node API unavailable")
				})
			case "decision":
				repository.moves[0].Status.Phase = "UnknownPhase"
			case "action":
				reconciler.CleanupOperator = failingCleanupOperator{}
			}
			before := repository.moves[0]
			if err := reconciler.reconcileMove(context.Background(), before); err == nil {
				t.Fatal("failure was hidden")
			}
			after := repository.moves[0]
			if after.Status.Phase != before.Status.Phase || after.Status.Reason != wantReason {
				t.Fatalf("failure changed phase or lost evidence: %+v", after.Status)
			}
			if repository.volumes[before.Spec.VolumeID].ActiveMove != before.Name {
				t.Fatal("failure released the active Move")
			}
			jobs, err := client.BatchV1().Jobs("system").List(context.Background(), metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 0 {
				t.Fatalf("failure left an unexpected job: %+v, %v", jobs, err)
			}
		})
	}
}

type rejectedStatusRepository struct {
	*memoryRepository
	rejectNext bool
}

func (r *rejectedStatusRepository) SetMoveStatus(ctx context.Context, name, uid string, next volumeapi.MoveStatus) error {
	if r.rejectNext {
		r.rejectNext = false
		return errors.New("status write rejected before persistence")
	}
	return r.memoryRepository.SetMoveStatus(ctx, name, uid, next)
}

func TestMoveActionSurvivesJournalFailureAndControllerRestart(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	reconciler.Repository = &rejectedStatusRepository{memoryRepository: repository, rejectNext: true}
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err == nil {
		t.Fatal("journal failure was hidden")
	}
	if repository.moves[0].Status.Phase != string(fsm.PhaseWaitingForDestinationPublish) {
		t.Fatal("rejected journal write advanced persisted phase")
	}
	cleanups, err := reconciler.Cleanups.List(ctx)
	if err != nil || len(cleanups) != 1 {
		t.Fatalf("cleanup intent did not precede journal write: %+v, %v", cleanups, err)
	}

	restarted := newTestReconciler(client, repository, withCleanupsFrom(reconciler))
	if err := restarted.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	cleanups, err = restarted.Cleanups.List(ctx)
	if err != nil || len(cleanups) != 1 {
		t.Fatalf("restart duplicated cleanup intent: %+v, %v", cleanups, err)
	}
	move := repository.moves[0]
	if move.Status.Phase != string(fsm.PhaseCleaningSource) || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatalf("restart lost cleanup progress or lock: %+v", move.Status)
	}
}

func TestMoveCompletionWaitsForPublishAndCleanupEvidence(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	move := repository.moves[0]
	state := repository.volumes[move.Spec.VolumeID]
	state.PublishedNodes = nil
	repository.volumes[move.Spec.VolumeID] = state
	if err := reconciler.reconcileMove(ctx, move); err != nil {
		t.Fatal(err)
	}
	jobs, err := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 || repository.moves[0].Status.Phase != move.Status.Phase {
		t.Fatalf("cleanup began before destination publication: %+v, %v", jobs, err)
	}
	state.PublishedNodes = []string{"destination"}
	repository.volumes[move.Spec.VolumeID] = state
	destination := *move.Status.DestinationCopy
	repository.readyPoolsConfigured = true
	repository.readyPools = []volumeapi.Pool{{
		Name: destination.PoolName, UID: destination.PoolUID, NodeName: destination.NodeName, MountPath: "/destination-pool",
		Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{
			Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: false}},
		}},
	}}
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	jobs, err = client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 || repository.moves[0].Status.Phase != move.Status.Phase {
		t.Fatalf("cleanup began before scanner publication proof: %+v, %v", jobs, err)
	}
	repository.readyPools[0].Status.Inventory.Copies[0].Published = true
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	if repository.moves[0].Status.Phase != string(fsm.PhaseCleaningSource) || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatal("cleanup request was not durably recorded with the Move locked")
	}
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	if repository.moves[0].Status.Phase != "Completing" || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatal("cleanup evidence was not persisted before unlock")
	}
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	state = repository.volumes[move.Spec.VolumeID]
	if repository.moves[0].Status.Phase != string(fsm.PhaseSucceeded) || state.ActiveMove != "" || state.OwnerNode != "destination" {
		t.Fatalf("cleanup did not finish on destination owner: %+v", state)
	}
}

func TestMoveCompletionJournalFailureRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	move := repository.moves[0]
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	move = repository.moves[0]
	if move.Status.Phase != "Completing" || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatal("completion evidence must precede unlock")
	}
	reconciler.Repository = &rejectedStatusRepository{memoryRepository: repository, rejectNext: true}
	if err := reconciler.reconcileMove(ctx, move); err == nil {
		t.Fatal("completion journal failure was hidden")
	}
	state := repository.volumes[move.Spec.VolumeID]
	if state.ActiveMove != "" || state.OwnerNode != "destination" || state.Phase != volumeapi.PhaseReady {
		t.Fatalf("completion did not release the destination owner: %+v", state)
	}
	if repository.moves[0].Status.Phase != "Completing" {
		t.Fatal("rejected completion journal unexpectedly persisted")
	}
	restarted := newTestReconciler(client, repository, withCleanupsFrom(reconciler))
	if err := restarted.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatalf("completion did not recover after restart: %v", err)
	}
	after := repository.moves[0].Status
	if after.Phase != string(fsm.PhaseSucceeded) || after.Reason != "" {
		t.Fatalf("completion did not converge: %+v", after)
	}
	state = repository.volumes[move.Spec.VolumeID]
	if state.ActiveMove != "" || state.OwnerNode != "destination" || state.Phase != volumeapi.PhaseReady {
		t.Fatalf("retry changed the released destination owner: %+v", state)
	}
	jobs, err := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("completion restarted disk work: %+v, %v", jobs, err)
	}
}

func TestCompletionRejectsConflictingAuthority(t *testing.T) {
	for _, scenario := range []string{"other owner", "other move", "moving", "blocked", "empty destination", "same source"} {
		t.Run(scenario, func(t *testing.T) {
			reconciler, repository, client := cleanupMoveFixture()
			settleCleanupFixture(t, reconciler, repository)
			move := repository.moves[0]
			move.Status.Phase = "Completing"
			state := repository.volumes[move.Spec.VolumeID]
			switch scenario {
			case "other owner":
				state.OwnerNode = "other"
			case "other move":
				state.ActiveMove = "new-move"
			case "moving":
				state.Phase = volumeapi.PhaseMoving
			case "blocked":
				state.Phase = volumeapi.PhaseBlocked
			case "empty destination":
				move.Status.DestinationNode = ""
			case "same source":
				move.Spec.SourceNode = "destination"
			}
			repository.moves[0] = move
			repository.volumes[move.Spec.VolumeID] = state
			if err := reconciler.reconcileMove(context.Background(), move); err != nil {
				t.Fatal(err)
			}
			after := repository.moves[0].Status
			if after.Phase != "Completing" || after.Reason != "CompletionAuthorityMismatch" {
				t.Fatalf("unsafe completion was not deferred: %+v", after)
			}
			got := repository.volumes[move.Spec.VolumeID]
			if got.Phase != state.Phase || got.OwnerNode != state.OwnerNode || got.ActiveMove != state.ActiveMove {
				t.Fatalf("completion altered conflicting authority: %+v", got)
			}
			if len(client.Actions()) != 0 {
				t.Fatalf("unsafe completion touched Kubernetes resources: %+v", client.Actions())
			}
		})
	}
}

func TestCompletionConfirmationPrecedesResourceDeletionAndUnlock(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	move := repository.moves[0]
	names := namesFor(move.Name)
	metadata := moveObjectMeta(move, names.Config, "system", transferLabels(names, move))
	metadata.UID = "config-uid"
	if _, err := client.CoreV1().ConfigMaps("system").Create(ctx, &corev1.ConfigMap{ObjectMeta: metadata}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	reconciler.Repository = &rejectedStatusRepository{memoryRepository: repository, rejectNext: true}
	if err := reconciler.reconcileMove(ctx, move); err == nil {
		t.Fatal("confirmation journal failure was hidden")
	}
	if repository.moves[0].Status.Phase != string(fsm.PhaseCleaningSource) || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatal("failed confirmation released the Move")
	}
	if _, err := client.CoreV1().ConfigMaps("system").Get(ctx, names.Config, metav1.GetOptions{}); err != nil {
		t.Fatalf("failed confirmation deleted transfer resource: %v", err)
	}
	if err := reconciler.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	if repository.moves[0].Status.Phase != string(fsm.PhaseCompleting) || repository.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
		t.Fatal("confirmation retry did not durably advance with lock intact")
	}
}

func TestCompletionRecoversAcrossAPIFailureBoundaries(t *testing.T) {
	for _, fault := range []string{"unlock response lost", "status response lost", "delete response lost", "repeated status rejection"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			base, repository, _ := cleanupMoveFixture()
			settleCleanupFixture(t, base, repository)
			repository.moves[0].Status.Phase = string(fsm.PhaseCompleting)
			move := repository.moves[0]
			names := namesFor(move.Name)
			// Completing must finish with only durable authority, even after all
			// workload, node and completed Job objects have disappeared.
			metadata := moveObjectMeta(move, names.SourcePod, "system", sourceLabels(names, move))
			metadata.UID = "source-pod-uid"
			client := fake.NewClientset(&corev1.Pod{ObjectMeta: metadata})
			reconciler := newTestReconciler(client, repository, withCleanupsFrom(base))
			var rejected *rejectedStatusRepository
			switch fault {
			case "unlock response lost":
				reconciler.Repository = &lostResponseRepository{memoryRepository: repository, stateCASResponses: 1}
			case "status response lost":
				reconciler.Repository = &lostResponseRepository{memoryRepository: repository, statusResponses: 1}
			case "delete response lost":
				injectAcceptedDeleteTimeout(t, client, "pods", names.SourcePod)
			case "repeated status rejection":
				rejected = &rejectedStatusRepository{memoryRepository: repository, rejectNext: true}
				reconciler.Repository = rejected
			}
			if err := reconciler.reconcileMove(ctx, move); err == nil {
				t.Fatal("injected failure was hidden")
			}
			if rejected != nil {
				for range 2 {
					rejected.rejectNext = true
					if err := reconciler.reconcileMove(ctx, repository.moves[0]); err == nil {
						t.Fatal("repeated failure was hidden")
					}
				}
			}
			restarted := newTestReconciler(client, repository, withCleanupsFrom(base))
			if err := restarted.ReconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			state := repository.volumes[move.Spec.VolumeID]
			if repository.moves[0].Status.Phase != string(fsm.PhaseSucceeded) || state.ActiveMove != "" || state.OwnerNode != "destination" {
				t.Fatalf("completion did not converge: move=%+v volume=%+v", repository.moves[0], state)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" || action.GetVerb() == "update" {
					t.Fatalf("completion restarted helper work: %+v", action)
				}
			}
		})
	}
}

type completionReadRepository struct {
	*memoryRepository
	getError error
}

func (r *completionReadRepository) Get(ctx context.Context, id string) (volumeapi.State, error) {
	if r.getError != nil {
		return volumeapi.State{}, r.getError
	}
	return r.memoryRepository.Get(ctx, id)
}

func TestCompletionAfterVolumeDeletion(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	settleCleanupFixture(t, reconciler, repository)
	repository.moves[0].Status.Phase = string(fsm.PhaseCompleting)
	move := repository.moves[0]
	reconciler.Repository = &rejectedStatusRepository{memoryRepository: repository, rejectNext: true}
	if err := reconciler.reconcileMove(ctx, move); err == nil {
		t.Fatal("completion journal failure was hidden")
	}
	if repository.volumes[move.Spec.VolumeID].ActiveMove != "" {
		t.Fatal("volume was not unlocked before simulated CSI deletion")
	}
	delete(repository.volumes, move.Spec.VolumeID)
	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, move.Spec.VolumeID)
	restarted := newTestReconciler(client, &completionReadRepository{memoryRepository: repository, getError: notFound}, withCleanupsFrom(reconciler))
	client.ClearActions()
	if err := restarted.ReconcileAll(ctx); err != nil {
		t.Fatalf("deleted Volume stranded completion: %v", err)
	}
	if repository.moves[0].Status.Phase != string(fsm.PhaseSucceeded) || len(repository.volumes) != 0 {
		t.Fatalf("deleted Volume did not finish metadata-only: %+v", repository.moves[0])
	}
	for _, action := range client.Actions() {
		// ReconcileAll also observes orphan workload holds, without examining
		// this deleted Volume's disk or helper resources.
		if list, ok := action.(ktesting.ListAction); ok && action.Matches("list", "pods") && action.GetResource().Group == "" &&
			action.GetNamespace() == metav1.NamespaceAll && list.GetListRestrictions().Labels.String() == admission.ManagedLabel+"=true" {
			continue
		}
		if list, ok := action.(ktesting.ListAction); ok && action.Matches("list", "pods") && action.GetNamespace() == "system" &&
			list.GetListRestrictions().Labels.String() == "" {
			continue
		}
		if action.GetVerb() != "get" && action.GetVerb() != "delete" {
			t.Fatalf("completion recreated or inspected disk resources: %+v", action)
		}
	}
}

func TestCompletionDoesNotTreatReadErrorsAsDeletion(t *testing.T) {
	for _, scenario := range []struct {
		phase   string
		missing bool
	}{
		{string(fsm.PhaseCompleting), false},
		{string(fsm.PhaseCleaningSource), false},
		{string(fsm.PhaseCleaningSource), true},
	} {
		t.Run(fmt.Sprintf("%s/missing=%v", scenario.phase, scenario.missing), func(t *testing.T) {
			reconciler, repository, client := cleanupMoveFixture()
			repository.moves[0].Status.Phase = scenario.phase
			var failure error = errors.New("API timeout")
			if scenario.missing {
				failure = apierrors.NewNotFound(schema.GroupResource{Resource: "shiftpvvolumes"}, "volume")
			}
			reconciler.Repository = &completionReadRepository{memoryRepository: repository, getError: failure}
			if err := reconciler.reconcileMove(context.Background(), repository.moves[0]); err == nil {
				t.Fatal("read failure was treated as successful completion")
			}
			if repository.moves[0].Status.Phase != scenario.phase || repository.moves[0].Status.Reason != "ObservationFailed" || len(client.Actions()) != 0 {
				t.Fatal("read failure changed phase or triggered side effects")
			}
		})
	}
}

type completionConflictRepository struct {
	*memoryRepository
}

func (r *completionConflictRepository) CompareAndSetState(ctx context.Context, id, phase, active, owner string, next volumeapi.State) error {
	current := r.volumes[id]
	current.ActiveMove = "new-move"
	r.volumes[id] = current
	return r.memoryRepository.CompareAndSetState(ctx, id, phase, active, owner, next)
}

func TestCompletionCASPreservesConcurrentMove(t *testing.T) {
	ctx := context.Background()
	reconciler, repository, client := cleanupMoveFixture()
	settleCleanupFixture(t, reconciler, repository)
	repository.moves[0].Status.Phase = string(fsm.PhaseCompleting)
	move := repository.moves[0]
	newNames := namesFor("new-move")
	if _, err := client.CoreV1().ConfigMaps("system").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: newNames.Config}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	reconciler.Repository = &completionConflictRepository{memoryRepository: repository}
	if err := reconciler.reconcileMove(ctx, move); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("concurrent lock was not fenced: %v", err)
	}
	restarted := newTestReconciler(client, repository, withCleanupsFrom(reconciler))
	if err := restarted.reconcileMove(ctx, repository.moves[0]); err != nil {
		t.Fatal(err)
	}
	if repository.volumes[move.Spec.VolumeID].ActiveMove != "new-move" || repository.moves[0].Status.Phase != string(fsm.PhaseCompleting) {
		t.Fatal("old completion overwrote the concurrent Move")
	}
	if _, err := client.CoreV1().ConfigMaps("system").Get(ctx, newNames.Config, metav1.GetOptions{}); err != nil {
		t.Fatalf("old completion deleted concurrent transfer resource: %v", err)
	}
}
