package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type lostResponseRepository struct {
	*memoryRepository
	createMoveResponses int
	deleteMoveResponses int
	stateCASResponses   int
	statusResponses     int
}

func (r *lostResponseRepository) DeleteMove(ctx context.Context, name, uid string) error {
	err := r.memoryRepository.DeleteMove(ctx, name, uid)
	if err == nil && r.deleteMoveResponses > 0 {
		r.deleteMoveResponses--
		return fmt.Errorf("API timeout after ShiftPVMove delete was accepted")
	}
	return err
}

func (r *lostResponseRepository) CreateMove(ctx context.Context, generateName string, spec volumeapi.MoveSpec) (volumeapi.Move, error) {
	move, err := r.memoryRepository.CreateMove(ctx, generateName, spec)
	if err == nil && r.createMoveResponses > 0 {
		r.createMoveResponses--
		return volumeapi.Move{}, fmt.Errorf("API timeout after ShiftPVMove create was accepted")
	}
	return move, err
}

func (r *lostResponseRepository) CompareAndSetState(ctx context.Context, id, phase, active, owner string, next volumeapi.State) error {
	err := r.memoryRepository.CompareAndSetState(ctx, id, phase, active, owner, next)
	if err == nil && r.stateCASResponses > 0 {
		r.stateCASResponses--
		return fmt.Errorf("API timeout after ShiftPVVolume status update was accepted")
	}
	return err
}

func (r *lostResponseRepository) SetMoveStatus(ctx context.Context, name, uid string, status volumeapi.MoveStatus) error {
	err := r.memoryRepository.SetMoveStatus(ctx, name, uid, status)
	if err == nil && r.statusResponses > 0 {
		r.statusResponses--
		return fmt.Errorf("API timeout after ShiftPVMove status update was accepted")
	}
	return err
}

func TestMoveDiscoveryConvergesAfterCreateResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	inner := &memoryRepository{
		volumes: map[string]volumeapi.State{volumeID: {Phase: volumeapi.PhaseReady, OwnerNode: "source", PublishedNodes: []string{"source"}}},
		pools:   []volumeapi.Pool{{Name: "source", NodeName: "source", MountPath: "/source-pool"}, {Name: "destination", NodeName: "destination", MountPath: "/destination-pool"}},
	}
	repository := &lostResponseRepository{memoryRepository: inner, createMoveResponses: 1}
	reconciler := newTestReconciler(fake.NewSimpleClientset(mobilityObjects(volumeID)...), repository)

	if err := reconciler.discoverMoves(ctx); err == nil {
		t.Fatal("lost create response was hidden")
	}
	if len(inner.moves) != 1 {
		t.Fatalf("accepted create was not retained: %#v", inner.moves)
	}
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(inner.moves) != 1 || inner.moves[0].Status.Phase != string(fsm.PhasePending) {
		t.Fatalf("create retry did not converge without duplication: %#v", inner.moves)
	}
}

func TestPendingMoveCancellationConvergesAfterDeleteResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid",
		Spec:   volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{Phase: string(fsm.PhasePending)},
	}
	inner := &memoryRepository{
		volumes: map[string]volumeapi.State{volumeID: {Phase: volumeapi.PhaseReady, OwnerNode: "source"}},
		pools:   []volumeapi.Pool{{Name: "source", NodeName: "source", MountPath: "/source-pool"}, {Name: "destination", NodeName: "destination", MountPath: "/destination-pool"}},
		moves:   []volumeapi.Move{move},
	}
	repository := &lostResponseRepository{memoryRepository: inner, deleteMoveResponses: 1}
	client := fake.NewSimpleClientset(readyNode("source", false), readyNode("destination", false))
	reconciler := newTestReconciler(client, repository)

	if err := reconciler.ReconcileAll(ctx); err == nil {
		t.Fatal("lost obsolete Move delete response was hidden")
	}
	if len(inner.moves) != 0 {
		t.Fatalf("accepted obsolete Move delete was not retained: %#v", inner.moves)
	}
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Fatalf("obsolete Move delete retry did not converge: %v", err)
	}
	state := inner.volumes[volumeID]
	if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || state.OwnerNode != "source" {
		t.Fatalf("obsolete Move cancellation changed volume authority: %#v", state)
	}
}

func TestCopyResourcesWaitForDurableDestinationAfterStatusResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, _, _ := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{
		Name:   "move-test",
		UID:    "move-uid",
		Spec:   volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{Phase: string(fsm.PhaseWaitingForCapacity), ClaimNamespace: "workload", CandidateNodes: []string{"destination"}, DestinationPoolUID: "destination-pool-uid", CapacityApproved: true, SourceBytes: 1, SourceCopy: &source},
	}
	inner := &memoryRepository{
		pools: []volumeapi.Pool{{Name: "source-pool", UID: "source-pool-uid", NodeName: "source", MountPath: "/source-pool"}, {Name: "destination-pool", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination-pool"}},
		moves: []volumeapi.Move{move},
	}
	repository := &lostResponseRepository{memoryRepository: inner, statusResponses: 1}
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: "workload", UID: "replacement-uid"},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: placementHoldName}},
			Volumes:         claimVolumes(),
		},
	}
	client := fake.NewSimpleClientset(replacement)
	reconciler := newTestReconciler(client, repository)
	observed := observation{
		DestinationNode: "destination",
		Replacement:     replacement,
		Names:           namesFor(move.Name),
	}

	if err := reconciler.ensureCopy(ctx, &move, observed); err == nil {
		t.Fatal("lost destination status response was hidden")
	}
	if _, err := client.CoreV1().Secrets("system").Get(ctx, observed.Names.Secret, metav1.GetOptions{}); err == nil {
		t.Fatal("copy resources started before the destination status write was acknowledged")
	}
	persisted := inner.moves[0]
	if persisted.Status.DestinationNode != "destination" || persisted.Status.CopyJobName != observed.Names.CopyJob || persisted.Status.ReplacementUID != "replacement-uid" {
		t.Fatalf("accepted destination journal was not retained: %+v", persisted.Status)
	}
	if err := reconciler.ensureCopy(ctx, &persisted, observed); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Secrets("system").Get(ctx, observed.Names.Secret, metav1.GetOptions{}); err != nil {
		t.Fatalf("copy resources were not created on retry: %v", err)
	}
}

func TestCopyResourcesConvergeAfterCreateResponseLost(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{
		Name:   "move-test",
		UID:    "move-uid",
		Spec:   volumeapi.MoveSpec{VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", SourceNode: "source"},
		Status: volumeapi.MoveStatus{DestinationNode: "destination"},
	}
	if move.Status.SourceCopy == nil {
		source, _, _ := testCopyIdentities(move.Spec.VolumeID, "source", "destination")
		move.Status.SourceCopy = &source
	}
	names := namesFor(move.Name)
	tests := []struct {
		name     string
		resource string
		object   string
	}{
		{name: "secret", resource: "secrets", object: names.Secret},
		{name: "config", resource: "configmaps", object: names.Config},
		{name: "source pod", resource: "pods", object: names.SourcePod},
		{name: "source service", resource: "services", object: names.SourceService},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			injectAcceptedCreateTimeout(t, client, test.resource, test.object)
			repository := &memoryRepository{pools: []volumeapi.Pool{
				{Name: "source-pool", UID: "source-pool-uid", NodeName: "source", MountPath: "/source-pool"},
				{Name: "destination-pool", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination-pool"},
			}}
			reconciler := newTestReconciler(client, repository)

			if err := reconciler.ensureCopyResources(ctx, move, names); err == nil {
				t.Fatal("lost transfer resource create response was hidden")
			}
			if err := reconciler.ensureCopyResources(ctx, move, names); err != nil {
				t.Fatalf("idempotent transfer resource retry failed: %v", err)
			}
		})
	}
}

func TestMobilityJobsConvergeAfterCreateResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, incoming, destination := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid",
		Spec:   volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{DestinationNode: "destination", SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination, CopyOperationID: "copy-move-uid", PromotionOperationID: "promote-move-uid"},
	}
	names := namesFor(move.Name)
	tests := []struct {
		name    string
		jobName string
		ensure  func(*Reconciler) error
	}{
		{name: "copy", jobName: names.CopyJob, ensure: func(r *Reconciler) error { return r.ensureCopyJob(ctx, move, names) }},
		{name: "promotion", jobName: names.PromotionJob, ensure: func(r *Reconciler) error { return r.ensurePromotionJob(ctx, move, names) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			injectAcceptedCreateTimeout(t, client, "jobs", test.jobName)
			assignJobUIDs(client)
			repository := &memoryRepository{volumes: map[string]volumeapi.State{move.Spec.VolumeID: {Phase: "Ready", OwnerNode: "destination", ActiveMove: move.Name, PublishedNodes: []string{"destination"}}}, pools: []volumeapi.Pool{
				{Name: "source-pool", UID: "source-pool-uid", NodeName: "source", MountPath: "/source-pool"},
				{Name: "destination-pool", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination-pool"},
			}}
			reconciler := newTestReconciler(client, repository)

			if err := test.ensure(reconciler); err == nil {
				t.Fatal("lost Job create response was hidden")
			}
			if _, err := client.BatchV1().Jobs("system").Get(ctx, test.jobName, metav1.GetOptions{}); err != nil {
				t.Fatalf("accepted Job create was not retained: %v", err)
			}
			if err := test.ensure(reconciler); err != nil {
				t.Fatalf("idempotent Job retry failed: %v", err)
			}
			jobs, err := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 1 {
				t.Fatalf("Job retry created duplicates: jobs=%d err=%v", len(jobs.Items), err)
			}
		})
	}
}

func TestVolumeLockConvergesAfterCASResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}}
	inner := &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {
		Phase: volumeapi.PhaseReady, OwnerNode: "source", PublishedNodes: []string{"source"},
	}}, pools: []volumeapi.Pool{{Name: "source-pool", UID: "source-pool-uid", NodeName: "source"}}}
	repository := &lostResponseRepository{memoryRepository: inner, stateCASResponses: 1}
	reconciler := &Reconciler{Repository: repository}
	state, _ := repository.Get(ctx, volumeID)
	observed := observation{
		Volume:         state,
		PV:             &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv"}},
		Claim:          &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "workload"}},
		Consumer:       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "workload", UID: "consumer-uid"}},
		CandidateNodes: []string{"destination"},
	}

	if err := reconciler.lockVolume(ctx, &move, observed); err == nil {
		t.Fatal("lost volume lock response was hidden")
	}
	locked := inner.volumes[volumeID]
	if locked.Phase != volumeapi.PhaseMoving || locked.OwnerNode != "source" || locked.ActiveMove != move.Name {
		t.Fatalf("accepted volume lock was not retained: %#v", locked)
	}
	if err := reconciler.lockVolume(ctx, &move, observed); err != nil {
		t.Fatalf("volume lock retry did not converge: %v", err)
	}
	if move.Status.ConsumerUID != "consumer-uid" || len(move.Status.CandidateNodes) != 1 {
		t.Fatalf("move journal was not populated after retry: %+v", move.Status)
	}
}

func TestOwnerCommitConvergesAfterCASResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	_, _, destination := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}, Status: volumeapi.MoveStatus{
		CandidateNodes: []string{"destination"}, DestinationNode: "destination", DestinationCopy: &destination,
	}}
	inner := &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {
		Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name,
	}}}
	repository := &lostResponseRepository{memoryRepository: inner, stateCASResponses: 1}
	reconciler := &Reconciler{Repository: repository, Namespace: "system", HelperImage: "helper"}
	placement := reconciler.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
	placement.Spec.NodeName = "destination"
	reconciler.Client = fake.NewSimpleClientset(placement)
	observed := observation{Volume: identifiedTestState(volumeID, inner.volumes[volumeID], inner.pools), DestinationNode: "destination"}

	if err := reconciler.commitOwner(ctx, &move, observed); err == nil {
		t.Fatal("lost owner commit response was hidden")
	}
	committed := inner.volumes[volumeID]
	if committed.Phase != volumeapi.PhaseReady || committed.OwnerNode != "destination" || committed.ActiveMove != move.Name {
		t.Fatalf("accepted owner commit was not retained: %#v", committed)
	}
	observed.Volume = committed
	observed.FSM.OwnerCommitted = true
	if err := reconciler.commitOwner(ctx, &move, observed); err != nil {
		t.Fatalf("owner commit retry did not converge: %v", err)
	}
}

func TestCompletionConvergesAfterActiveMoveClearResponseLost(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, _, destination := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		UID: "move-uid", Status: volumeapi.MoveStatus{Phase: string(fsm.PhaseCompleting), DestinationNode: "destination", DestinationPoolUID: destination.PoolUID, SourceCopy: &source, DestinationCopy: &destination}}
	inner := &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {
		UID: destination.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: "destination", ActiveMove: move.Name, PublishedNodes: []string{"destination"}, CurrentCopy: &destination,
	}}, moves: []volumeapi.Move{move}}
	cleanups := newTestCleanupStore()
	preparer := &Reconciler{Repository: inner, Cleanups: cleanups, CleanupOperator: receiptCleanupOperator{}}
	if err := preparer.ensureCleanupContract(ctx, &move); err != nil {
		t.Fatal(err)
	}
	repository := &lostResponseRepository{memoryRepository: inner, stateCASResponses: 1}
	reconciler := &Reconciler{Client: fake.NewSimpleClientset(), Repository: repository, Namespace: "system", Cleanups: cleanups, CleanupOperator: receiptCleanupOperator{}}
	observed := observation{Volume: identifiedTestState(volumeID, inner.volumes[volumeID], inner.pools), Names: namesFor(move.Name)}
	for name, current := range map[string]*volume.CopyIdentity{
		"missing current copy": nil,
		"different current copy": func() *volume.CopyIdentity {
			changed := destination
			changed.CopyID = "different-copy"
			return &changed
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			invalid := observed
			invalid.Volume.CurrentCopy = current
			if err := reconciler.markSucceeded(ctx, &move, invalid); err == nil {
				t.Fatal("completion accepted without exact destination authority")
			}
		})
	}

	if err := reconciler.markSucceeded(ctx, &move, observed); err == nil {
		t.Fatal("lost completion response was hidden")
	}
	completed := inner.volumes[volumeID]
	if completed.ActiveMove != "" || completed.OwnerNode != "destination" || completed.Phase != volumeapi.PhaseReady {
		t.Fatalf("accepted activeMove clear was not retained: %#v", completed)
	}
	observed.Volume = completed
	if err := reconciler.markSucceeded(ctx, &move, observed); err != nil {
		t.Fatalf("completion retry did not converge: %v", err)
	}
}

func TestTransferCleanupConvergesAfterDeleteResponseLost(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{Name: "move-test", UID: "move-uid"}
	names := namesFor(move.Name)
	owned := func(name string) metav1.ObjectMeta {
		metadata := moveObjectMeta(move, name, "system", transferLabels(names, move))
		metadata.UID = types.UID(name + "-uid")
		return metadata
	}
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: owned(names.SourcePod)},
		&corev1.Service{ObjectMeta: owned(names.SourceService)},
		&corev1.ConfigMap{ObjectMeta: owned(names.Config)},
		&corev1.Secret{ObjectMeta: owned(names.Secret)},
	)
	injectAcceptedDeleteTimeout(t, client, "pods", names.SourcePod)
	reconciler := &Reconciler{Client: client, Namespace: "system"}

	if err := reconciler.deleteTransferResources(ctx, move, names); err == nil {
		t.Fatal("lost delete response was hidden")
	}
	if err := reconciler.deleteTransferResources(ctx, move, names); err != nil {
		t.Fatalf("transfer cleanup retry did not converge: %v", err)
	}
	if objects, err := client.CoreV1().Pods("system").List(ctx, metav1.ListOptions{}); err != nil || len(objects.Items) != 0 {
		t.Fatalf("source Pod remained after cleanup retry: pods=%d err=%v", len(objects.Items), err)
	}
}

func injectAcceptedCreateTimeout(t *testing.T, client *fake.Clientset, resource, name string) {
	t.Helper()
	injected := false
	client.PrependReactor("create", resource, func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		object := create.GetObject()
		metadata, ok := object.(metav1.Object)
		if !ok || metadata.GetName() != name || injected {
			return false, nil, nil
		}
		injected = true
		if metadata.GetUID() == "" {
			metadata.SetUID(types.UID(name + "-uid"))
		}
		if err := client.Tracker().Create(action.GetResource(), object.DeepCopyObject(), action.GetNamespace()); err != nil {
			t.Fatalf("inject accepted create: %v", err)
		}
		return true, nil, fmt.Errorf("API timeout after %s create was accepted", name)
	})
}

func injectAcceptedDeleteTimeout(t *testing.T, client *fake.Clientset, resource, name string) {
	t.Helper()
	injected := false
	client.PrependReactor("delete", resource, func(action ktesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(ktesting.DeleteAction)
		if deleteAction.GetName() != name || injected {
			return false, nil, nil
		}
		injected = true
		if err := client.Tracker().Delete(action.GetResource(), action.GetNamespace(), name); err != nil {
			t.Fatalf("inject accepted delete: %v", err)
		}
		return true, nil, fmt.Errorf("API timeout after %s delete was accepted", name)
	})
}
