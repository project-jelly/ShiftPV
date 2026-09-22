package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func recoveryFixture(t *testing.T, owner string) (*Reconciler, *memoryRepository, *fake.Clientset) {
	t.Helper()
	r, repo, client, _, _ := recoveryTransactionFixture(t, owner, owner == "destination")
	return r, repo, client
}

// recoveryTransactionFixture builds a blocked ResumeOwner move whose volume is
// still owned by owner. recordDestination journals the destination transaction
// identities on the move: the destination owner reached them by definition, and
// a source-owned rollback has to settle the same recorded artifacts.
func recoveryTransactionFixture(t *testing.T, owner string, recordDestination bool) (*Reconciler, *memoryRepository, *fake.Clientset, volume.CopyIdentity, volume.CopyIdentity) {
	t.Helper()
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	source, incoming, destination := testCopyIdentities(id, "source", "destination")
	incoming.CopyID = "move-move-uid-incoming"
	destination.CopyID = "move-move-uid-serving"
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: "source", Recovery: "ResumeOwner"}, Status: volumeapi.MoveStatus{
		Phase: "Blocked", Reason: "OriginalFailure", PersistentVolumeName: "pv", ClaimNamespace: "workload", ClaimName: "claim", DestinationNode: "destination", SourceCopy: &source,
	}}
	state := volumeapi.State{UID: source.VolumeUID, Phase: "Blocked", ActiveMove: move.Name, OwnerNode: owner, CurrentCopy: &source, CapacityBytes: 32 << 20}
	if recordDestination {
		move.Status.DestinationPoolUID = destination.PoolUID
		move.Status.IncomingCopy, move.Status.DestinationCopy = &incoming, &destination
		move.Status.CopyJobName = namesFor(move.Name).CopyJob
		move.Status.CopyOperationID, move.Status.PromotionOperationID = "copy-"+move.UID, "promote-"+move.UID
		move.Status.CapacityApproved = true
	}
	if owner == "destination" {
		state.CurrentCopy = &destination
	}
	repo := &memoryRepository{moves: []volumeapi.Move{move}, volumes: map[string]volumeapi.State{id: state}, pools: []volumeapi.Pool{
		{Name: source.PoolName, UID: source.PoolUID, NodeName: "source", MountPath: "/source"},
		{Name: destination.PoolName, UID: destination.PoolUID, NodeName: "destination", MountPath: "/destination"},
	}}
	fixture := newMobilityFixture(id)
	fixture.SourceNode.Spec.Unschedulable = false
	fixture.Consumer.Spec.NodeName = owner
	client := fake.NewSimpleClientset(fixture.Objects()...)
	return newTestReconciler(client, repo, withTestCleanups()), repo, client, incoming, destination
}

func finishRecoveryJobs(t *testing.T, client *fake.Clientset, condition batchv1.JobConditionType) {
	t.Helper()
	jobs, err := client.BatchV1().Jobs("system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
		if _, err := client.BatchV1().Jobs("system").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryResumesOnlyCurrentOwnerAndSurvivesEveryBoundary(t *testing.T) {
	for _, owner := range []string{"source", "destination"} {
		t.Run(owner, func(t *testing.T) {
			r, repo, client := recoveryFixture(t, owner)
			id := repo.moves[0].Spec.VolumeID
			for cycle := 0; cycle < 24; cycle++ {
				// Construct a fresh reconciler on every pass: no in-memory recovery state.
				restarted := newTestReconciler(client, repo, withCleanupsFrom(r), func(fresh *Reconciler) {
					fresh.ServiceAccountName = r.ServiceAccountName
				})
				if err := restarted.ReconcileAll(context.Background()); err != nil {
					t.Fatal(err)
				}
				if repo.volumes[id].OwnerNode != owner {
					t.Fatal("recovery changed authoritative owner")
				}
				if repo.volumes[id].Phase == "Ready" {
					state := repo.volumes[id]
					state.PublishedNodes = []string{owner}
					repo.volumes[id] = state
				}
				finishRecoveryJobs(t, client, batchv1.JobComplete)
			}
			if repo.moves[0].Status.Phase != "Blocked" || repo.moves[0].Status.Reason != "OriginalFailure" || repo.moves[0].Status.RecoveryPhase != recoveryRecovered || repo.volumes[id].ActiveMove != "" || !strings.Contains(repo.moves[0].Status.Message, "no operator action is required") {
				t.Fatalf("move=%+v volume=%+v", repo.moves[0], repo.volumes[id])
			}
			jobs, _ := client.BatchV1().Jobs("system").List(context.Background(), metav1.ListOptions{})
			if len(jobs.Items) != 0 {
				t.Fatal("recovery jobs left behind")
			}
		})
	}
}

func TestRecoveryFailsClosedOnUnsafeObservation(t *testing.T) {
	for _, scenario := range []string{"foreign writer", "unknown owner", "changed owner", "different move", "not blocked", "cordoned owner", "unready peer", "binding UID changed", "API failure", "unrecorded destination"} {
		t.Run(scenario, func(t *testing.T) {
			r, repo, client := recoveryFixture(t, "source")
			id := repo.moves[0].Spec.VolumeID
			state := repo.volumes[id]
			switch scenario {
			case "foreign writer":
				state.PublishedNodes = []string{"destination"}
			case "unknown owner":
				state.OwnerNode = "unknown"
			case "changed owner":
				repo.moves[0].Status.RecoveryOwner = "destination"
			case "different move":
				state.ActiveMove = "newer-move"
			case "not blocked":
				state.Phase = "Moving"
			case "unrecorded destination":
				repo.moves[0].Status.DestinationNode = ""
				repo.moves[0].Status.CopyJobName = "copy"
			case "cordoned owner", "unready peer":
				name := "source"
				if scenario == "unready peer" {
					name = "destination"
				}
				node, _ := client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
				if scenario == "cordoned owner" {
					node.Spec.Unschedulable = true
				} else {
					node.Status.Conditions = nil
				}
				_, _ = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
			case "binding UID changed":
				claim, _ := client.CoreV1().PersistentVolumeClaims("workload").Get(context.Background(), "claim", metav1.GetOptions{})
				claim.UID = "replacement-claim"
				_, _ = client.CoreV1().PersistentVolumeClaims("workload").Update(context.Background(), claim, metav1.UpdateOptions{})
			case "API failure":
				client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("timeout") })
			}
			repo.volumes[id] = state
			if err := r.ReconcileAll(context.Background()); err == nil {
				t.Fatal("unsafe observation was accepted")
			}
			if repo.volumes[id].Phase == "Ready" || repo.volumes[id].OwnerNode != state.OwnerNode || repo.moves[0].Status.RecoveryReason == "" || !strings.Contains(repo.moves[0].Status.Message, "operator action required") {
				t.Fatalf("unsafe recovery state: %+v %+v", repo.volumes[id], repo.moves[0])
			}
			jobs, _ := client.BatchV1().Jobs("system").List(context.Background(), metav1.ListOptions{})
			if len(jobs.Items) != 0 {
				t.Fatal("unsafe observation started helper jobs")
			}
		})
	}
}

func TestRecoveryWaitsForOldHelpersAndFailedVerification(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "source", recoveryQuiescing
	repo.moves[0] = move
	names := namesFor(move.Name)
	jobMeta := moveObjectMeta(move, names.CopyJob, "system", transferLabels(names, move))
	jobMeta.UID = "old-job"
	podMeta := moveObjectMeta(move, names.SourcePod, "system", transferLabels(names, move))
	podMeta.UID = "old-pod"
	_, _ = client.BatchV1().Jobs("system").Create(context.Background(), &batchv1.Job{ObjectMeta: jobMeta}, metav1.CreateOptions{})
	_, _ = client.CoreV1().Pods("system").Create(context.Background(), &corev1.Pod{ObjectMeta: podMeta, Spec: corev1.PodSpec{NodeName: "source"}}, metav1.CreateOptions{})
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.RecoveryPhase != recoveryQuiescing {
		t.Fatal("advanced without read-back of termination")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "delete" {
			continue
		}
		opts := action.(k8stesting.DeleteAction).GetDeleteOptions()
		if opts.Preconditions == nil || opts.Preconditions.UID == nil || (opts.GracePeriodSeconds != nil && *opts.GracePeriodSeconds == 0) {
			t.Fatalf("unsafe delete: %#v", opts)
		}
	}
	for i := 0; i < 2; i++ {
		if err := r.ReconcileAll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	jobs, _ := client.BatchV1().Jobs("system").List(context.Background(), metav1.ListOptions{})
	if len(jobs.Items) != 1 || jobs.Items[0].Spec.TTLSecondsAfterFinished != nil || !jobs.Items[0].Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatalf("verify job not read-only/durable: %+v", jobs)
	}
	finishRecoveryJobs(t, client, batchv1.JobFailed)
	if err := r.ReconcileAll(context.Background()); err == nil {
		t.Fatal("failed verification accepted")
	}
	if repo.volumes[move.Spec.VolumeID].Phase != "Blocked" {
		t.Fatal("failed verification opened mount guard")
	}
}

func TestRecoveryQuiescesScheduledPlacementBeforeDestinationIsRecorded(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Status.DestinationNode = ""
	move.Status.CandidateNodes = []string{"destination"}
	move.Status.RecoveryOwner = "source"
	move.Status.RecoveryPhase = recoveryQuiescing
	repo.moves[0] = move

	names := namesFor(move.Name)
	placement := r.placementPod(move, &corev1.Pod{}, names)
	placement.UID = "placement-uid"
	placement.Spec.NodeName = "destination"
	if _, err := client.CoreV1().Pods("system").Create(context.Background(), placement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("scheduled reservation blocked recovery before destination persistence: %v", err)
	}
	if _, err := client.CoreV1().Pods("system").Get(context.Background(), names.PlacementPod, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("placement reservation remained after quiesce: %v", err)
	}
	if repo.moves[0].Status.RecoveryReason != "" || repo.moves[0].Status.RecoveryPhase != recoveryQuiescing {
		t.Fatalf("unexpected recovery state after reservation deletion: %+v", repo.moves[0].Status)
	}
	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.RecoveryPhase != recoveryVerifying {
		t.Fatalf("recovery did not advance after reservation disappearance: %+v", repo.moves[0].Status)
	}
}

func TestRecoveryRejectsUnrecordedDataHelper(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Status.DestinationNode = ""
	move.Status.RecoveryOwner = "source"
	move.Status.RecoveryPhase = recoveryQuiescing
	repo.moves[0] = move

	names := namesFor(move.Name)
	_, err := client.CoreV1().Pods("system").Create(context.Background(), &corev1.Pod{
		ObjectMeta: func() metav1.ObjectMeta {
			metadata := moveObjectMeta(move, names.SourcePod, "system", transferLabels(names, move))
			metadata.UID = "unknown-helper"
			return metadata
		}(),
		Spec: corev1.PodSpec{NodeName: "unknown"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.ReconcileAll(context.Background()); err == nil {
		t.Fatal("unrecorded data helper was accepted")
	}
	if repo.moves[0].Status.RecoveryReason != "RecoveryStepFailed" || !strings.Contains(repo.moves[0].Status.RecoveryMessage, "unrecorded node") {
		t.Fatalf("unexpected recovery error: %+v", repo.moves[0].Status)
	}
}

func TestRecoveryPlacementHonorsPDBAndPodUID(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Status.RecoveryOwner = "source"
	claim, _ := client.CoreV1().PersistentVolumeClaims("workload").Get(context.Background(), "claim", metav1.GetOptions{})
	pod, _ := client.CoreV1().Pods("workload").Get(context.Background(), "consumer", metav1.GetOptions{})
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	_, _ = client.CoreV1().Pods("workload").Update(context.Background(), pod, metav1.UpdateOptions{})
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		eviction := action.(k8stesting.CreateAction).GetObject().(*policyv1.Eviction)
		if eviction.DeleteOptions == nil || eviction.DeleteOptions.Preconditions == nil || *eviction.DeleteOptions.Preconditions.UID != pod.UID {
			t.Fatal("eviction has no observed UID")
		}
		return true, nil, apierrors.NewTooManyRequests("PDB denied", 1)
	})
	if ready, err := r.recoverPlacement(context.Background(), move, claim); err == nil || ready {
		t.Fatal("PDB bypassed")
	}
}

func TestDiscoveryWaitsForDestinationRecoveryJournalAfterFinalCAS(t *testing.T) {
	r, repo, client := recoveryFixture(t, "destination")
	id := repo.moves[0].Spec.VolumeID
	repo.moves[0].Status.RecoveryPhase = recoveryCompleting
	repo.moves[0].Status.RecoveryOwner = "destination"
	repo.volumes[id] = volumeapi.State{Phase: "Ready", OwnerNode: "destination", PublishedNodes: []string{"destination"}}
	node, _ := client.CoreV1().Nodes().Get(context.Background(), "destination", metav1.GetOptions{})
	node.Spec.Unschedulable = true
	_, _ = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	if err := r.discoverMoves(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 1 {
		t.Fatal("new transaction discovered before recovery terminal journal")
	}
	if err := r.reconcileRecovery(context.Background(), repo.moves[0]); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.RecoveryPhase != recoveryRecovered {
		t.Fatal("final CAS crash did not converge")
	}
}

func rollbackRecoveryFixture(t *testing.T, copies []volumeapi.CopyObservation) (*Reconciler, *memoryRepository, volume.CopyIdentity, volume.CopyIdentity) {
	t.Helper()
	r, repo, _, incoming, destination := recoveryTransactionFixture(t, "source", true)
	move := repo.moves[0]
	transitionedAt := time.Date(2026, 9, 13, 1, 0, 0, 0, time.UTC)
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "source", recoveryRetiring
	move.Status.LastTransitionTime = transitionedAt.Format(time.RFC3339Nano)
	move.Status.SourceBytes = 32 << 20
	// This fixture starts after a durable rollback scan fence has been observed.
	move.Status.RollbackRequiredGeneration = 1
	repo.moves[0] = move

	observedAt := transitionedAt.Add(time.Second)
	for index := range repo.pools {
		if repo.pools[index].NodeName != "destination" {
			continue
		}
		repo.pools[index].Generation = 1
		repo.pools[index].Finalizers = []string{volumeapi.PoolProtectionFinalizer}
		repo.pools[index].Status = volumeapi.PoolStatus{
			ObservedGeneration: 1,
			LastProbeTime:      metav1.NewTime(observedAt),
			Conditions: []metav1.Condition{{
				Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1,
			}},
			Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(observedAt), Valid: true, Copies: copies},
		}
	}
	r.Now = func() time.Time { return observedAt.Add(time.Second) }
	r.PoolReadinessStaleAfter = time.Minute
	return r, repo, incoming, destination
}

func TestRecoveryRollbackCleansExactlyOneObservedDestinationArtifact(t *testing.T) {
	for _, role := range []string{volume.RoleIncoming, volume.RoleServing} {
		t.Run(role, func(t *testing.T) {
			r, repo, incoming, destination := rollbackRecoveryFixture(t, nil)
			target := incoming
			if role == volume.RoleServing {
				target = destination
			}
			repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &target, Present: true}}
			move := repo.moves[0]
			state, err := repo.Get(context.Background(), move.Spec.VolumeID)
			if err != nil {
				t.Fatal(err)
			}
			done, err := r.settleRecoveryArtifacts(context.Background(), &move, state)
			if err != nil || done {
				t.Fatalf("first settlement done=%v err=%v", done, err)
			}
			cleanup, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move))
			if err != nil || cleanup.Spec.Reason != "MoveRollback" || cleanup.Spec.Target != target || cleanup.Status.Phase != "Completed" {
				t.Fatalf("cleanup=%#v err=%v", cleanup, err)
			}
			settled := repo.moves[0]
			if settled.Status.CapacityApproved || settled.Status.CapacityReason != recoveryCapacitySettled {
				t.Fatalf("capacity hold was not durably settled: %+v", settled.Status)
			}
			done, err = r.settleRecoveryArtifacts(context.Background(), &settled, state)
			if err != nil || !done {
				t.Fatalf("settled retry done=%v err=%v", done, err)
			}
		})
	}
}

func TestRecoveryRollbackAcceptsOnlyFreshExactAbsence(t *testing.T) {
	t.Run("no destination effect before source identity journal", func(t *testing.T) {
		r, repo, _ := recoveryFixture(t, "source")
		move := repo.moves[0]
		move.Status.SourceCopy = nil
		move.Status.RecoveryOwner, move.Status.RecoveryPhase = "source", recoveryRetiring
		move.Status.CapacityReason = "DestinationFilesystemSpace"
		repo.moves[0] = move
		state, _ := repo.Get(context.Background(), move.Spec.VolumeID)
		done, err := r.settleRecoveryArtifacts(context.Background(), &move, state)
		if err != nil || done {
			t.Fatalf("first settlement done=%v err=%v", done, err)
		}
		if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
			t.Fatalf("no-effect recovery did not settle: %+v", repo.moves[0].Status)
		}
	})

	t.Run("already absent", func(t *testing.T) {
		r, repo, _, _ := rollbackRecoveryFixture(t, nil)
		move := repo.moves[0]
		state, _ := repo.Get(context.Background(), move.Spec.VolumeID)
		done, err := r.settleRecoveryArtifacts(context.Background(), &move, state)
		if err != nil || done {
			t.Fatalf("first settlement done=%v err=%v", done, err)
		}
		if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
			t.Fatalf("fresh absence did not settle capacity: %+v", repo.moves[0].Status)
		}
		if _, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
			t.Fatalf("already-absent rollback invented a cleanup receipt: %v", err)
		}
	})

	t.Run("inventory before requested generation", func(t *testing.T) {
		r, repo, _, _ := rollbackRecoveryFixture(t, nil)
		move := repo.moves[0]
		move.Status.RollbackRequiredGeneration = repo.pools[1].Generation + 1
		state, _ := repo.Get(context.Background(), move.Spec.VolumeID)
		if done, err := r.settleRecoveryArtifacts(context.Background(), &move, state); err == nil || done {
			t.Fatalf("non-causal absence was accepted: done=%v err=%v", done, err)
		}
		if !repo.moves[0].Status.CapacityApproved {
			t.Fatal("capacity hold was released without post-Retiring evidence")
		}
	})
}

func TestRecoveryRollbackPreservesAmbiguousArtifactsForReview(t *testing.T) {
	for _, scenario := range []string{"both transaction copies", "problem observation", "conflicting copy", "published conflict", "published incoming", "published destination"} {
		t.Run(scenario, func(t *testing.T) {
			r, repo, incoming, destination := rollbackRecoveryFixture(t, nil)
			switch scenario {
			case "both transaction copies":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &incoming, Present: true}, {Identity: &destination, Present: true}}
			case "problem observation":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Marker: "path:volumes/unknown", Present: true, Problem: "UnrecordedPath"}}
			case "conflicting copy", "published conflict":
				conflict := destination
				conflict.CopyID = "foreign-copy"
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &conflict, Present: true, Published: scenario == "published conflict"}}
			case "published incoming":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &incoming, Present: true, Published: true}}
			case "published destination":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}}
			}
			move := repo.moves[0]
			err := r.reconcileRecovery(context.Background(), move)
			if !errors.Is(err, errRecoveryCleanupNeedsReview) || repo.moves[0].Status.RecoveryReason != "CleanupNeedsReview" {
				t.Fatalf("ambiguous artifact was not sent to review: status=%+v err=%v", repo.moves[0].Status, err)
			}
			if !repo.moves[0].Status.CapacityApproved {
				t.Fatal("ambiguous artifact released the capacity hold")
			}
			if _, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
				t.Fatalf("ambiguous target created a destructive intent: %v", err)
			}
		})
	}
}

func TestPostcommitRecoveryWaitsForActualDestinationPublicationBeforeSourceCleanup(t *testing.T) {
	r, repo, _ := recoveryFixture(t, "destination")
	move := repo.moves[0]
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "destination", recoveryRetiring
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	state.Phase = volumeapi.PhaseReady
	state.PublishedNodes = nil
	repo.volumes[move.Spec.VolumeID] = state

	if err := r.reconcileRecovery(context.Background(), move); err == nil {
		t.Fatal("source cleanup started before destination publication")
	}
	if !repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason == recoveryCapacitySettled {
		t.Fatalf("unpublished destination released source hold: %+v", repo.moves[0].Status)
	}
	if _, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
		t.Fatalf("unpublished destination created cleanup intent: %v", err)
	}

	state.PublishedNodes = []string{"destination"}
	repo.volumes[move.Spec.VolumeID] = state
	move = repo.moves[0]
	destination := *move.Status.DestinationCopy
	repo.readyPoolsConfigured = true
	repo.readyPools = []volumeapi.Pool{{
		Name: destination.PoolName, UID: destination.PoolUID, NodeName: destination.NodeName, MountPath: "/destination",
		Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{
			Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: false}},
		}},
	}}
	if err := r.reconcileRecovery(context.Background(), move); err == nil {
		t.Fatal("source cleanup started before destination scanner publication proof")
	}
	if !repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason == recoveryCapacitySettled {
		t.Fatalf("unproven destination released source hold: %+v", repo.moves[0].Status)
	}
	repo.readyPools[0].Status.Inventory.Copies[0].Published = true
	if err := r.reconcileRecovery(context.Background(), move); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
		t.Fatalf("published destination did not settle source hold: %+v", repo.moves[0].Status)
	}
	cleanup, err := r.Cleanups.Get(context.Background(), cleanupapiAuthority(move))
	if err != nil || cleanup.Spec.Reason != "MoveSource" || cleanup.Spec.Target != *move.Status.SourceCopy || cleanup.Status.Phase != "Completed" {
		t.Fatalf("postcommit cleanup=%#v err=%v", cleanup, err)
	}
}

// TestPostcommitScannerPublicationWaitCarriesNoNilCause pins the operator-facing
// text of the ordinary wait where the destination Pool reads back fine but the
// scanner has not published the copy yet. That branch has no cause to report,
// so wrapping a nil error there would surface %!w(<nil>) in the durable
// recovery message that operators read and grep.
func TestPostcommitScannerPublicationWaitCarriesNoNilCause(t *testing.T) {
	r, repo, _ := recoveryFixture(t, "destination")
	move := repo.moves[0]
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "destination", recoveryRetiring
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	state.Phase = volumeapi.PhaseReady
	state.PublishedNodes = []string{"destination"}
	repo.volumes[move.Spec.VolumeID] = state

	destination := *move.Status.DestinationCopy
	repo.readyPoolsConfigured = true
	repo.readyPools = []volumeapi.Pool{{
		Name: destination.PoolName, UID: destination.PoolUID, NodeName: destination.NodeName, MountPath: "/destination",
		Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{
			Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: false}},
		}},
	}}

	err := r.reconcileRecovery(context.Background(), repo.moves[0])
	if err == nil {
		t.Fatal("source cleanup started before destination scanner publication proof")
	}
	message := repo.moves[0].Status.RecoveryMessage
	for name, text := range map[string]string{"returned error": err.Error(), "recovery message": message} {
		if strings.Contains(text, "%!w") || strings.Contains(text, "<nil>") {
			t.Fatalf("%s formats a nil cause: %q", name, text)
		}
		if !strings.Contains(text, "waiting for exact destination scanner publication before source cleanup") {
			t.Fatalf("%s lost the operator-visible wait prefix: %q", name, text)
		}
	}
	if strings.HasSuffix(message, ":") || strings.HasSuffix(message, ": ") {
		t.Fatalf("recovery message keeps a dangling cause separator: %q", message)
	}
}

func TestRecoveredMoveReleasesFinalizerOnlyAfterCapacitySettlement(t *testing.T) {
	r, repo, _ := recoveryFixture(t, "source")
	move := repo.moves[0]
	move.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "source", recoveryResuming
	move.Status.CapacityReason = recoveryCapacitySettled
	repo.moves[0] = move

	if err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := repo.volumes[move.Spec.VolumeID]
	state.PublishedNodes = []string{"source"}
	repo.volumes[move.Spec.VolumeID] = state
	for cycle := 0; cycle < 4; cycle++ {
		if err := r.ReconcileAll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	got := repo.moves[0]
	if got.Status.RecoveryPhase != recoveryRecovered || repo.volumes[move.Spec.VolumeID].ActiveMove != "" ||
		slices.Contains(got.Finalizers, volumeapi.MoveProtectionFinalizer) || !volumeapi.MoveCleanupSettled(got) {
		t.Fatalf("recovery did not terminally settle: move=%+v volume=%+v", got, repo.volumes[move.Spec.VolumeID])
	}
	reserved, err := poolcapacity.ReservedBytes(repo.volumes, repo.moves, "destination")
	if err != nil || reserved != 0 {
		t.Fatalf("settled recovery retained destination capacity: reserved=%d err=%v", reserved, err)
	}

	unsafe := got
	unsafe.Finalizers = []string{volumeapi.MoveProtectionFinalizer}
	unsafe.Status.CapacityApproved = true
	unsafe.Status.CapacityReason = ""
	repo.moves[0] = unsafe
	if err := r.ReconcileAll(context.Background()); err == nil {
		t.Fatal("Recovered without capacity settlement was accepted")
	}
	if !slices.Contains(repo.moves[0].Finalizers, volumeapi.MoveProtectionFinalizer) {
		t.Fatal("unsafe Recovered move lost its finalizer")
	}
}

func cleanupapiAuthority(move volumeapi.Move) cleanupapi.Authority {
	return cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}
}

func rollbackCleanupSpec(move volumeapi.Move, target volume.CopyIdentity) cleanupapi.Spec {
	return cleanupapi.Spec{
		OperationID: volumeapi.MoveRollbackOperationID(move.UID),
		Target:      target,
		Reason:      "MoveRollback",
		Authority:   cleanupapiAuthority(move),
	}
}

// seedConfirmingAbsenceJournal reproduces the durable shape a 0.4.1 cluster is
// stuck in: the exact intent, an executor, a retired and purged API receipt, and
// a requested post-receipt scan fence that nothing has confirmed yet.
func seedConfirmingAbsenceJournal(t *testing.T, r *Reconciler, spec cleanupapi.Spec) {
	t.Helper()
	ctx := context.Background()
	seedWorkingJournal(t, r, spec, cleanupapi.PhaseRunning)
	request, err := r.Cleanups.Get(ctx, spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	// Write the receipt through the store rather than an operator stub, so a
	// recording operator's call count stays a clean signal about the code path
	// under test.
	if err := r.Cleanups.UpdateStatus(ctx, request, cleanupapi.Status{
		Phase: cleanupapi.PhaseVerifying, Executor: request.Status.Executor,
		Receipt: &cleanupapi.Receipt{
			OperationID: spec.OperationID, ExecutorUID: request.Status.Executor.JobUID,
			ObservedAt: r.now().Format(time.RFC3339Nano), Retired: true, Purged: true,
			LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if request, err = r.Cleanups.Get(ctx, spec.Authority); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Cleanups.ReconcileAbsence(ctx, request); err != nil {
		t.Fatal(err)
	}
	seeded, err := r.Cleanups.Get(ctx, spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if seeded.Status.Phase != cleanupapi.PhaseConfirmingAbsence || seeded.Status.Receipt == nil ||
		seeded.Status.AbsenceProof == nil || seeded.Status.AbsenceProof.ConfirmedAt != "" {
		t.Fatalf("seed is not a requested-but-unconfirmed absence fence: %#v", seeded.Status)
	}
}

// seedWorkingJournal records an exact intent that never reached a purge
// receipt, which is the shape a terminal Move must never execute from.
func seedWorkingJournal(t *testing.T, r *Reconciler, spec cleanupapi.Spec, phase string) {
	t.Helper()
	ctx := context.Background()
	request, err := r.Cleanups.Ensure(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if phase == cleanupapi.PhasePending {
		return
	}
	if err := r.Cleanups.UpdateStatus(ctx, request, cleanupapi.Status{Phase: phase, Executor: &cleanupapi.Executor{
		JobName: request.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: spec.Target.NodeName,
	}}); err != nil {
		t.Fatal(err)
	}
}

// listedJournal reads the embedded journal the way the uninstall checker does,
// which does not require the parent to still be protected.
func listedJournal(t *testing.T, r *Reconciler, move volumeapi.Move) cleanupapi.Cleanup {
	t.Helper()
	cleanups, err := r.Cleanups.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, cleanup := range cleanups {
		if cleanup.Spec.Authority == cleanupapiAuthority(move) {
			return cleanup
		}
	}
	t.Fatalf("no cleanup journal is embedded on move %q", move.Name)
	return cleanupapi.Cleanup{}
}

// cleanupPoolObject reaches the Pool the journal store fences against.
func cleanupPoolObject(t *testing.T, store *cleanupapi.Store, poolName string) (*unstructured.Unstructured, dynamic.ResourceInterface) {
	t.Helper()
	pools := store.Client.Resource(volumeapi.PoolResource)
	pool, err := pools.Get(context.Background(), poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pool, pools
}

// settledTerminalMoveFixture builds the exact 0.4.1 shape: a terminal Move whose
// capacity is already released, whose volume no longer references it, and whose
// executor requests are recorded rather than fabricated. Protection is still
// present so a journal can be seeded; stripMoveProtection removes it afterwards.
func settledTerminalMoveFixture(t *testing.T, phase, recovery, capacityReason string) (*Reconciler, *memoryRepository, *recordingCleanupOperator, volumeapi.Move, volume.CopyIdentity) {
	t.Helper()
	r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
	operator := &recordingCleanupOperator{}
	withRecordingCleanupOperator(operator)(r)
	repo.journalParents = r.Cleanups.Client
	move := repo.moves[0]
	move.Status.Phase, move.Status.RecoveryPhase = phase, recovery
	move.Status.CapacityApproved, move.Status.CapacityReason = false, capacityReason
	move.Status.LastTransitionTime = r.now().Format(time.RFC3339Nano)
	repo.moves[0] = move
	state := repo.volumes[move.Spec.VolumeID]
	state.Phase, state.ActiveMove = volumeapi.PhaseReady, ""
	repo.volumes[move.Spec.VolumeID] = state
	return r, repo, operator, move, incoming
}

// stripMoveProtection reproduces the 0.4.1 release of the protection finalizer
// while the journal was still mid-flight, which is what made it unreachable.
func stripMoveProtection(t *testing.T, repo *memoryRepository, move volumeapi.Move) {
	t.Helper()
	if err := repo.RemoveMoveFinalizer(context.Background(), move.Name, move.UID); err != nil {
		t.Fatal(err)
	}
}

// reconcileUntilQuiet runs the whole loop repeatedly and returns the reconcile
// errors it reported, so a test can pin both the durable outcome and the single
// operator-visible message that announced it.
func reconcileUntilQuiet(t *testing.T, r *Reconciler, cycles int) []string {
	t.Helper()
	reported := []string{}
	for cycle := 0; cycle < cycles; cycle++ {
		if err := r.ReconcileAll(context.Background()); err != nil {
			reported = append(reported, err.Error())
		}
	}
	return reported
}

// staleDestinationScan makes the fenced Pool report a superseded observation, so
// the absence fence cannot be proven on this pass.
func staleDestinationScan(t *testing.T, store *cleanupapi.Store, poolName string) {
	t.Helper()
	pools := store.Client.Resource(volumeapi.PoolResource)
	pool, err := pools.Get(context.Background(), poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(pool.Object, pool.GetGeneration()-1, "status", "observedGeneration"); err != nil {
		t.Fatal(err)
	}
	if _, err := pools.UpdateStatus(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryRollbackWaitsForItsOwnJournalAfterTheArtifactIsGone pins the 0.4.1
// production failure. The rollback purge already ran, so the destination
// inventory no longer shows the artifact, but the journal is still at
// ConfirmingAbsence holding only a requested fence. Absence of the artifact
// proves only that the effect happened; the journal still owns the receipt and
// the post-receipt absence proof. Declaring settlement there releases the hold
// with RecoverySettled, and ReconcileAll never hands a Recovered Move back to
// recovery, so the journal would stay unfinished for the life of the cluster.
func TestRecoveryRollbackWaitsForItsOwnJournalAfterTheArtifactIsGone(t *testing.T) {
	ctx := context.Background()

	t.Run("unfinished fence keeps the hold", func(t *testing.T) {
		r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
		move := repo.moves[0]
		seedConfirmingAbsenceJournal(t, r, rollbackCleanupSpec(move, incoming))
		staleDestinationScan(t, r.Cleanups, incoming.PoolName)

		state, err := repo.Get(ctx, move.Spec.VolumeID)
		if err != nil {
			t.Fatal(err)
		}
		if done, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil || done {
			t.Fatalf("unproven absence fence settled: done=%v err=%v", done, err)
		}
		if !repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason == recoveryCapacitySettled {
			t.Fatalf("unfinished rollback journal released the hold: %+v", repo.moves[0].Status)
		}
	})

	t.Run("journal is driven to Completed", func(t *testing.T) {
		r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
		move := repo.moves[0]
		seedConfirmingAbsenceJournal(t, r, rollbackCleanupSpec(move, incoming))

		state, err := repo.Get(ctx, move.Spec.VolumeID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil {
			t.Fatal(err)
		}
		journal, err := r.Cleanups.Get(ctx, cleanupapiAuthority(move))
		if err != nil || journal.Status.Phase != cleanupapi.PhaseCompleted {
			t.Fatalf("settlement left the rollback journal unfinished: phase=%q err=%v", journal.Status.Phase, err)
		}
		if repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
			t.Fatalf("completed rollback journal did not settle the hold: %+v", repo.moves[0].Status)
		}
	})
}

// TestSettledTerminalMoveStillFinishesItsCleanupJournal covers the upgrade path
// for clusters already carrying the stuck journals: the Move is settled, its
// capacity is released and its protection finalizer is gone, yet status.cleanup
// is mid-flight. ReconcileAll must finish that journal instead of parking the
// Move for GC, without re-approving capacity or starting any executor.
func TestSettledTerminalMoveStillFinishesItsCleanupJournal(t *testing.T) {
	for _, scenario := range []struct{ name, phase, recovery, capacityReason string }{
		{"rollback journal on a settled Blocked move", "Blocked", recoveryRecovered, recoveryCapacitySettled},
		{"source journal on a Succeeded move", "Succeeded", "", ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			r, repo, operator, move, incoming := settledTerminalMoveFixture(t, scenario.phase, scenario.recovery, scenario.capacityReason)
			spec := rollbackCleanupSpec(move, incoming)
			if scenario.phase == "Succeeded" {
				var err error
				if spec, err = moveCleanupSpec(move); err != nil {
					t.Fatal(err)
				}
			}
			seedConfirmingAbsenceJournal(t, r, spec)
			stripMoveProtection(t, repo, move)
			state := repo.volumes[move.Spec.VolumeID]

			if reported := reconcileUntilQuiet(t, r, 4); len(reported) != 0 {
				t.Fatalf("self-heal reported errors: %v", reported)
			}

			// Read the journal the way the uninstall checker does: Store.Get
			// requires a protected parent, and protection is released again
			// exactly when the journal settles.
			if phase := listedJournal(t, r, move).Status.Phase; phase != cleanupapi.PhaseCompleted {
				t.Fatalf("settled move never finished its journal: phase=%q", phase)
			}
			listed, err := repo.ListMoves(ctx)
			if err != nil || len(listed) != 1 {
				t.Fatalf("self-heal changed the journal set: %+v err=%v", listed, err)
			}
			got := listed[0]
			if got.Status.Phase != scenario.phase || got.Status.RecoveryPhase != scenario.recovery ||
				got.Status.CapacityApproved || got.Status.CapacityReason != scenario.capacityReason {
				t.Fatalf("self-heal changed the settled move: %+v", got.Status)
			}
			if slices.Contains(got.Finalizers, volumeapi.MoveProtectionFinalizer) {
				t.Fatalf("protection was not released after the journal settled: %v", got.Finalizers)
			}
			if !volumeapi.MoveCleanupSettled(got) {
				t.Fatalf("self-healed move is not settled: %+v", got.Status)
			}
			if !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
				t.Fatalf("self-heal touched the volume: %+v", repo.volumes[move.Spec.VolumeID])
			}
			reserved, err := poolcapacity.ReservedBytes(repo.volumes, repo.moves, "destination")
			if err != nil || reserved != 0 {
				t.Fatalf("self-heal re-reserved destination capacity: reserved=%d err=%v", reserved, err)
			}
			if operator.reclaims != 0 {
				t.Fatalf("self-heal requested %d cleanup executors", operator.reclaims)
			}
		})
	}
}

// TestTerminalMoveNeverExecutesAnIntentWithoutAReceipt pins the authority
// boundary of the terminal arm. A settled Move has already released its capacity
// hold, so a journal that never reached a purge receipt has no live authority to
// order one: driving it into Reclaim would create a real helper Job and block the
// loop on --helper-timeout. Missing intent preserves data instead.
func TestTerminalMoveNeverExecutesAnIntentWithoutAReceipt(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhasePending, cleanupapi.PhaseRunning} {
		t.Run(phase, func(t *testing.T) {
			r, repo, operator, move, incoming := settledTerminalMoveFixture(t, "Blocked", recoveryRecovered, recoveryCapacitySettled)
			seedWorkingJournal(t, r, rollbackCleanupSpec(move, incoming), phase)
			stripMoveProtection(t, repo, move)

			reported := reconcileUntilQuiet(t, r, 4)
			journal := listedJournal(t, r, move)
			if journal.Status.Phase != cleanupapi.PhaseNeedsReview || journal.Status.Reason != "TerminalMoveCleanupIntentUnfinished" {
				t.Fatalf("receiptless intent on a settled move was not preserved for review: %+v", journal.Status)
			}
			if !strings.Contains(journal.Status.Message, phase) {
				t.Fatalf("review message does not name the unfinished phase: %q", journal.Status.Message)
			}
			if operator.reclaims != 0 {
				t.Fatalf("terminal cleanup requested %d executors", operator.reclaims)
			}
			if len(reported) != 1 || !strings.Contains(reported[0], "TerminalMoveCleanupIntentUnfinished") {
				t.Fatalf("contradiction was not announced exactly once: %v", reported)
			}
			assertMoveUnpinned(t, repo, move)
		})
	}
}

// TestTerminalMoveSendsALostCleanupPoolToReview covers the other half: the
// journal does hold a receipt, but the Pool its absence fence is anchored to has
// been re-registered or removed. No retry can restore that identity, so it is a
// contradiction to record on the journal rather than an error to log forever
// while the re-asserted finalizer pins the Move.
func TestTerminalMoveSendsALostCleanupPoolToReview(t *testing.T) {
	for _, scenario := range []string{"pool re-registered", "pool deleted"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, operator, move, incoming := settledTerminalMoveFixture(t, "Blocked", recoveryRecovered, recoveryCapacitySettled)
			seedConfirmingAbsenceJournal(t, r, rollbackCleanupSpec(move, incoming))
			pool, pools := cleanupPoolObject(t, r.Cleanups, incoming.PoolName)
			if scenario == "pool deleted" {
				if err := pools.Delete(ctx, pool.GetName(), metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				pool.SetUID("reinstalled-pool-uid")
				if _, err := pools.Update(ctx, pool, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			stripMoveProtection(t, repo, move)

			reported := reconcileUntilQuiet(t, r, 4)
			journal := listedJournal(t, r, move)
			if journal.Status.Phase != cleanupapi.PhaseNeedsReview || journal.Status.Reason != "CleanupPoolIdentityChanged" {
				t.Fatalf("lost cleanup Pool was not recorded as a contradiction: %+v", journal.Status)
			}
			if journal.Status.Receipt == nil || journal.Status.AbsenceProof == nil {
				t.Fatalf("review erased the receipt or the absence fence: %+v", journal.Status)
			}
			if operator.reclaims != 0 {
				t.Fatalf("lost cleanup Pool requested %d executors", operator.reclaims)
			}
			if len(reported) != 1 || !strings.Contains(reported[0], "CleanupPoolIdentityChanged") {
				t.Fatalf("contradiction was not announced exactly once: %v", reported)
			}
			assertMoveUnpinned(t, repo, move)
		})
	}
}

// TestTerminalCleanupCannotProtectADeletingMove keeps the self-heal path
// fail-closed. Protection can never be added back to an object the API server is
// already deleting, so the journal is left exactly as it stands and the
// contradiction is reported instead of being worked around.
func TestTerminalCleanupCannotProtectADeletingMove(t *testing.T) {
	r, repo, operator, move, incoming := settledTerminalMoveFixture(t, "Blocked", recoveryRecovered, recoveryCapacitySettled)
	seedConfirmingAbsenceJournal(t, r, rollbackCleanupSpec(move, incoming))
	stripMoveProtection(t, repo, move)
	repo.deletingMoves = map[string]bool{move.Name: true}

	reported := reconcileUntilQuiet(t, r, 3)
	if len(reported) != 3 || !strings.Contains(reported[0], "already deleting") {
		t.Fatalf("deleting move was not reported fail-closed: %v", reported)
	}
	if phase := listedJournal(t, r, move).Status.Phase; phase != cleanupapi.PhaseConfirmingAbsence {
		t.Fatalf("deleting move changed its journal: phase=%q", phase)
	}
	if operator.reclaims != 0 {
		t.Fatalf("deleting move requested %d executors", operator.reclaims)
	}
}

// assertMoveUnpinned proves the re-asserted protection is released again once
// the journal reaches a terminal phase, so review never pins the Move forever.
func assertMoveUnpinned(t *testing.T, repo *memoryRepository, move volumeapi.Move) {
	t.Helper()
	listed, err := repo.ListMoves(context.Background())
	if err != nil || len(listed) != 1 {
		t.Fatalf("moves=%+v err=%v", listed, err)
	}
	if slices.Contains(listed[0].Finalizers, volumeapi.MoveProtectionFinalizer) {
		t.Fatalf("review pinned the move with protection: %v", listed[0].Finalizers)
	}
}
