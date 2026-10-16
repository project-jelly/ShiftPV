package cleanupapi

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	testVolumeID      = "shiftpv-0123456789abcdef0123456789abcdef"
	testReceiptDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func TestEnsureEmbedsJournalOnExactParent(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	parent.Object["status"] = map[string]any{"ownerNode": "worker-a"}
	store, client := testStore(parent)
	spec := testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete")

	created, err := store.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if created.Name != cleanupName(spec.Target) || created.UID != spec.Authority.UID || created.Status.Phase != PhasePending {
		t.Fatalf("created=%#v", created)
	}
	object, err := client.Resource(volumeapi.VolumeResource).Get(context.Background(), testVolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if owner, _, _ := unstructured.NestedString(object.Object, "status", "ownerNode"); owner != "worker-a" {
		t.Fatalf("unrelated parent status was overwritten: %q", owner)
	}
	if _, found, err := unstructured.NestedMap(object.Object, "status", "cleanup"); err != nil || !found {
		t.Fatalf("embedded journal missing: found=%v err=%v", found, err)
	}
	resumed, err := store.Ensure(context.Background(), spec)
	if err != nil || resumed.Spec != created.Spec {
		t.Fatalf("idempotent ensure=%#v err=%v", resumed, err)
	}
	changed := spec
	changed.OperationID = "delete-other"
	if _, err := store.Ensure(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed intent error=%v", err)
	}
}

func TestJournalRequiresExactProtectedParentEvenWhileTerminating(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	now := metav1.NewTime(time.Unix(1_700_000_000, 0).UTC())
	parent.SetDeletionTimestamp(&now)
	store, client := testStore(parent)
	spec := testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete")
	created, err := store.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatalf("terminating protected parent should remain writable: %v", err)
	}
	if _, err := store.Get(context.Background(), Authority{Kind: "ShiftPVVolume", Name: testVolumeID, UID: "recreated-uid"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("recreated parent error=%v", err)
	}
	object, err := client.Resource(volumeapi.VolumeResource).Get(context.Background(), testVolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetFinalizers(nil)
	if _, err := client.Resource(volumeapi.VolumeResource).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), created.Spec.Authority); !errors.Is(err, ErrConflict) {
		t.Fatalf("unprotected terminating parent error=%v", err)
	}
}

func TestTransitionRequiresReceiptThenFreshAbsence(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	store, _ := testStore(parent)
	cleanup, err := store.Ensure(context.Background(), testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete"))
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", NodeName: "worker-a"}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	unboundReceipt := &Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: testNow(), Retired: true, Purged: true, LocalReceiptDigest: testReceiptDigest}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseVerifying, Executor: executor, Receipt: unboundReceipt}); !errors.Is(err, ErrConflict) {
		t.Fatalf("unbound executor entered Verifying: %v", err)
	}
	podBound := *executor
	podBound.PodUID = "pod-uid"
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: &podBound}); err != nil {
		t.Fatalf("bind Pod UID: %v", err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	changedPod := podBound
	changedPod.PodUID = "replacement-pod"
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: &changedPod}); err != nil {
		t.Fatalf("rebind same-Job retry Pod: %v", err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	stalePodReceipt := &Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: testNow(), Retired: true, Purged: true, LocalReceiptDigest: testReceiptDigest}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseVerifying, Executor: executor, Receipt: stalePodReceipt}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Pod wrote receipt after retry rebind: %v", err)
	}
	changedJob := changedPod
	changedJob.JobUID = "replacement-job"
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: &changedJob}); !errors.Is(err, ErrConflict) {
		t.Fatalf("replacement Job error=%v", err)
	}
	executor = &changedPod
	missingDigest := &Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: testNow(), Retired: true, Purged: true}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseVerifying, Executor: executor, Receipt: missingDigest}); !errors.Is(err, ErrConflict) {
		t.Fatalf("digestless receipt entered Verifying: %v", err)
	}
	receipt := &Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: testNow(), Retired: true, Purged: true, LocalReceiptDigest: testReceiptDigest}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseCompleted, Executor: executor, Receipt: receipt, SettledAt: testNow()}); !errors.Is(err, ErrConflict) {
		t.Fatalf("receipt-only completion error=%v", err)
	}
	proof := &AbsenceProof{RequestID: "scan-request", PoolName: cleanup.Spec.Target.PoolName, PoolUID: cleanup.Spec.Target.PoolUID, RequiredGeneration: 4}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseConfirmingAbsence, Executor: executor, Receipt: receipt, AbsenceProof: proof}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseCompleted, Executor: executor, Receipt: receipt, AbsenceProof: proof, SettledAt: testNow()}); !errors.Is(err, ErrConflict) {
		t.Fatalf("unconfirmed absence error=%v", err)
	}
	confirmed := *proof
	confirmed.ObservedGeneration = 4
	confirmed.Valid, confirmed.Complete, confirmed.Absent, confirmed.ConfirmedAt = true, true, true, testNow()
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseCompleted, Executor: executor, Receipt: receipt, AbsenceProof: &confirmed, SettledAt: testNow()}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	if err := store.UpdateStatus(context.Background(), cleanup, Status{
		Phase: PhaseNeedsReview, Reason: "CopyReappeared", Executor: executor, Receipt: receipt,
		AbsenceProof: &confirmed, SettledAt: cleanup.Status.SettledAt,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Completed journal reopened: %v", err)
	}
}

func TestReconcileAbsenceUsesPostReceiptPoolGeneration(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	pool := testPool(3, 3, true, false, "", []any{copyObservationMap(testCopy(), true)})
	store, client := testStore(parent, pool)
	client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		update := action.(ktesting.UpdateAction)
		if update.GetSubresource() == "" {
			object := update.GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	cleanup := verifyingCleanup(t, store)
	confirming, complete, err := store.ReconcileAbsence(context.Background(), cleanup)
	if err != nil || complete || confirming.Status.Phase != PhaseConfirmingAbsence || confirming.Status.AbsenceProof.RequiredGeneration != 4 {
		t.Fatalf("confirming=%#v complete=%v err=%v", confirming, complete, err)
	}
	if _, complete, err = store.ReconcileAbsence(context.Background(), confirming); err != nil || complete {
		t.Fatalf("stale observation completed=%v err=%v", complete, err)
	}
	updatePoolStatus(t, client, func(pool *unstructured.Unstructured) error {
		if err := unstructured.SetNestedField(pool.Object, int64(4), "status", "observedGeneration"); err != nil {
			return err
		}
		return unstructured.SetNestedSlice(pool.Object, []any{copyObservationMap(testCopy(), true)}, "status", "inventory", "copies")
	})
	if _, complete, err = store.ReconcileAbsence(context.Background(), confirming); err != nil || complete {
		t.Fatalf("present target completed=%v err=%v", complete, err)
	}
	for name, observations := range map[string][]any{
		"per-copy problem":          {map[string]any{"marker": "path:volumes/unknown", "present": true, "problem": "UnrecordedPath"}},
		"identityless present copy": {map[string]any{"marker": "path:volumes/unknown", "present": true}},
		"published absent copy": {func() map[string]any {
			observation := copyObservationMap(testCopy(), false)
			observation["published"] = true
			return observation
		}()},
		"foreign pool identity": {func() map[string]any {
			foreign := testCopy()
			foreign.PoolUID = "foreign-pool-uid"
			return copyObservationMap(foreign, false)
		}()},
	} {
		t.Run(name, func(t *testing.T) {
			setPoolCopies(t, client, observations)
			if _, complete, err := store.ReconcileAbsence(context.Background(), confirming); err != nil || complete {
				t.Fatalf("ambiguous inventory completed=%v err=%v", complete, err)
			}
		})
	}
	setPoolCopies(t, client, []any{copyObservationMap(testCopy(), false)})
	settled, complete, err := store.ReconcileAbsence(context.Background(), confirming)
	if err != nil || !complete || settled.Status.Phase != PhaseCompleted || settled.Status.AbsenceProof == nil || !settled.Status.AbsenceProof.Absent {
		t.Fatalf("settled=%#v complete=%v err=%v", settled, complete, err)
	}
}

// TestUpdateStatusKeepsResolvedObservationAcrossConflictRetries pins that a
// conflict retry republishes the observation the FIRST attempt resolved.
//
// UpdateStatus fills the observation fields the caller left open once, and
// carries that resolved status across retries. Re-resolving them on every
// attempt would publish an observedGeneration the caller never observed and a
// lastTransitionTime later than the transition it describes, which is a silent
// change to what the journal means rather than to whether the write succeeds.
func TestUpdateStatusKeepsResolvedObservationAcrossConflictRetries(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		conflicts int
	}{
		{"one conflict", 1},
		{"two conflicts", 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
			store, client := testStore(parent)
			// An advancing clock and a generation that moves between attempts are
			// what make a re-resolved observation observable at all.
			readings := 0
			store.Now = func() time.Time {
				readings++
				return time.Unix(1_700_000_100+int64(readings), 0).UTC()
			}
			cleanup, err := store.Ensure(context.Background(), testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete"))
			if err != nil {
				t.Fatal(err)
			}
			firstGeneration := currentParentGeneration(t, client)
			wantTime := time.Unix(1_700_000_100+int64(readings)+1, 0).UTC().Format(time.RFC3339Nano)

			remaining := testCase.conflicts
			client.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.(ktesting.UpdateAction).GetSubresource() != "status" || remaining <= 0 {
					return false, nil, nil
				}
				remaining--
				bumpParentGeneration(t, client)
				return true, nil, apierrors.NewConflict(
					schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvvolumes"}, testVolumeID, errors.New("injected"))
			})

			executor := &Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", NodeName: "worker-a"}
			before := readings
			if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: executor}); err != nil {
				t.Fatalf("retried update: %v", err)
			}
			if remaining != 0 {
				t.Fatalf("%d injected conflicts were never consumed", remaining)
			}
			settled, err := store.Get(context.Background(), cleanup.Spec.Authority)
			if err != nil {
				t.Fatal(err)
			}
			if settled.Status.ObservedGeneration != firstGeneration {
				t.Fatalf("republished observedGeneration = %d, want the first attempt's %d",
					settled.Status.ObservedGeneration, firstGeneration)
			}
			if settled.Status.LastTransitionTime != wantTime {
				t.Fatalf("republished lastTransitionTime = %q, want the first attempt's %q",
					settled.Status.LastTransitionTime, wantTime)
			}
			if used := readings - before; used != 1 {
				t.Fatalf("clock was read %d times across %d conflicts, want exactly 1", used, testCase.conflicts)
			}
			if later := currentParentGeneration(t, client); later == firstGeneration {
				t.Fatalf("fixture never moved the generation between attempts: still %d", later)
			}
		})
	}
}

func currentParentGeneration(t *testing.T, client *fake.FakeDynamicClient) int64 {
	t.Helper()
	object, err := client.Resource(volumeapi.VolumeResource).Get(context.Background(), testVolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return object.GetGeneration()
}

// bumpParentGeneration moves the parent generation without going through the
// reactor chain, so the next attempt reads a generation the first never saw.
func bumpParentGeneration(t *testing.T, client *fake.FakeDynamicClient) {
	t.Helper()
	object, err := client.Tracker().Get(volumeapi.VolumeResource, "", testVolumeID)
	if err != nil {
		t.Fatal(err)
	}
	typed, ok := object.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("parent is %T, want *unstructured.Unstructured", object)
	}
	typed.SetGeneration(typed.GetGeneration() + 1)
	if err := client.Tracker().Update(volumeapi.VolumeResource, typed, ""); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileAbsenceRejectsLegacyVerifyingWithoutPodBinding(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	store, client := testStore(parent)
	cleanup, err := store.Ensure(context.Background(), testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete"))
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", NodeName: "worker-a"}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	object, err := client.Resource(volumeapi.VolumeResource).Get(context.Background(), testVolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stored, found, err := unstructured.NestedMap(object.Object, "status", "cleanup")
	if err != nil || !found {
		t.Fatalf("cleanup journal missing: found=%t err=%v", found, err)
	}
	status := stored["status"].(map[string]any)
	status["phase"] = PhaseVerifying
	status["receipt"] = map[string]any{
		"operationID": cleanup.Spec.OperationID, "executorUID": executor.JobUID, "observedAt": testNow(),
		"retired": true, "purged": true, "localReceiptDigest": testReceiptDigest,
	}
	if err := unstructured.SetNestedMap(object.Object, stored, "status", "cleanup"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(volumeapi.VolumeResource).UpdateStatus(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	legacy, err := store.Get(context.Background(), cleanup.Spec.Authority)
	if err != nil || legacy.Status.Phase != PhaseVerifying || legacy.Status.Executor == nil || legacy.Status.Executor.PodUID != "" {
		t.Fatalf("legacy journal=%#v err=%v", legacy, err)
	}
	if _, completed, err := store.ReconcileAbsence(context.Background(), legacy); !errors.Is(err, ErrConflict) || completed {
		t.Fatalf("legacy journal completed=%t err=%v", completed, err)
	}
	unchanged, err := store.Get(context.Background(), cleanup.Spec.Authority)
	if err != nil || unchanged.Status.Phase != PhaseVerifying || unchanged.Status.AbsenceProof != nil {
		t.Fatalf("legacy journal advanced: %#v err=%v", unchanged.Status, err)
	}
}

func TestListScansVolumeAndMoveParents(t *testing.T) {
	volumeParent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	moveParent := testParent("ShiftPVMove", "move-a", "move-uid", volumeapi.MoveProtectionFinalizer)
	store, _ := testStore(volumeParent, moveParent)
	volumeCleanup, err := store.Ensure(context.Background(), testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete"))
	if err != nil {
		t.Fatal(err)
	}
	moveSpec := testSpec("ShiftPVMove", "move-a", "move-uid", "MoveSource")
	moveCleanup, err := store.Ensure(context.Background(), moveSpec)
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.ListForVolume(context.Background(), testVolumeID)
	if err != nil || len(items) != 2 {
		t.Fatalf("items=%#v err=%v", items, err)
	}
	if items[0].Name != volumeCleanup.Name || items[1].Name != moveCleanup.Name {
		t.Fatalf("unexpected items=%#v", items)
	}
	all, err := store.List(context.Background())
	if err != nil || len(all) != 2 {
		t.Fatalf("all=%#v err=%v", all, err)
	}
}

func TestSpecRejectsOrphanAuthority(t *testing.T) {
	spec := testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "OrphanReclaim")
	spec.Authority = Authority{Kind: "Namespace", Name: "kube-system", UID: "installation-a"}
	if err := spec.Validate(); err == nil {
		t.Fatal("orphan cleanup unexpectedly validated")
	}
}

func TestSpecAcceptsMoveRollbackOnlyOnMoveParent(t *testing.T) {
	spec := testSpec("ShiftPVMove", "move-a", "move-uid", "MoveRollback")
	if err := spec.Validate(); err != nil {
		t.Fatalf("Move rollback rejected: %v", err)
	}
	spec.Authority = Authority{Kind: "ShiftPVVolume", Name: testVolumeID, UID: "volume-uid"}
	if err := spec.Validate(); err == nil {
		t.Fatal("Move rollback accepted a Volume parent")
	}
}

// verifyingCleanup drives a fresh journal to Verifying behind a Pod-bound
// executor and its API receipt, the precondition absence reconciliation needs.
func verifyingCleanup(t *testing.T, store *Store) Cleanup {
	t.Helper()
	cleanup, err := store.Ensure(context.Background(), testSpec("ShiftPVVolume", testVolumeID, "volume-uid", "VolumeDelete"))
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{JobName: cleanup.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: "worker-a"}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseRunning, Executor: executor}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	receipt := &Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.JobUID, ObservedAt: testNow(), Retired: true, Purged: true, LocalReceiptDigest: testReceiptDigest}
	if err := store.UpdateStatus(context.Background(), cleanup, Status{Phase: PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	cleanup, _ = store.Get(context.Background(), cleanup.Spec.Authority)
	return cleanup
}

func updatePoolStatus(t *testing.T, client *fake.FakeDynamicClient, mutate func(*unstructured.Unstructured) error) {
	t.Helper()
	pool, err := client.Resource(volumeapi.PoolResource).Get(context.Background(), "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := mutate(pool); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(volumeapi.PoolResource).UpdateStatus(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func setPoolCopies(t *testing.T, client *fake.FakeDynamicClient, observations []any) {
	t.Helper()
	updatePoolStatus(t, client, func(pool *unstructured.Unstructured) error {
		return unstructured.SetNestedSlice(pool.Object, observations, "status", "inventory", "copies")
	})
}

func testStore(objects ...runtime.Object) (*Store, *fake.FakeDynamicClient) {
	listKinds := map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList",
		volumeapi.MoveResource:   "ShiftPVMoveList",
		volumeapi.PoolResource:   "ShiftPVPoolList",
	}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)
	return &Store{Client: client, Now: func() time.Time { return time.Unix(1_700_000_100, 0).UTC() }}, client
}

func testParent(kind, name, uid, finalizer string) *unstructured.Unstructured {
	resourceVersion := "1"
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1",
		"kind":       kind,
		"metadata": map[string]any{
			"name": name, "uid": uid, "resourceVersion": resourceVersion, "generation": int64(2), "finalizers": []any{finalizer},
		},
		"spec": map[string]any{},
	}}
}

func testPool(generation, observedGeneration int64, valid, truncated bool, message string, copies []any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{
			"name": "pool-a", "uid": "pool-uid", "resourceVersion": "1", "generation": generation, "finalizers": []any{volumeapi.PoolProtectionFinalizer},
		},
		"spec": map[string]any{"nodeName": "worker-a", "scanEpoch": int64(1)},
		"status": map[string]any{
			"observedGeneration": observedGeneration,
			"inventory":          map[string]any{"valid": valid, "truncated": truncated, "message": message, "copies": copies},
		},
	}}
}

func testSpec(kind, name, uid, reason string) Spec {
	return Spec{OperationID: "cleanup-operation", Target: testCopy(), Reason: reason, Authority: Authority{Kind: kind, Name: name, UID: uid}}
}

func testCopy() CopyIdentity {
	return CopyIdentity{
		InstallationID: "installation-a", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: testVolumeID,
		VolumeUID: "volume-uid", CopyID: "copy-a", NodeName: "worker-a", Role: volume.RoleServing,
	}
}

func copyObservationMap(identity CopyIdentity, present bool) map[string]any {
	encoded, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&identity)
	return map[string]any{"marker": "placement-" + identity.CopyID + ".json", "identity": encoded, "present": present}
}

func testNow() string { return time.Unix(1_700_000_200, 0).UTC().Format(time.RFC3339Nano) }

func TestGetHistoryReadsUnprotectedExactParentWithoutGrantingWrites(t *testing.T) {
	for _, kind := range []string{"ShiftPVVolume", "ShiftPVMove"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			name, uid, finalizer, reason := testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer, "VolumeDelete"
			resource := volumeapi.VolumeResource
			if kind == "ShiftPVMove" {
				name, uid, finalizer, reason = "move-history", "move-uid", volumeapi.MoveProtectionFinalizer, "MoveSource"
				resource = volumeapi.MoveResource
			}
			store, client := testStore(testParent(kind, name, uid, finalizer))
			expected, err := store.Ensure(ctx, testSpec(kind, name, uid, reason))
			if err != nil {
				t.Fatal(err)
			}
			parent, err := client.Resource(resource).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			parent.SetFinalizers(nil)
			if _, err := client.Resource(resource).Update(ctx, parent, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			history, err := store.GetHistory(ctx, expected.Spec.Authority)
			if err != nil || history.Spec != expected.Spec || history.UID != uid || history.Status.Phase != PhasePending {
				t.Fatalf("history=%+v err=%v", history, err)
			}
			if _, err := store.Get(ctx, expected.Spec.Authority); !errors.Is(err, ErrConflict) {
				t.Fatalf("unprotected execution read allowed: %v", err)
			}
			if err := store.UpdateStatus(ctx, history, history.Status); !errors.Is(err, ErrConflict) {
				t.Fatalf("history granted write authority: %v", err)
			}
			replaced := expected.Spec.Authority
			replaced.UID = "other-uid"
			if _, err := store.GetHistory(ctx, replaced); !errors.Is(err, ErrConflict) {
				t.Fatalf("replaced parent accepted: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("history path mutated: %+v", action)
				}
			}
		})
	}
}
