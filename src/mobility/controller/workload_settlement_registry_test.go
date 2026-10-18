package controller

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Replays the companion model's retirement trace through the actual Registry,
// embedded cleanup journal and reconciler. Only API storage, scanner and effect
// results are injected; this test does not establish real filesystem behavior.
func TestWorkloadSettlementRegistryTrace(t *testing.T) {
	for _, owner := range []string{"source", "destination"} {
		for _, policy := range []corev1.PersistentVolumeReclaimPolicy{corev1.PersistentVolumeReclaimDelete, corev1.PersistentVolumeReclaimRetain} {
			for _, replaced := range []bool{false, true} {
				claimEnd := "absent"
				if replaced {
					claimEnd = "replaced"
				}
				for _, lostBoundary := range []string{"Volume fence", "Move intent", "final unlock"} {
					t.Run(owner+"/"+string(policy)+"/"+claimEnd+"/"+lostBoundary, func(t *testing.T) {
						f, original, replacement := retirementRegistryFixture(t, owner, policy, replaced)
						operator := &retirementEffectStub{}
						lost, sawLoss := false, false
						f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
							if lost {
								return nil
							}
							phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
							intent, _, _ := unstructured.NestedBool(object.Object, "status", "settlementRequested")
							active, _, _ := unstructured.NestedString(object.Object, "status", "activeMove")
							if lostBoundary == "Volume fence" && resource == volumeapi.VolumeResource && phase == volumeapi.PhaseBlocked ||
								lostBoundary == "Move intent" && resource == volumeapi.MoveResource && intent ||
								lostBoundary == "final unlock" && resource == volumeapi.VolumeResource && phase == volumeapi.PhaseReady && active == "" {
								lost = true
								return io.ErrUnexpectedEOF
							}
							return nil
						}
						pendingPasses, stalePasses := 0, 0
						var lastWait error
						for range 64 {
							// A fresh process reads both the Move and its embedded journal.
							f.reconciler = newTestReconciler(f.client, f.registry, func(r *Reconciler) {
								r.Cleanups = &cleanupapi.Store{Client: f.dynamic}
								r.CleanupOperator, r.ServiceAccountName = operator, "shiftpv-controller"
							})
							err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0))
							if errors.Is(err, io.ErrUnexpectedEOF) {
								sawLoss = true
							} else if err != nil {
								lastWait = err
							}
							finishRecoveryJobs(t, f.client, batchv1.JobComplete)
							live, move := f.state(t, 0), f.move(t, 0)
							if live.UID != original.UID || live.OwnerNode != original.OwnerNode || !reflect.DeepEqual(live.CurrentCopy, original.CurrentCopy) || live.CapacityBytes != original.CapacityBytes || len(live.PublishedNodes) != 0 || live.Deleting || live.HasCleanup || !reflect.DeepEqual(live.Finalizers, original.Finalizers) {
								t.Fatalf("retirement changed owner/accounting or fabricated publication: %+v", live)
							}
							journal, journalErr := f.reconciler.Cleanups.GetHistory(f.ctx, moveCleanupAuthority(move))
							if journalErr != nil && !errors.Is(journalErr, cleanupapi.ErrNoJournal) {
								t.Fatal(journalErr)
							}
							if journalErr == nil && journal.Status.Phase == cleanupapi.PhasePending {
								pendingPasses++
								operator.allowed = pendingPasses >= 3
							}
							withholdAbsence := journalErr == nil && journal.Status.Phase == cleanupapi.PhaseConfirmingAbsence && stalePasses < 3
							if withholdAbsence {
								stalePasses++
							}
							if !move.Status.CapacityApproved && (journalErr != nil || cleanupapi.ValidateSettled(journal) != nil) {
								t.Fatal("temporary hold released without exact receipt and fresh absence")
							}
							if live.ActiveMove == "" && (move.Status.CapacityApproved || move.Status.RecoveryPhase == "" || live.Phase != volumeapi.PhaseReady) {
								t.Fatal("Volume unlocked before retirement settlement")
							}
							if live.ActiveMove == "" {
								jobs, err := f.client.BatchV1().Jobs("system").List(f.ctx, metav1.ListOptions{})
								if err != nil || len(jobs.Items) != 0 {
									t.Fatalf("Volume unlocked with executor/verifier artifacts: %+v %v", jobs, err)
								}
							} else if len(move.Finalizers) == 0 {
								t.Fatal("Move protection released while it still owned the Volume gate")
							}
							assertRetirementReservations(t, f, move, original)
							if volumeapi.MoveCleanupSettled(move) && len(move.Finalizers) == 0 && live.ActiveMove == "" {
								break
							}
							retirementRegistryScan(t, f, original, operator.receipted, withholdAbsence, journal)
						}
						move, state := f.move(t, 0), f.state(t, 0)
						if move.Spec.Recovery != "" {
							t.Fatal("automatic settlement required an operator ResumeOwner request")
						}
						if !lost || !sawLoss || pendingPasses < 3 || stalePasses != 3 || !volumeapi.MoveCleanupSettled(move) || len(move.Finalizers) != 0 || state.ActiveMove != "" {
							t.Fatalf("trace did not close all boundaries: lost=%v observed=%v pending=%d stale=%d state=%+v move=%+v lastWait=%v", lost, sawLoss, pendingPasses, stalePasses, state, move.Status, lastWait)
						}
						pv, err := f.client.CoreV1().PersistentVolumes().Get(f.ctx, f.workloads[0].PV.Name, metav1.GetOptions{})
						if err != nil || pv.Spec.PersistentVolumeReclaimPolicy != policy {
							t.Fatalf("Move changed reclaim policy: %+v %v", pv, err)
						}
						if replaced {
							claim, err := f.client.CoreV1().PersistentVolumeClaims(replacement.Namespace).Get(f.ctx, replacement.Name, metav1.GetOptions{})
							if err != nil || !reflect.DeepEqual(claim, replacement) {
								t.Fatalf("old Move adopted replacement: %+v %v", claim, err)
							}
						}
					})
				}
			}
		}
	}
}

func retirementRegistryFixture(t *testing.T, owner string, policy corev1.PersistentVolumeReclaimPolicy, replaced bool) (*preparationRegistryFixture, volumeapi.State, *corev1.PersistentVolumeClaim) {
	t.Helper()
	f, move := preparationAtCapacity(t)
	if err := f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	assignJobUIDs(f.client)
	// The fake API supplies generation changes; production scan code still
	// requests and consumes the actual returned fence on separate passes.
	f.dynamic.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	move = f.move(t, 0)
	state := f.state(t, 0)
	if owner == "destination" {
		next := state
		next.OwnerNode, next.CurrentCopy, next.Phase = owner, move.Status.DestinationCopy, volumeapi.PhaseReady
		if err := f.registry.CompareAndSetState(f.ctx, move.Spec.VolumeID, state.Phase, move.Name, state.OwnerNode, next); err != nil {
			t.Fatal(err)
		}
	}
	move.Status.Phase = "Copying"
	if owner == "destination" {
		move.Status.Phase = "WaitingForDestinationPublish"
	}
	if _, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, move.Status); err != nil {
		t.Fatal(err)
	}
	w := f.workloads[0]
	pv, err := f.client.CoreV1().PersistentVolumes().Get(f.ctx, w.PV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pv.Spec.PersistentVolumeReclaimPolicy = policy
	if _, err = f.client.CoreV1().PersistentVolumes().Update(f.ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = f.client.CoreV1().Pods(w.Consumer.Namespace).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = f.client.CoreV1().PersistentVolumeClaims(w.Claim.Namespace).Delete(f.ctx, w.Claim.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	var replacement *corev1.PersistentVolumeClaim
	if replaced {
		replacement = w.Claim.DeepCopy()
		replacement.UID, replacement.ResourceVersion, replacement.Spec.VolumeName = "replacement-uid", "", "replacement-pv"
		replacement, err = f.client.CoreV1().PersistentVolumeClaims(replacement.Namespace).Create(f.ctx, replacement, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	return f, f.state(t, 0), replacement
}

// This effect stub deliberately holds a real journal Pending across retries,
// then supplies a simulated executor receipt. The actual Store still demands a
// later exact Pool generation and absent target before releasing the hold.
type retirementEffectStub struct{ allowed, receipted bool }

func (s *retirementEffectStub) AdvanceReclaim(ctx context.Context, request cleanupapi.Cleanup, journal cleanupapi.Journal) (cleanupapi.Cleanup, bool, error) {
	if !s.allowed {
		return request, false, nil
	}
	result, done, err := (receiptCleanupOperator{}).AdvanceReclaim(ctx, request, journal)
	if err == nil && result.Status.Receipt != nil {
		s.receipted = true
	}
	return result, done, err
}

func (*retirementEffectStub) ObserveCompletedCleanup(ctx context.Context, current cleanupapi.Cleanup) (bool, error) {
	return (receiptCleanupOperator{}).ObserveCompletedCleanup(ctx, current)
}

func retirementRegistryScan(t *testing.T, f *preparationRegistryFixture, retained volumeapi.State, purged, withholdAbsence bool, journal cleanupapi.Cleanup) {
	t.Helper()
	move := f.move(t, 0)
	for _, copy := range []*volume.CopyIdentity{move.Status.SourceCopy, move.Status.DestinationCopy} {
		if withholdAbsence && copy.PoolName == journal.Spec.Target.PoolName {
			continue
		}
		object, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, copy.PoolName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var observed volumeapi.PoolStatus
		statusMap, _, err := unstructured.NestedMap(object.Object, "status")
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(statusMap, &observed); err != nil {
			t.Fatal(err)
		}
		observed.ObservedGeneration, observed.LastProbeTime = object.GetGeneration(), metav1.Now()
		observed.Conditions = testBackingConditions(object.GetGeneration())
		observed.Inventory = &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.Now()}
		if *copy == *retained.CurrentCopy || !purged {
			observed.Inventory.Copies = []volumeapi.CopyObservation{{Identity: copy, Marker: "placement-" + copy.CopyID + ".json", Present: true}}
		}
		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&observed)
		if err != nil {
			t.Fatal(err)
		}
		object.Object["status"] = status
		if _, err = f.dynamic.Resource(volumeapi.PoolResource).UpdateStatus(f.ctx, object, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRetirementReservations(t *testing.T, f *preparationRegistryFixture, move volumeapi.Move, retained volumeapi.State) {
	t.Helper()
	for _, copy := range []*volume.CopyIdentity{move.Status.SourceCopy, move.Status.DestinationCopy} {
		got, err := poolcapacity.ReservedBytesForPool(map[string]volumeapi.State{move.Spec.VolumeID: f.state(t, 0)}, []volumeapi.Move{move}, copy.PoolUID)
		want := int64(0)
		if copy.NodeName == retained.OwnerNode || move.Status.CapacityApproved {
			want = retained.CapacityBytes
		}
		if err != nil || got != want {
			t.Fatalf("original/temporary reservation changed: pool=%s got=%d want=%d err=%v", copy.PoolName, got, want, err)
		}
	}
}
