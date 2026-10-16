package controller

import (
	"errors"
	"io"
	"reflect"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

// A prepared transaction has committed its destination, then the independent
// Node writer records a failure. No test executor fabricates a purge receipt.
func committedCleanupFailureFixture(t *testing.T, phase string) (*preparationRegistryFixture, *recordingCleanupOperator) {
	t.Helper()
	f, move := preparationAtCapacity(t)
	replacement := singleLifecycleReplacement(t, f)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	if !move.Status.CapacityApproved || move.Status.DestinationCopy == nil {
		t.Fatalf("fixture did not obtain a durable execution plan: %+v", move.Status)
	}
	status := move.Status
	status.Phase, status.PromotionJobName = phase, namesFor(move.Name).PromotionJob
	move, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
	if err != nil {
		t.Fatal(err)
	}
	state := f.state(t, 0)
	next := state
	next.Phase, next.OwnerNode, next.CurrentCopy = volumeapi.PhaseReady, move.Status.DestinationNode, move.Status.DestinationCopy
	if err := f.registry.CompareAndSetState(f.ctx, move.Spec.VolumeID, state.Phase, state.ActiveMove, state.OwnerNode, next); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.ReconcilePublished(f.ctx, move.Spec.VolumeID, next.OwnerNode, *next.CurrentCopy, true); err != nil {
		t.Fatal(err)
	}
	replacement.Spec.NodeName = next.OwnerNode
	replacement.Spec.NodeSelector = map[string]string{corev1.LabelHostname: next.OwnerNode}
	replacement.Spec.SchedulingGates = nil
	replacement.Annotations = map[string]string{placementAnnotationKey: "owner"}
	replacement.Status.Phase = corev1.PodRunning
	if _, err := f.client.CoreV1().Pods(replacement.Namespace).Update(f.ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, next.CurrentCopy.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(next.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedSlice(pool.Object, []any{map[string]any{"identity": identity, "present": true, "published": true}}, "status", "inventory", "copies")
	if _, err := f.commitDynamicUpdate(volumeapi.PoolResource, pool); err != nil {
		t.Fatal(err)
	}
	spec, err := moveCleanupSpec(move)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.reconciler.Cleanups.Ensure(f.ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	cleanupStatus := current.Status
	cleanupStatus.Phase = cleanupapi.PhaseRunning
	cleanupStatus.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: "system", PodName: "source-node", PodUID: "source-node-uid", NodeName: move.Spec.SourceNode}
	if err := f.reconciler.Cleanups.UpdateStatus(f.ctx, current, cleanupStatus); err != nil {
		t.Fatal(err)
	}
	current = blockedObservationJournal(t, f)
	cleanupStatus = current.Status
	cleanupStatus.Phase, cleanupStatus.Reason, cleanupStatus.Message = cleanupapi.PhaseNeedsReview, "CopyEvidenceInvalid", "node storage identity mismatch"
	if err := f.reconciler.Cleanups.UpdateStatus(f.ctx, current, cleanupStatus); err != nil {
		t.Fatal(err)
	}
	operator := &recordingCleanupOperator{}
	f.reconciler.CleanupOperator = operator
	return f, operator
}

func blockedObservationJournal(t *testing.T, f *preparationRegistryFixture) cleanupapi.Cleanup {
	t.Helper()
	journal, err := f.reconciler.Cleanups.Get(f.ctx, moveCleanupAuthority(f.move(t, 0)))
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

func assertBlockedCleanupAuthority(t *testing.T, f *preparationRegistryFixture, before volumeapi.Move, owner volumeapi.State, journal cleanupapi.Cleanup, operator *recordingCleanupOperator) {
	t.Helper()
	current, state := f.move(t, 0), f.state(t, 0)
	if state.UID != owner.UID || state.Phase != volumeapi.PhaseBlocked || state.OwnerNode != owner.OwnerNode ||
		state.ActiveMove != before.Name || !reflect.DeepEqual(state.CurrentCopy, owner.CurrentCopy) || !reflect.DeepEqual(state.PublishedNodes, owner.PublishedNodes) || state.CapacityBytes != owner.CapacityBytes {
		t.Fatalf("failure observation changed committed Volume authority: %+v", state)
	}
	if !current.Status.CapacityApproved || current.Status.DestinationPoolUID != before.Status.DestinationPoolUID ||
		!reflect.DeepEqual(current.Status.SourceCopy, before.Status.SourceCopy) || !reflect.DeepEqual(current.Status.IncomingCopy, before.Status.IncomingCopy) ||
		!reflect.DeepEqual(current.Status.DestinationCopy, before.Status.DestinationCopy) || !slices.Contains(current.Finalizers, volumeapi.MoveProtectionFinalizer) {
		t.Fatalf("failure observation released or changed the transaction hold: %+v", current)
	}
	if got := blockedObservationJournal(t, f); !reflect.DeepEqual(got, journal) || got.Status.Receipt != nil || got.Status.AbsenceProof != nil {
		t.Fatalf("failure observation changed Node evidence or invented settlement: %+v", got)
	}
	states, err := f.registry.ListVolumes(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{before.Status.SourceCopy.PoolUID, before.Status.DestinationPoolUID} {
		reserved, err := poolcapacity.ReservedBytesForPool(states, moves, uid)
		if err != nil || reserved != preparationRequestedBytes {
			t.Fatalf("Pool %s lost capacity authority: reserved=%d err=%v", uid, reserved, err)
		}
	}
	if operator.reclaims != 0 || operator.observations != 0 {
		t.Fatalf("failure observation reached an executor: %+v", operator)
	}
	for _, action := range f.client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("failure observation changed workload or helper: %#v", action)
		}
	}
}

func TestBlockedCleanupFailureSurvivesIndependentWriterFaults(t *testing.T) {
	for _, phase := range []string{"WaitingForDestinationPublish", "CleaningSource"} {
		for _, fault := range []string{"journal CAS conflict", "Volume response lost", "Move response lost"} {
			t.Run(phase+"/"+fault, func(t *testing.T) {
				f, operator := committedCleanupFailureFixture(t, phase)
				before, owner := f.move(t, 0), f.state(t, 0)
				fired := false
				f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
					writtenPhase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
					if fired || writtenPhase != volumeapi.PhaseBlocked {
						return nil
					}
					if fault == "Move response lost" {
						if resource != volumeapi.MoveResource {
							return nil
						}
						fired = true
						return io.ErrUnexpectedEOF
					}
					if resource != volumeapi.VolumeResource {
						return nil
					}
					fired = true
					if fault == "Volume response lost" {
						return io.ErrUnexpectedEOF
					}
					// A separate Node writer updates only its journal after Volume
					// blocking was accepted. The actual Move CAS must reject the
					// stale RV rather than replacing this independent evidence.
					stored, err := f.dynamic.Tracker().Get(volumeapi.MoveResource, "", before.Name)
					if err != nil {
						return err
					}
					changed := stored.(*unstructured.Unstructured).DeepCopy()
					_ = unstructured.SetNestedField(changed.Object, "independent Node observation", "status", "cleanup", "status", "message")
					_, err = f.commitDynamicUpdate(volumeapi.MoveResource, changed)
					return err
				}
				f.client.ClearActions()
				err := f.reconciler.reconcilePersistedMove(f.ctx, before)
				if !fired || fault == "journal CAS conflict" && !errors.Is(err, volumeapi.ErrStateConflict) || fault != "journal CAS conflict" && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("real accepted-write boundary was not exercised: fired=%v err=%v", fired, err)
				}
				journal := blockedObservationJournal(t, f)
				assertBlockedCleanupAuthority(t, f, before, owner, journal, operator)
				if fault != "Move response lost" && f.move(t, 0).Status.Phase != phase {
					t.Fatal("ambiguous Volume write advanced the Move phase")
				}
				f.afterWrite = nil
				previous := f.reconciler
				f.reconciler = newTestReconciler(f.client, f.registry, withCleanupsFrom(previous), func(r *Reconciler) {
					r.SourceUsage, r.CapacityProbe = previous.SourceUsage, previous.CapacityProbe
				})
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				blocked := f.move(t, 0)
				if blocked.Status.Phase != "Blocked" || blocked.Status.Reason != "CleanupFailed" {
					t.Fatalf("restart lost independent cleanup failure: %+v", blocked.Status)
				}
				assertBlockedCleanupAuthority(t, f, before, owner, journal, operator)
				// Recovery is an explicit fresh spec request. Its first durable
				// step must retain the destination as owner, never restore source.
				object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, blocked.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				_ = unstructured.SetNestedField(object.Object, "ResumeOwner", "spec", "recovery")
				if _, err := f.dynamic.Resource(volumeapi.MoveResource).Update(f.ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				recovering := f.move(t, 0)
				if recovering.Status.RecoveryOwner != owner.OwnerNode || recovering.Status.RecoveryPhase != recoveryQuiescing || recovering.Status.Reason != "CleanupFailed" {
					t.Fatalf("ResumeOwner changed the committed recovery direction: %+v", recovering.Status)
				}
				assertBlockedCleanupAuthority(t, f, before, owner, journal, operator)
			})
		}
	}
}

func TestBlockedCleanupFailureObservationRequiresExactFailedIntent(t *testing.T) {
	for _, change := range []string{"none", "Volume UID", "owner", "copy", "ActiveMove", "unlocked", "no journal", "Running journal", "other operation", "other target", "other parent UID", "Committing phase", "Completing phase"} {
		t.Run(change, func(t *testing.T) {
			f, operator := committedCleanupFailureFixture(t, "CleaningSource")
			move, state := f.move(t, 0), f.state(t, 0)
			next := state
			next.Phase = volumeapi.PhaseBlocked
			if err := f.registry.CompareAndSetState(f.ctx, move.Spec.VolumeID, state.Phase, state.ActiveMove, state.OwnerNode, next); err != nil {
				t.Fatal(err)
			}
			resource, name := volumeapi.VolumeResource, move.Spec.VolumeID
			if slices.Contains([]string{"no journal", "Running journal", "other operation", "other target", "other parent UID", "Committing phase", "Completing phase"}, change) {
				resource, name = volumeapi.MoveResource, move.Name
			}
			object, err := f.dynamic.Resource(resource).Get(f.ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "Volume UID":
				object.SetUID("replacement-volume-uid")
			case "owner":
				_ = unstructured.SetNestedField(object.Object, "foreign-node", "status", "ownerNode")
			case "copy":
				_ = unstructured.SetNestedField(object.Object, "foreign-copy", "status", "currentCopy", "copyID")
			case "ActiveMove":
				_ = unstructured.SetNestedField(object.Object, "another-move", "status", "activeMove")
			case "unlocked":
				_ = unstructured.SetNestedField(object.Object, "", "status", "activeMove")
			case "no journal":
				unstructured.RemoveNestedField(object.Object, "status", "cleanup")
			case "Running journal":
				_ = unstructured.SetNestedField(object.Object, cleanupapi.PhaseRunning, "status", "cleanup", "status", "phase")
			case "other operation":
				_ = unstructured.SetNestedField(object.Object, "cleanup-another-move", "status", "cleanup", "spec", "operationID")
			case "other target":
				_ = unstructured.SetNestedField(object.Object, "foreign-copy", "status", "cleanup", "spec", "target", "copyID")
			case "other parent UID":
				_ = unstructured.SetNestedField(object.Object, "foreign-move-uid", "status", "cleanup", "spec", "authority", "uid")
			case "Committing phase":
				_ = unstructured.SetNestedField(object.Object, "Committing", "status", "phase")
			case "Completing phase":
				_ = unstructured.SetNestedField(object.Object, "Completing", "status", "phase")
			}
			if err := f.dynamic.Tracker().Update(resource, object, ""); err != nil {
				t.Fatal(err)
			}
			move = f.move(t, 0)
			f.client.ClearActions()
			f.dynamic.ClearActions()
			observed, err := f.reconciler.observe(f.ctx, move)
			if change == "none" {
				if err != nil || !observed.FSM.CleanupFailed {
					t.Fatalf("exact failed intent was masked: %+v err=%v", observed.FSM, err)
				}
			} else if observed.FSM.CleanupFailed {
				t.Fatalf("unproven failure was admitted: %+v err=%v", observed.FSM, err)
			}
			if observed.FSM.OwnerCommitted || observed.FSM.CompletionReady || hasCommittedDestinationAuthority(move, f.state(t, 0), false) {
				t.Fatal("Blocked failure granted Ready execution or completion authority")
			}
			if err == nil {
				lifecycleFacts(move, &observed)
				decision, err := fsm.Decide(lifecycleState(move), observed.FSM)
				if err != nil || decision.Action == fsm.ActionEnsureCleanup || decision.Action == fsm.ActionConfirmCleanup || decision.Action == fsm.ActionMarkSucceeded {
					t.Fatalf("failure observation reopened execution: %+v %v", decision, err)
				}
			}
			if operator.reclaims != 0 || operator.observations != 0 {
				t.Fatal("observation contacted an executor")
			}
			for _, action := range append(f.client.Actions(), f.dynamic.Actions()...) {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("observation mutated API: %#v", action)
				}
			}
		})
	}
}
