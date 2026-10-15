package controller

import (
	"errors"
	"io"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// A status response can fail while the parent reconcile context remains live.
// The caller must re-observe, not persist its old whole status as a diagnostic.
func TestLegacyRecoveryStatusFailureRequiresReobservation(t *testing.T) {
	for _, test := range []struct {
		name     string
		accepted bool
		failure  error
	}{
		{"EOF after storage", true, io.ErrUnexpectedEOF},
		{"API timeout after storage", true, apierrors.NewTimeoutError("accepted status, response lost", 1)},
		{"rejected before storage", false, errors.New("status rejected before storage")},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, move := legacyRecoveryStatusFixture(t)
			protected := f.state(t, 0)
			attempts, writes := 0, 0
			injected := false
			f.dynamic.PrependReactor("update", "shiftpvmoves", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "status" {
					return false, nil, nil
				}
				attempts++
				if !test.accepted && !injected {
					injected = true
					return true, nil, test.failure
				}
				return false, nil, nil
			})
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				if resource != volumeapi.MoveResource {
					return nil
				}
				writes++
				phase, _, _ := unstructured.NestedString(object.Object, "status", "recoveryPhase")
				if test.accepted && !injected && phase == recoveryVerifying {
					injected = true
					return test.failure
				}
				return nil
			}
			err := f.reconciler.reconcileRecovery(f.ctx, move)
			if !injected || !errors.Is(err, test.failure) || f.ctx.Err() != nil {
				t.Fatalf("failure was not returned with a live context: injected=%v err=%v ctx=%v", injected, err, f.ctx.Err())
			}
			wantPhase, wantWrites := recoveryQuiescing, 0
			if test.accepted {
				wantPhase, wantWrites = recoveryVerifying, 1
			}
			stored := f.move(t, 0)
			if attempts != 1 || writes != wantWrites || stored.Status.RecoveryPhase != wantPhase || stored.Status.RecoveryReason != "" {
				t.Fatalf("failed status was replayed by diagnostics: attempts=%d writes=%d phase=%s reason=%s", attempts, writes, stored.Status.RecoveryPhase, stored.Status.RecoveryReason)
			}
			if !errors.Is(err, errMoveStatusPersistence) {
				t.Fatalf("failed status did not require re-observation: %v", err)
			}
			if got := f.state(t, 0); !reflect.DeepEqual(got, protected) {
				t.Fatalf("status failure changed protected source: before=%+v after=%+v", protected, got)
			}
			if !test.accepted && !reflect.DeepEqual(stored, move) {
				t.Fatalf("rejected status changed the journal: before=%+v after=%+v", move, stored)
			}

			// A new reconciler uses only fresh API state. The accepted case keeps
			// Verifying; the rejected case retries Quiescing before creating the
			// one owner-verification Job. Neither skips its completion evidence.
			f.afterWrite = nil
			restarted := newTestReconciler(f.client, f.registry)
			for range 2 {
				if err := restarted.reconcileRecovery(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
			}
			jobs, err := f.client.BatchV1().Jobs(restarted.Namespace).List(f.ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 1 || jobs.Items[0].Name != recoveryNames(move).Base+"-verify" ||
				!moveOwned(jobs.Items[0].OwnerReferences, move.UID) || f.move(t, 0).Status.RecoveryPhase != recoveryVerifying {
				t.Fatalf("fresh recovery did not resume one verification Job: jobs=%+v err=%v", jobs, err)
			}
			finishRecoveryJobs(t, f.client, batchv1.JobComplete)
			if err := restarted.reconcileRecovery(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			if got := f.move(t, 0); got.Status.RecoveryPhase != recoveryRetiring || got.Status.RecoveryReason != "" {
				t.Fatalf("fresh recovery did not advance after verification: %+v", got.Status)
			}
			if got := f.state(t, 0); !reflect.DeepEqual(got, protected) {
				t.Fatalf("verification changed source authority before settlement: %+v", got)
			}
		})
	}
}

func legacyRecoveryStatusFixture(t *testing.T) (*preparationRegistryFixture, volumeapi.Move) {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, f.moveNames[0], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Seed an existing legacy journal; this is not a protocol migration.
	unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
	if err := unstructured.SetNestedField(object.Object, "ResumeOwner", "spec", "recovery"); err != nil {
		t.Fatal(err)
	}
	if err := f.dynamic.Tracker().Update(volumeapi.MoveResource, object, ""); err != nil {
		t.Fatal(err)
	}
	move, state := f.move(t, 0), f.state(t, 0)
	next := state
	next.Phase, next.ActiveMove = volumeapi.PhaseBlocked, move.Name
	if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, state, next); err != nil {
		t.Fatal(err)
	}
	node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Spec.Unschedulable = false
	if _, err := f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	w := f.workloads[0]
	move.Status.Phase = "Blocked"
	move.Status.RecoveryPhase, move.Status.RecoveryOwner = recoveryQuiescing, "source"
	move.Status.SourceCopy = state.CurrentCopy
	move.Status.PersistentVolumeName = w.PV.Name
	move.Status.ClaimNamespace, move.Status.ClaimName = w.Claim.Namespace, w.Claim.Name
	if err := f.registry.SetMoveStatus(f.ctx, move.Name, move.UID, move.Status); err != nil {
		t.Fatal(err)
	}
	return f, f.move(t, 0)
}
