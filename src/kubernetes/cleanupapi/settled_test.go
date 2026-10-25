package cleanupapi

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestValidateSettledUsesCompleteRecordedEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Cleanup)
	}{
		{"incomplete phase", func(c *Cleanup) { c.Status.Phase = PhaseConfirmingAbsence }},
		{"needs review", func(c *Cleanup) { c.Status.Phase = PhaseNeedsReview }},
		{"parent UID", func(c *Cleanup) { c.UID = "replacement-parent" }},
		{"invalid intent", func(c *Cleanup) { c.Spec.Target.VolumeUID = "replacement-volume" }},
		{"missing executor", func(c *Cleanup) { c.Status.Executor = nil }},
		{"executor node", func(c *Cleanup) { c.Status.Executor.NodeName = "another-node" }},
		{"executor Pod UID", func(c *Cleanup) { c.Status.Executor.PodUID = "" }},
		{"receipt operation", func(c *Cleanup) { c.Status.Receipt.OperationID = "another-operation" }},
		{"receipt executor", func(c *Cleanup) { c.Status.Receipt.ExecutorUID = "another-executor" }},
		{"not retired", func(c *Cleanup) { c.Status.Receipt.Retired = false }},
		{"not purged", func(c *Cleanup) { c.Status.Receipt.Purged = false }},
		{"receipt digest", func(c *Cleanup) { c.Status.Receipt.LocalReceiptDigest = "invalid" }},
		{"receipt time", func(c *Cleanup) { c.Status.Receipt.ObservedAt = "invalid" }},
		{"missing fence", func(c *Cleanup) { c.Status.AbsenceProof = nil }},
		{"fence Pool name", func(c *Cleanup) { c.Status.AbsenceProof.PoolName = "another-pool" }},
		{"fence Pool UID", func(c *Cleanup) { c.Status.AbsenceProof.PoolUID = "another-pool-uid" }},
		{"fence request", func(c *Cleanup) { c.Status.AbsenceProof.RequestID = "" }},
		{"missing generation", func(c *Cleanup) { c.Status.AbsenceProof.RequiredGeneration = 0 }},
		{"stale generation", func(c *Cleanup) { c.Status.AbsenceProof.ObservedGeneration = 0 }},
		{"invalid inventory", func(c *Cleanup) { c.Status.AbsenceProof.Valid = false }},
		{"partial inventory", func(c *Cleanup) { c.Status.AbsenceProof.Complete = false }},
		{"target present", func(c *Cleanup) { c.Status.AbsenceProof.Absent = false }},
		{"confirmation time", func(c *Cleanup) { c.Status.AbsenceProof.ConfirmedAt = "invalid" }},
		{"settlement time", func(c *Cleanup) { c.Status.SettledAt = "invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
			store, client := testStore(parent, testPool(3, 3, true, false, "", nil))
			client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
				update := action.(ktesting.UpdateAction)
				if update.GetSubresource() == "" {
					object := update.GetObject().(*unstructured.Unstructured)
					object.SetGeneration(object.GetGeneration() + 1)
				}
				return false, nil, nil
			})
			current := verifyingCleanup(t, store)
			current, _, err := store.ReconcileAbsence(context.Background(), current)
			if err != nil {
				t.Fatal(err)
			}
			updatePoolStatus(t, client, func(pool *unstructured.Unstructured) error {
				return unstructured.SetNestedField(pool.Object, int64(4), "status", "observedGeneration")
			})
			current, complete, err := store.ReconcileAbsence(context.Background(), current)
			if err != nil || !complete {
				t.Fatalf("real Store settlement: complete=%v err=%v", complete, err)
			}
			if err := ValidateSettled(current); err != nil {
				t.Fatalf("valid recorded evidence rejected: %v", err)
			}
			test.edit(&current)
			if err := ValidateSettled(current); !errors.Is(err, ErrConflict) {
				t.Fatalf("invalid evidence accepted: %v", err)
			}
			if _, complete, err := settledAbsence(current); !errors.Is(err, ErrConflict) || complete {
				t.Fatalf("runtime re-observation diverged: complete=%v err=%v", complete, err)
			}
		})
	}
}
