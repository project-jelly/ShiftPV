package controller

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func preparationAtCapacity(t *testing.T) (*preparationRegistryFixture, volumeapi.Move) {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	move := f.untilPrepared(t, 0)
	state := f.state(t, 0)
	next := state
	next.Phase = volumeapi.PhaseMoving
	if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, state, next); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.ReconcilePublished(f.ctx, move.Spec.VolumeID, "source", *state.CurrentCopy, false); err != nil {
		t.Fatal(err)
	}
	status := move.Status
	status.Phase, status.EvictionRequested = "WaitingForCapacity", true
	move, err := f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
	if err != nil {
		t.Fatal(err)
	}
	return f, move
}

func TestPreparedExecutionConvertsExistingHoldAfterLimitReduction(t *testing.T) {
	f, move := preparationAtCapacity(t)
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(pool.Object, "1Gi", "spec", "capacity", "limit")
	if _, err = f.dynamic.Resource(volumeapi.PoolResource).Update(f.ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if !f.move(t, 0).Status.CapacityApproved {
		t.Fatal("previously granted hold was revoked by limit edit")
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedExecutionApprovalResponseLossCannotReplayOutsideLock(t *testing.T) {
	f, move := preparationAtCapacity(t)
	lost := errors.New("approval stored, response lost")
	approvals := 0
	f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
		approved, _, _ := unstructured.NestedBool(object.Object, "status", "capacityApproved")
		if resource == volumeapi.MoveResource && approved {
			approvals++
			return lost
		}
		return nil
	}
	if err := f.reconciler.reconcileMove(f.ctx, move); !errors.Is(err, lost) {
		t.Fatalf("ambiguous approval not returned: %v", err)
	}
	stored := f.move(t, 0)
	if approvals != 1 || stored.Status.Preparation != nil || !stored.Status.CapacityApproved || stored.Status.Phase != "WaitingForCapacity" {
		t.Fatalf("approval replayed or hold lost: writes=%d status=%+v", approvals, stored.Status)
	}
	jobs, err := f.client.BatchV1().Jobs("system").List(f.ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("disk work started on ambiguous approval: jobs=%+v err=%v", jobs, err)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedExecutionMeasurementResponseLossStopsBeforeApproval(t *testing.T) {
	f, move := preparationAtCapacity(t)
	lost := errors.New("measurement stored, response lost")
	measurementWrites := 0
	f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
		measured, _, _ := unstructured.NestedInt64(object.Object, "status", "sourceBytes")
		if resource == volumeapi.MoveResource && measured > 0 {
			measurementWrites++
			return lost
		}
		return nil
	}
	if err := f.reconciler.reconcileMove(f.ctx, move); !errors.Is(err, lost) {
		t.Fatalf("ambiguous measurement not returned: %v", err)
	}
	stored := f.move(t, 0)
	if measurementWrites != 1 || stored.Status.Preparation == nil || stored.Status.CapacityApproved || stored.Status.IncomingCopy != nil {
		t.Fatalf("measurement error allowed approval/replay: writes=%d status=%+v", measurementWrites, stored.Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedExecutionCancelsBeforeDiskAuthority(t *testing.T) {
	for _, change := range []string{"physical space", "claim missing"} {
		t.Run(change, func(t *testing.T) {
			f, move := preparationAtCapacity(t)
			if change == "physical space" {
				f.reconciler.CapacityProbe = fakeMoveCapacityProbe{usage: 2 << 20, stats: poolcapacity.Filesystem{AvailableBytes: 1 << 20}}
				f.reconciler.SourceUsage = f.reconciler.CapacityProbe.(SourceUsage)
				if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
					t.Fatal(err)
				}
				move = f.move(t, 0)
				if move.Status.CapacityReason != "DestinationFilesystemSpace" || move.Status.CapacityApproved {
					t.Fatalf("physical denial not recorded: %+v", move.Status)
				}
			} else if err := f.client.CoreV1().PersistentVolumeClaims(move.Status.ClaimNamespace).Delete(f.ctx, move.Status.ClaimName, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			if f.move(t, 0).Status.Phase != "Canceling" {
				t.Fatalf("pre-disk failure did not cancel: %+v", f.move(t, 0).Status)
			}
			for range 8 {
				if err := f.reconciler.reconcileCancellation(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
			}
			state, settled := f.state(t, 0), f.move(t, 0)
			if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || settled.Status.CapacityApproved || settled.Status.IncomingCopy != nil {
				t.Fatalf("source/hold did not settle: state=%+v move=%+v", state, settled)
			}
			assertPreparationReserved(t, f, 0)
		})
	}
}
