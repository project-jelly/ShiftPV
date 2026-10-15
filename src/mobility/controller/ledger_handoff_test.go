package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

const handoffCapacity = int64(8 << 20)

// Pause after the real Registry decoded its Volume snapshot, outside the fake
// client's internal lock. Move reads and writes must remain able to proceed.
type handoffSnapshotRepository struct {
	*volumeapi.Registry
	snapshot  chan struct{}
	resume    chan struct{}
	listOrder []string
}

func (r *handoffSnapshotRepository) ListVolumes(ctx context.Context) (map[string]volumeapi.State, error) {
	r.listOrder = append(r.listOrder, "Volumes")
	states, err := r.Registry.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	close(r.snapshot)
	select {
	case <-r.resume:
		return states, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *handoffSnapshotRepository) ListMoves(ctx context.Context) ([]volumeapi.Move, error) {
	r.listOrder = append(r.listOrder, "Moves")
	return r.Registry.ListMoves(ctx)
}

// The second Move uses the real authorizing reader: ledger, shared Pool lock,
// physical probe and durable approval write. Free physical space deliberately
// exceeds the logical limit, so statfs cannot hide an omitted reservation.
func TestLedgerHandoffSerializesDestinationAdmission(t *testing.T) {
	testLedgerHandoffAdmission(t, false)
}

func TestLedgerHandoffAllowsSourceOutflowWithConservativeAdmission(t *testing.T) {
	testLedgerHandoffAdmission(t, true)
}

func testLedgerHandoffAdmission(t *testing.T, sourceAdmission bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newLedgerHandoffFixture(t, !sourceAdmission)
	pool := f.destinationPool
	if sourceAdmission {
		pool = f.sourcePool
	}
	paused := &handoffSnapshotRepository{Registry: f.registry, snapshot: make(chan struct{}), resume: make(chan struct{})}
	reader := *f.reconciler
	reader.Repository = paused
	type admissionResult struct {
		reason string
		err    error
	}
	admission := make(chan admissionResult, 1)
	go func() {
		candidate := f.candidate
		reason, err := reader.approveDestinationPool(ctx, &candidate, pool)
		admission <- admissionResult{reason, err}
	}()
	select {
	case <-paused.snapshot:
	case <-ctx.Done():
		t.Fatal("admission did not capture Volume snapshot")
	}

	if sourceAdmission {
		// A source reader must not prevent an independent destination grant.
		// Its old Volume snapshot already retains the source reservation, so
		// even owner handoff and terminal settlement may proceed before Moves.
		reason, err := f.reconciler.approveDestinationPool(ctx, &f.move, f.destinationPool)
		if err != nil || reason != "" {
			close(paused.resume)
			t.Fatalf("destination approval while source admission holds its lock: reason=%q err=%v", reason, err)
		}
		stored, err := f.registry.GetMove(ctx, f.move.Name)
		if err != nil || !stored.Status.CapacityApproved {
			close(paused.resume)
			t.Fatalf("destination approval was not durable: move=%+v err=%v", stored, err)
		}
		f.move.Status.Phase = "Committing"
		if err := f.registry.SetMoveStatus(ctx, f.move.Name, f.move.UID, f.move.Status); err != nil {
			close(paused.resume)
			t.Fatalf("advance approved handoff to commit: %v", err)
		}
	}

	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		close(started)
		if err := f.reconciler.commitOwner(ctx, &f.move, f.observed); err != nil {
			finished <- err
			return
		}
		finished <- f.completeMove(ctx)
	}()
	<-started

	// Destination ingress must wait. Source outflow must finish while the
	// reader is paused; the old owner snapshot is a conservative upper bound.
	var earlyErr error
	completedEarly := false
	if sourceAdmission {
		select {
		case earlyErr = <-finished:
			completedEarly = true
		case <-ctx.Done():
			close(paused.resume)
			t.Fatal("source reader unnecessarily blocked owner outflow and settlement")
		}
		if earlyErr == nil {
			f.assertReserved(t, f.sourcePool.UID, 0)
		}
	} else {
		// All race boundaries use channels. This deadline only verifies that
		// a contending writer cannot finish while admission owns its fence.
		blocked := time.NewTimer(time.Second)
		select {
		case earlyErr = <-finished:
			completedEarly = true
		case <-blocked.C:
		case <-ctx.Done():
			earlyErr, completedEarly = ctx.Err(), true
		}
		blocked.Stop()
	}
	close(paused.resume)

	var decision admissionResult
	select {
	case decision = <-admission:
	case <-ctx.Done():
		t.Fatal("admission did not finish after releasing the snapshot barrier")
	}
	if !completedEarly {
		select {
		case earlyErr = <-finished:
		case <-ctx.Done():
			t.Fatal("owner handoff did not resume after admission released the Pool lock")
		}
	}
	if earlyErr != nil {
		t.Fatalf("owner handoff/settlement failed: %v", earlyErr)
	}
	if completedEarly && !sourceAdmission {
		t.Error("owner handoff and terminal settlement completed inside a capacity reader's Pool fence")
	}
	// The destination-only fence relies on this order. Reversing the lists
	// can miss a new destination approval followed by a source owner outflow.
	if !reflect.DeepEqual(paused.listOrder, []string{"Volumes", "Moves"}) {
		t.Errorf("capacity admission read order=%v, want Volumes then Moves", paused.listOrder)
	}
	// For source outflow, this denial deliberately reflects the old Volume
	// snapshot even though the fresh ledger above is zero. Exactness is not
	// required; a competing hold must never be granted from an undercount.
	if decision.err != nil || decision.reason != "DestinationReservationLimit" {
		t.Errorf("admission omitted the existing reservation: reason=%q err=%v", decision.reason, decision.err)
	}
	stored, err := f.registry.GetMove(ctx, f.candidate.Name)
	if err != nil || stored.Status.CapacityApproved {
		t.Errorf("competing approval must remain absent: approved=%v err=%v", stored.Status.CapacityApproved, err)
	}
	state, err := f.registry.Get(ctx, f.move.Spec.VolumeID)
	if err != nil || state.OwnerNode != f.destinationPool.NodeName || state.ActiveMove != "" {
		t.Fatalf("handoff did not complete after fence release: state=%+v err=%v", state, err)
	}
	f.assertReserved(t, f.destinationPool.UID, handoffCapacity)
	f.assertReserved(t, f.sourcePool.UID, 0)
}

func TestLedgerHandoffReobservesCommittedOwnerAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	f := newLedgerHandoffFixture(t, true)
	lost, writes := false, 0
	f.client.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		owner, _, _ := unstructured.NestedString(object.Object, "status", "ownerNode")
		if object.GetName() != f.move.Spec.VolumeID || owner != f.destinationPool.NodeName {
			return false, nil, nil
		}
		writes++
		if lost {
			return false, nil, nil
		}
		lost = true
		if err := f.client.Tracker().Update(volumeapi.VolumeResource, object.DeepCopy(), ""); err != nil {
			return true, nil, err
		}
		return true, nil, io.ErrUnexpectedEOF
	})
	if err := f.reconciler.commitOwner(ctx, &f.move, f.observed); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected committed write with lost response, got %v", err)
	}
	f.assertReserved(t, f.sourcePool.UID, handoffCapacity)
	f.assertReserved(t, f.destinationPool.UID, handoffCapacity)
	// Both admission paths must regain the original shared locks even though
	// the successful owner write returned an error to its caller.
	admissions := make(chan error, 2)
	for _, pool := range []volumeapi.Pool{f.sourcePool, f.destinationPool} {
		go func() {
			candidate := f.candidate
			reason, err := f.reconciler.approveDestinationPool(ctx, &candidate, pool)
			if err == nil && reason != "DestinationReservationLimit" {
				err = fmt.Errorf("Pool %s accepted a conflicting reservation: reason=%q", pool.UID, reason)
			}
			admissions <- err
		}()
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for range 2 {
		select {
		case err := <-admissions:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("response loss leaked a Pool lock and blocked capacity admission")
		}
	}

	// A fresh controller uses the same durable API objects. Retry the stale
	// precommit observation first: Registry CAS must recognize its exact outcome.
	restarted := *f.reconciler
	restarted.PoolLocks = &poolcapacity.Locker{}
	if err := restarted.commitOwner(ctx, &f.move, f.observed); err != nil {
		t.Fatalf("retry after response loss rejected the committed copy: %v", err)
	}
	if writes != 1 {
		t.Fatalf("response-loss retry rewrote owner: writes=%d", writes)
	}
	state, err := f.registry.Get(ctx, f.move.Spec.VolumeID)
	if err != nil || state.CurrentCopy == nil || *state.CurrentCopy != *f.move.Status.DestinationCopy {
		t.Fatalf("stored destination changed after retry: state=%+v err=%v", state, err)
	}
	observed := observation{Volume: state}
	observed.FSM.OwnerCommitted = true
	if err := restarted.commitOwner(ctx, &f.move, observed); err != nil {
		t.Fatalf("re-observed committed copy failed: %v", err)
	}
	if err := f.completeMove(ctx); err != nil {
		t.Fatal(err)
	}
	f.assertReserved(t, f.sourcePool.UID, 0)
	f.assertReserved(t, f.destinationPool.UID, handoffCapacity)
}

type ledgerHandoffFixture struct {
	client                      *dynamicfake.FakeDynamicClient
	registry                    *volumeapi.Registry
	reconciler                  *Reconciler
	move, candidate             volumeapi.Move
	sourcePool, destinationPool volumeapi.Pool
	observed                    observation
}

func newLedgerHandoffFixture(t *testing.T, approved bool) *ledgerHandoffFixture {
	t.Helper()
	ctx := context.Background()
	source, _, _ := testCopyIdentities("shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "source", "destination")
	other, _, _ := testCopyIdentities("shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "other", "destination")
	other.VolumeUID = "other-volume-uid"
	f := &ledgerHandoffFixture{
		sourcePool:      volumeapi.Pool{Name: source.PoolName, UID: source.PoolUID, NodeName: "source", CapacityLimit: "8Mi"},
		destinationPool: volumeapi.Pool{Name: "destination-pool", UID: "destination-pool-uid", NodeName: "destination", CapacityLimit: "8Mi"},
		move: volumeapi.Move{Name: "handoff", UID: "handoff-uid", Spec: volumeapi.MoveSpec{VolumeID: source.VolumeID, SourceNode: source.NodeName},
			Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity", SourceCopy: &source, SourceBytes: 1 << 20, CandidateNodes: []string{"destination"}}},
		candidate: volumeapi.Move{Name: "competing", UID: "competing-uid", Spec: volumeapi.MoveSpec{VolumeID: other.VolumeID, SourceNode: other.NodeName},
			Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity", SourceCopy: &other, SourceBytes: 1 << 20}},
	}
	objects := []runtime.Object{}
	for _, move := range []volumeapi.Move{f.move, f.candidate} {
		copy, err := runtime.DefaultUnstructuredConverter.ToUnstructured(move.Status.SourceCopy)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects,
			&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
				"metadata": map[string]any{"name": move.Spec.VolumeID, "uid": move.Status.SourceCopy.VolumeUID, "resourceVersion": "1"},
				"spec":     map[string]any{"volumeID": move.Spec.VolumeID, "requestName": move.Name, "initialNode": move.Spec.SourceNode, "capacityBytes": handoffCapacity},
				"status":   map[string]any{"phase": volumeapi.PhaseMoving, "ownerNode": move.Spec.SourceNode, "activeMove": move.Name, "currentCopy": copy},
			}},
			&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
				"metadata": map[string]any{"name": move.Name, "uid": move.UID, "resourceVersion": "1", "generation": int64(1), "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
				"spec":     map[string]any{"volumeID": move.Spec.VolumeID, "sourceNode": move.Spec.SourceNode},
			}})
	}
	f.client = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
	}, objects...)
	f.registry = &volumeapi.Registry{Client: f.client}
	f.reconciler = &Reconciler{Client: fake.NewSimpleClientset(), Namespace: "system", HelperImage: "helper", Repository: f.registry,
		PoolLocks: &poolcapacity.Locker{}, CapacityProbe: fakeMoveCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
		Cleanups: &cleanupapi.Store{Client: f.client}}
	if approved {
		f.move.Status.DestinationNode, f.move.Status.DestinationPoolUID = f.destinationPool.NodeName, f.destinationPool.UID
		if err := prepareMoveCopyIdentitiesForPool(&f.move, f.destinationPool); err != nil {
			t.Fatal(err)
		}
		f.move.Status.CapacityApproved = true
		f.move.Status.Phase = "Committing"
	}
	for _, move := range []volumeapi.Move{f.move, f.candidate} {
		if err := f.registry.SetMoveStatus(ctx, move.Name, move.UID, move.Status); err != nil {
			t.Fatal(err)
		}
	}
	placement := f.reconciler.placementPod(f.move, &corev1.Pod{}, namesFor(f.move.Name))
	placement.Spec.NodeName = f.destinationPool.NodeName
	if _, err := f.reconciler.Client.CoreV1().Pods("system").Create(ctx, placement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err := f.registry.Get(ctx, f.move.Spec.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	f.observed = observation{Volume: state, DestinationNode: f.destinationPool.NodeName}
	return f
}

func (f *ledgerHandoffFixture) assertReserved(t *testing.T, poolUID string, want int64) {
	t.Helper()
	states, err := f.registry.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	moves, err := f.registry.ListMoves(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := poolcapacity.ReservedBytesForPool(states, moves, poolUID)
	if err != nil || got != want {
		t.Errorf("Pool %s reserved=%d want=%d err=%v", poolUID, got, want, err)
	}
}

// Inject the cleanup worker's already-completed journal, then use the real
// completion action and Registry writes. Filesystem/absence execution is out
// of scope: this fixture isolates the owner/hold accounting transition.
func (f *ledgerHandoffFixture) completeMove(ctx context.Context) error {
	move, err := f.registry.GetMove(ctx, f.move.Name)
	if err != nil {
		return err
	}
	spec, err := moveCleanupSpec(move)
	if err != nil {
		return err
	}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&spec)
	if err != nil {
		return err
	}
	object, err := f.client.Resource(volumeapi.MoveResource).Get(ctx, move.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedMap(object.Object, map[string]any{"spec": encoded, "status": map[string]any{"phase": cleanupapi.PhaseCompleted}}, "status", "cleanup"); err != nil {
		return err
	}
	if _, err := f.client.Resource(volumeapi.MoveResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
		return err
	}
	move.Status.Phase = "Completing"
	if err := f.registry.SetMoveStatus(ctx, move.Name, move.UID, move.Status); err != nil {
		return err
	}
	state, err := f.registry.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return err
	}
	if err := f.reconciler.markSucceeded(ctx, &move, observation{Volume: state, Names: namesFor(move.Name)}); err != nil {
		return fmt.Errorf("complete committed Move: %w", err)
	}
	move.Status.Phase = "Succeeded"
	return f.registry.SetMoveStatus(ctx, move.Name, move.UID, move.Status)
}
