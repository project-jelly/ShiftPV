//go:build linux

package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	ktesting "k8s.io/client-go/testing"
)

// Exercise the real watch workers, Registry, journal Store, and filesystem
// effects without RPC. The fixture also has a remote destination Pool proving
// MoveSource authority; the two local Pools are the independent work lanes.
func TestNodeWatchBlockedCleanupLeavesIndependentPoolProgress(t *testing.T) {
	f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked, release := make(chan struct{}), make(chan struct{})
	var entered, released sync.Once
	f.node.VerifyBacking = func(_ string, pool volumeapi.Pool) error {
		if pool.UID == f.cleanup.Spec.Target.PoolUID {
			entered.Do(func() { close(blocked) })
			// Block outside fake-client reactors, whose mutex would otherwise
			// serialize unrelated API requests and manufacture a global stall.
			<-release
		}
		return nil
	}
	volumes, moves := watch.NewRaceFreeFake(), watch.NewRaceFreeFake()
	volumeOpened, moveOpened := make(chan struct{}, 1), make(chan struct{}, 1)
	f.api.PrependWatchReactor("shiftpvvolumes", func(ktesting.Action) (bool, watch.Interface, error) {
		volumeOpened <- struct{}{}
		return true, volumes, nil
	})
	f.api.PrependWatchReactor("shiftpvmoves", func(ktesting.Action) (bool, watch.Interface, error) {
		moveOpened <- struct{}{}
		return true, moves, nil
	})
	var creationReceipts, cleanupReceipts atomic.Int32
	f.api.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if _, found, _ := unstructured.NestedMap(object.Object, "status", "creationReceipt"); found {
			creationReceipts.Add(1)
		}
		return false, nil, nil
	})
	f.api.PrependReactor("update", "shiftpvmoves", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if _, found, _ := unstructured.NestedMap(object.Object, "status", "cleanup", "status", "receipt"); found {
			cleanupReceipts.Add(1)
		}
		return false, nil, nil
	})
	done := make(chan error, 1)
	go func() { done <- f.node.Run(ctx) }()
	t.Cleanup(func() {
		released.Do(func() { close(release) })
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
			if !t.Failed() && (creationReceipts.Load() != 1 || cleanupReceipts.Load() != 1) {
				t.Errorf("duplicate watch effects: creation receipts=%d cleanup receipts=%d", creationReceipts.Load(), cleanupReceipts.Load())
			}
		case <-time.After(5 * time.Second):
			t.Error("Node watch workers did not stop")
		}
	})
	for _, opened := range []<-chan struct{}{volumeOpened, moveOpened} {
		awaitWatchProgress(t, opened, "intent watch did not open")
	}
	a, err := f.api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m, err := f.api.Resource(volumeapi.MoveResource).Get(ctx, f.cleanup.Spec.Authority.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initialWatchObjects(volumes, a)
	initialWatchObjects(moves, m)
	awaitWatchProgress(t, blocked, "MoveSource cleanup did not reach its backing check")
	// Repeated Move notifications must not consume the other worker while A
	// remains in progress. B is admitted only after A is definitely blocked.
	for i := 2; i <= 5; i++ {
		duplicate := m.DeepCopy()
		duplicate.SetResourceVersion(fmt.Sprint(i))
		moves.Modify(duplicate)
	}
	b := addIndependentWatchCreation(t, f)
	volumes.Add(b)
	waitWatchProgress(t, "independent Pool creation stalled behind cleanup", func() (bool, error) {
		state, err := f.node.Volumes.Get(ctx, b.GetName())
		return volumeapi.ValidCreationReceipt(state), err
	})
	state, err := f.node.Volumes.Get(ctx, b.GetName())
	if err != nil || state.CurrentCopy == nil || state.CreationReceipt == nil || state.UID != string(b.GetUID()) || state.CreationReceipt.ExecutorUID != f.node.Identity.PodUID {
		t.Fatalf("independent creation identity changed: %+v %v", state, err)
	}
	root := filepath.Join(f.node.HostRoot, "mnt/independent")
	if err := ownership.VerifyServingPath(root, *state.CurrentCopy); err != nil {
		t.Fatalf("independent creation receipt has no exact serving copy: %v", err)
	}
	f.requirePayload(t)
	assertWatchCleanupAuthority(t, f, cleanupapi.PhaseRunning)
	if cleanupReceipts.Load() != 0 {
		t.Fatal("blocked cleanup wrote a receipt")
	}
	released.Do(func() { close(release) })
	waitWatchProgress(t, "released cleanup did not finish", func() (bool, error) {
		current, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
		return current.Status.Phase == cleanupapi.PhaseVerifying, err
	})
	assertWatchCleanupAuthority(t, f, cleanupapi.PhaseVerifying)
	if _, err := os.Stat(f.path); !os.IsNotExist(err) {
		t.Fatalf("exact MoveSource target was not purged: %v", err)
	}
	if err := ownership.VerifyServingPath(root, *state.CurrentCopy); err != nil {
		t.Fatalf("cleanup damaged the independent serving copy: %v", err)
	}
}

func addIndependentWatchCreation(t *testing.T, f moveCleanupFixture) *unstructured.Unstructured {
	t.Helper()
	ctx := context.Background()
	pool, err := f.api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pool.SetName("independent-pool")
	pool.SetUID("independent-pool-uid")
	_ = unstructured.SetNestedField(pool.Object, "/mnt/independent", "spec", "mountPath")
	if _, err := f.api.Resource(volumeapi.PoolResource).Create(ctx, pool, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.node.HostRoot, "mnt/independent"), 0700); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: pool.GetName(), PoolUID: string(pool.GetUID()),
		VolumeID: "shiftpv-fedcba9876543210fedcba9876543210", VolumeUID: "independent-volume-uid",
		CopyID: "initial-independent-volume-uid", NodeName: f.node.Identity.NodeName, Role: volume.RoleServing}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	if err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": copy.VolumeID, "uid": copy.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"requestName": "independent-pvc", "initialNode": copy.NodeName, "capacityBytes": int64(1024)},
		"status": map[string]any{"phase": volumeapi.PhasePending, "ownerNode": copy.NodeName,
			"creationOperationID": "create-" + copy.VolumeUID, "currentCopy": encoded},
	}}
	if _, err := f.api.Resource(volumeapi.VolumeResource).Create(ctx, object, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err := f.node.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.Volumes.BindCreation(ctx, state, f.node.Identity); err != nil {
		t.Fatal(err)
	}
	object, err = f.api.Resource(volumeapi.VolumeResource).Get(ctx, copy.VolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetResourceVersion("6")
	return object
}

func assertWatchCleanupAuthority(t *testing.T, f moveCleanupFixture, phase string) {
	t.Helper()
	ctx := context.Background()
	current, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
	if err != nil || current.Spec != f.cleanup.Spec || current.Status.Phase != phase || current.Status.AbsenceProof != nil {
		t.Fatalf("cleanup authority changed: %+v %v", current, err)
	}
	if current.UID != f.cleanup.UID || current.Status.Executor == nil || *current.Status.Executor != *f.cleanup.Status.Executor {
		t.Fatalf("cleanup executor binding changed: %+v", current)
	}
	if phase == cleanupapi.PhaseRunning && current.Status.Receipt != nil {
		t.Fatal("blocked cleanup has a premature receipt")
	}
	if phase == cleanupapi.PhaseVerifying {
		r := current.Status.Receipt
		if r == nil || r.OperationID != f.cleanup.Spec.OperationID || r.ExecutorUID != f.node.Identity.PodUID || !r.Retired || !r.Purged || r.LocalReceiptDigest == "" {
			t.Fatalf("cleanup lacks its exact Node receipt: %+v", r)
		}
	}
	move, err := f.node.Volumes.GetMove(ctx, f.cleanup.Spec.Authority.Name)
	if err != nil || move.UID != f.cleanup.Spec.Authority.UID || move.Status.Phase != "CleaningSource" || !move.Status.CapacityApproved {
		t.Fatalf("Node released the Move reservation: %+v %v", move, err)
	}
	state, err := f.node.Volumes.Get(ctx, testID)
	if err != nil || state.ActiveMove != move.Name {
		t.Fatalf("Node released the Volume authority: %+v %v", state, err)
	}
}

func awaitWatchProgress(t *testing.T, ready <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

func waitWatchProgress(t *testing.T, message string, ready func() (bool, error)) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		done, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal(message)
		}
	}
}
