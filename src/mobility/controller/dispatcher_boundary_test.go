package controller

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// Block after the real Registry returns, outside the fake client's global
// reactor mutex. Otherwise the fixture itself would serialize unrelated work.
type blockedVolumeRepository struct {
	*volumeapi.Registry
	volumeID string
	entered  chan struct{}
	release  <-chan struct{}
	once     sync.Once
	active   atomic.Int32
	maximum  atomic.Int32
	lists    atomic.Int32
}

func (r *blockedVolumeRepository) Get(ctx context.Context, id string) (volumeapi.State, error) {
	state, err := r.Registry.Get(ctx, id)
	if err != nil || id != r.volumeID {
		return state, err
	}
	active := r.active.Add(1)
	defer r.active.Add(-1)
	for previous := r.maximum.Load(); active > previous; previous = r.maximum.Load() {
		if r.maximum.CompareAndSwap(previous, active) {
			break
		}
	}
	r.once.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		return volumeapi.State{}, ctx.Err()
	case <-r.release:
		return state, nil
	}
}

func (r *blockedVolumeRepository) ListMoves(ctx context.Context) ([]volumeapi.Move, error) {
	r.lists.Add(1)
	return r.Registry.ListMoves(ctx)
}

func TestVolumeDispatcherProgressDuringAnotherVolumeRead(t *testing.T) {
	for _, resume := range []string{"release", "restart"} {
		t.Run(resume, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 2)
			// B appears only after A is blocked, so B's stored progress cannot
			// have come from an earlier serial pass.
			bVolume, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, f.volumeIDs[1], metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			bMove, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, f.moveNames[1], metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.dynamic.Tracker().Delete(volumeapi.VolumeResource, "", f.volumeIDs[1]); err != nil {
				t.Fatal(err)
			}
			if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", f.moveNames[1]); err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			repository := &blockedVolumeRepository{Registry: f.registry, volumeID: f.volumeIDs[0], entered: make(chan struct{}), release: release}
			wake := make(chan struct{}, 1)
			f.reconciler.Repository, f.reconciler.Wake, f.reconciler.Interval = repository, wake, time.Hour
			stop := runDispatcherForTest(t, f.reconciler)
			select {
			case <-repository.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("Volume A did not enter its Registry read")
			}
			for _, object := range []*unstructured.Unstructured{bVolume, bMove} {
				if err := f.dynamic.Tracker().Add(object); err != nil {
					t.Fatal(err)
				}
			}
			notifyReconciler(wake)
			waitDispatcherCondition(t, "B must persist its exact plan while A is blocked", func() bool {
				return f.move(t, 1).Status.SourceCopy != nil
			})
			// Repeated complete scans must coalesce A while it is already being
			// processed. This also exercises events arriving during execution.
			for range 5 {
				before := repository.lists.Load()
				notifyReconciler(wake)
				waitDispatcherCondition(t, "event did not trigger a rescan", func() bool { return repository.lists.Load() > before })
			}
			if repository.maximum.Load() != 1 || f.move(t, 0).Status.SourceCopy != nil || f.state(t, 0).ActiveMove != "" {
				t.Fatalf("blocked Volume lane overlapped or changed authority: max=%d move=%+v", repository.maximum.Load(), f.move(t, 0).Status)
			}
			if resume == "release" {
				close(release)
				waitDispatcherCondition(t, "A did not resume after its read recovered", func() bool { return f.move(t, 0).Status.SourceCopy != nil })
				stop()
			} else {
				stop() // Cancellation must release a blocked worker and join it.
				if f.move(t, 0).Status.SourceCopy != nil {
					t.Fatal("cancelled read authorized a Move write")
				}
				restarted := newTestReconciler(f.client, f.registry, func(r *Reconciler) { r.Interval = time.Hour })
				stopRestart := runDispatcherForTest(t, restarted)
				waitDispatcherCondition(t, "restart did not relist persisted work", func() bool { return f.move(t, 0).Status.SourceCopy != nil })
				stopRestart()
			}
			moves, err := f.registry.ListMoves(f.ctx)
			if err != nil || len(moves) != 2 {
				t.Fatalf("repeated scans created duplicate transactions: moves=%d err=%v", len(moves), err)
			}
			assertNoEviction(t, f.client)
		})
	}
}

func TestVolumeDispatcherBusyPoolDoesNotOccupyAllWorkers(t *testing.T) {
	f := newPreparationRegistryFixture(t, 4)
	if err := f.dynamic.Tracker().Add(preparationPool("independent", "5Gi")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.CoreV1().Nodes().Create(f.ctx, readyNode("independent", false), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	before := make([]volumeapi.MoveStatus, 3)
	for index := range f.moveNames {
		prepareDispatcherPlacement(t, f, index)
		if index < len(before) {
			before[index] = f.move(t, index).Status
		}
	}
	unlock := f.reconciler.PoolLocks.Lock("destination-pool-uid")
	defer unlock()
	f.reconciler.Interval = 10 * time.Millisecond
	stop := runDispatcherForTest(t, f.reconciler)
	// Three contending Volumes exceed the worker count. They must return to
	// the queue instead of holding both workers while Pool B is available.
	waitDispatcherCondition(t, "independent Pool reservation was starved by busy Pool waiters", func() bool {
		return f.move(t, 3).Status.Preparation != nil
	})
	stop()
	for index, expected := range before {
		move := f.move(t, index)
		if !reflect.DeepEqual(move.Status, expected) {
			t.Fatalf("busy Pool changed transaction %d: before=%+v after=%+v", index, expected, move.Status)
		}
		pod, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
		if err != nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName != "destination" {
			t.Fatalf("busy Pool discarded its scheduler placement: pod=%+v err=%v", pod, err)
		}
		if state := f.state(t, index); state.Phase != volumeapi.PhaseReady || state.ActiveMove != move.Name {
			t.Fatalf("busy Pool interrupted source authority: %+v", state)
		}
	}
	if hold := f.move(t, 3).Status.Preparation; hold.Pool.UID != "independent-pool-uid" || hold.ReservedBytes != preparationRequestedBytes {
		t.Fatalf("wrong independent hold: %+v", hold)
	}
}

func prepareDispatcherPlacement(t *testing.T, f *preparationRegistryFixture, index int) {
	t.Helper()
	for range 8 {
		move := f.move(t, index)
		if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
			t.Fatal(err)
		}
		pod, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
		if err != nil {
			continue
		}
		pod.Spec.NodeName = "destination"
		if index == len(f.moveNames)-1 {
			pod.Spec.NodeName = "independent"
		}
		pod.Status.Phase = corev1.PodRunning
		if _, err := f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("preparation did not create a placement")
}

func runDispatcherForTest(t *testing.T, r *Reconciler) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		t.Helper()
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("dispatcher shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("dispatcher did not join its cancelled workers")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitDispatcherCondition(t *testing.T, message string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(time.Millisecond)
	}
}
