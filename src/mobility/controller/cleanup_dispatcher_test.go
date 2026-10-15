package controller

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestVolumeDispatcherPendingCleanupsDoNotOccupyAllWorkers(t *testing.T) {
	f := newPreparationRegistryFixture(t, mobilityWorkers+2)
	assignJobUIDs(f.client)
	pending := mobilityWorkers + 1
	journals := make([]cleanupapi.Cleanup, pending)
	states := make([]volumeapi.State, pending)
	for index := range pending {
		prepareTerminalCleanupDispatcher(t, f, index)
		move := f.move(t, index)
		spec, err := moveCleanupSpec(move)
		if err != nil {
			t.Fatal(err)
		}
		verifyingCleanupJob(t, f.reconciler, f.client, spec, time.Hour)
		journals[index], err = f.reconciler.Cleanups.Get(f.ctx, spec.Authority)
		if err != nil {
			t.Fatal(err)
		}
		states[index] = f.state(t, index)
	}
	// If the production path accidentally uses a synchronous observer again,
	// both workers would wait here until shutdown. No timeout frees a worker.
	helper := &helperpod.Runner{Client: f.client, Namespace: "system", Pools: f.registry, Image: "helper", Timeout: time.Hour}
	client := &nodeexecutor.Client{Volumes: f.registry, Fallback: helper, Timeout: time.Hour}
	observer := &pendingCleanupObserver{advance: client.AdvanceReclaim}
	f.reconciler.CleanupOperator = observer
	f.reconciler.Interval = 10 * time.Millisecond
	f.client.ClearActions()
	stop := runDispatcherForTest(t, f.reconciler)
	waitDispatcherCondition(t, "pending cleanup Jobs starved independent Volume preparation", func() bool {
		return observer.count() == pending && f.move(t, pending).Status.SourceCopy != nil
	})
	stop()
	for index, before := range journals {
		after, err := f.reconciler.Cleanups.Get(f.ctx, before.Spec.Authority)
		move := f.move(t, index)
		if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(f.state(t, index), states[index]) || !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
			t.Fatalf("pending cleanup changed journal, authority or finalizer: before=%+v after=%+v move=%+v err=%v", before, after, move, err)
		}
	}
	for _, action := range f.client.Actions() {
		if action.GetResource().Resource == "jobs" && action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("pending observer mutated an executor: %+v", action)
		}
	}
}

type pendingCleanupObserver struct {
	advance func(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error)
	mu      sync.Mutex
	seen    map[string]bool
}

func (o *pendingCleanupObserver) AdvanceReclaim(ctx context.Context, expected cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error) {
	current, ready, err := o.advance(ctx, expected, store)
	if err == nil && !ready {
		o.mu.Lock()
		if o.seen == nil {
			o.seen = map[string]bool{}
		}
		o.seen[current.Spec.Authority.UID] = true
		o.mu.Unlock()
	}
	return current, ready, err
}

func (o *pendingCleanupObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen)
}

func prepareTerminalCleanupDispatcher(t *testing.T, f *preparationRegistryFixture, index int) {
	t.Helper()
	move, state := f.move(t, index), f.state(t, index)
	source := *state.CurrentCopy
	destination := source
	destination.PoolName, destination.PoolUID, destination.NodeName = "destination-pool", "destination-pool-uid", "destination"
	destination.CopyID = "move-" + move.UID + "-serving"
	sourceObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&source)
	if err != nil {
		t.Fatal(err)
	}
	destinationObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&destination)
	if err != nil {
		t.Fatal(err)
	}
	object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
	object.Object["status"] = map[string]any{"phase": "Succeeded", "destinationNode": "destination", "sourceCopy": sourceObject,
		"destinationCopy": destinationObject, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano)}
	if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
		t.Fatal(err)
	}
	object, err = f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, move.Spec.VolumeID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Object["status"] = map[string]any{"phase": "Ready", "ownerNode": "destination", "currentCopy": destinationObject, "publishedNodes": []any{"destination"}}
	if _, err := f.commitDynamicUpdate(volumeapi.VolumeResource, object); err != nil {
		t.Fatal(err)
	}
}
