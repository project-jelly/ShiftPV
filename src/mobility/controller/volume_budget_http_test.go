package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type volumeBudgetHTTPCall struct {
	stall     bool
	entered   sync.Once
	active    atomic.Int32
	maximum   atomic.Int32
	cancelled atomic.Int32
	expired   atomic.Int32
	wrongErr  atomic.Int32
}

// Only GetMove uses HTTP. All writes still use the real Registry and its
// resourceVersion-aware fixture. Blocking happens outside fake-client locks.
type volumeBudgetHTTPRepository struct {
	*volumeapi.Registry
	reader     *volumeapi.Registry
	calls      map[string]*volumeBudgetHTTPCall
	entered    chan string
	recovered  atomic.Bool
	unexpected atomic.Int32
}

func (r *volumeBudgetHTTPRepository) GetMove(ctx context.Context, name string) (volumeapi.Move, error) {
	call := r.calls[name]
	active := call.active.Add(1)
	defer call.active.Add(-1)
	for maximum := call.maximum.Load(); active > maximum; maximum = call.maximum.Load() {
		if call.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	move, err := r.reader.GetMove(ctx, name)
	// A successfully received response may race with controller shutdown.
	// Only an unsuccessful cancelled read must retain the context error.
	if err != nil && ctx.Err() != nil {
		if !errors.Is(err, ctx.Err()) {
			call.wrongErr.Add(1)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			call.expired.Add(1)
		}
	}
	return move, err
}

func newVolumeBudgetHTTPRepository(t *testing.T, f *preparationRegistryFixture, stalled int) *volumeBudgetHTTPRepository {
	t.Helper()
	r := &volumeBudgetHTTPRepository{Registry: f.registry, calls: map[string]*volumeBudgetHTTPCall{}, entered: make(chan string, len(f.moveNames))}
	for index, name := range f.moveNames {
		r.calls[name] = &volumeBudgetHTTPCall{stall: index < stalled}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		const prefix = "/apis/shiftpv.io/v1alpha1/shiftpvmoves/"
		name := strings.TrimPrefix(request.URL.Path, prefix)
		call := r.calls[name]
		if request.Method != http.MethodGet || !strings.HasPrefix(request.URL.Path, prefix) || call == nil {
			r.unexpected.Add(1)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if call.stall && !r.recovered.Load() {
			call.entered.Do(func() { r.entered <- name })
			<-request.Context().Done()
			call.cancelled.Add(1)
			return
		}
		object, err := f.dynamic.Tracker().Get(volumeapi.MoveResource, "", name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(object)
	}))
	t.Cleanup(server.Close)
	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	r.reader = &volumeapi.Registry{Client: client}
	return r
}

func TestVolumeBudgetHTTPProgressAndRestart(t *testing.T) {
	f := newPreparationRegistryFixture(t, 4)
	late := detachVolumeBudgetWork(t, f, 3)
	before := make([]volumeapi.State, 3)
	original := make([]volumeapi.Move, len(f.moveNames))
	for index := range before {
		before[index], original[index] = f.state(t, index), f.move(t, index)
	}
	original[3] = volumeapi.Move{Name: late[1].GetName(), UID: string(late[1].GetUID())}
	repository := newVolumeBudgetHTTPRepository(t, f, 3)
	wake := make(chan struct{}, 1)
	f.reconciler.Repository, f.reconciler.Wake, f.reconciler.Interval = repository, wake, time.Hour
	f.reconciler.ReconcileTimeout = 200 * time.Millisecond
	stop := runDispatcherForTest(t, f.reconciler)
	for range mobilityWorkers {
		select {
		case <-repository.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("both workers did not reach stalled HTTP requests")
		}
	}
	// The fourth Volume did not exist when both workers became blocked.
	for _, object := range late {
		if err := f.dynamic.Tracker().Add(object); err != nil {
			t.Fatal(err)
		}
	}
	notifyReconciler(wake)
	waitDispatcherCondition(t, "late healthy Volume did not persist its plan after request expiry", func() bool {
		return f.move(t, 3).Status.SourceCopy != nil
	})
	assertVolumeBudgetPlan(t, f, 3, original[3].UID)
	for index := range before {
		call := repository.calls[f.moveNames[index]]
		waitDispatcherCondition(t, "stalled HTTP request did not observe caller deadline cancellation", func() bool {
			return call.cancelled.Load() > 0 && call.expired.Load() > 0
		})
	}
	stop()
	for index, expected := range before {
		move := f.move(t, index)
		if !reflect.DeepEqual(f.state(t, index), expected) || !reflect.DeepEqual(move, original[index]) {
			t.Fatalf("expired observation changed stalled source or journal: index=%d state=%+v move=%+v", index, f.state(t, index), move)
		}
		assertVolumeBudgetConsumer(t, f, index)
	}
	assertVolumeBudgetHTTPCalls(t, repository)
	assertNoEviction(t, f.client)

	// A new controller rereads the same durable Move after the API recovers.
	repository.recovered.Store(true)
	restarted := newTestReconciler(f.client, repository, func(r *Reconciler) {
		r.Interval, r.ReconcileTimeout = time.Hour, 200*time.Millisecond
	})
	stopRestart := runDispatcherForTest(t, restarted)
	for index := range before {
		waitDispatcherCondition(t, "restart did not resume the existing transaction", func() bool {
			return f.move(t, index).Status.SourceCopy != nil
		})
		assertVolumeBudgetPlan(t, f, index, original[index].UID)
	}
	stopRestart()
	assertVolumeBudgetHTTPCalls(t, repository)
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil || len(moves) != len(original) {
		t.Fatalf("restart duplicated transactions: moves=%d err=%v", len(moves), err)
	}
	assertNoEviction(t, f.client)
}

func TestVolumeBudgetHTTPHonorsEarlierParentDeadline(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	repository := newVolumeBudgetHTTPRepository(t, f, 1)
	f.reconciler.Repository, f.reconciler.ReconcileTimeout = repository, time.Hour
	before, move := f.state(t, 0), f.move(t, 0)
	ctx, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	defer cancel()
	_, err := f.reconciler.advanceVolume(ctx, volumeWork{volumeID: f.volumeIDs[0], moves: []moveReference{{move.Name, move.UID}}})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("actual parent expiry lost its identity: error=%v context=%v", err, ctx.Err())
	}
	call := repository.calls[move.Name]
	waitDispatcherCondition(t, "HTTP server did not observe parent cancellation", func() bool { return call.cancelled.Load() == 1 })
	if !reflect.DeepEqual(f.state(t, 0), before) || !reflect.DeepEqual(f.move(t, 0), move) || call.expired.Load() != 1 {
		t.Fatal("parent expiry changed authority or failed to reach the actual HTTP client")
	}
	assertVolumeBudgetHTTPCalls(t, repository)
	assertNoEviction(t, f.client)
}

func detachVolumeBudgetWork(t *testing.T, f *preparationRegistryFixture, index int) []*unstructured.Unstructured {
	t.Helper()
	var objects []*unstructured.Unstructured
	for _, resource := range []struct {
		name   string
		isMove bool
	}{{f.volumeIDs[index], false}, {f.moveNames[index], true}} {
		gvr := volumeapi.VolumeResource
		if resource.isMove {
			gvr = volumeapi.MoveResource
		}
		object, err := f.dynamic.Resource(gvr).Get(f.ctx, resource.name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, object)
		if err := f.dynamic.Tracker().Delete(gvr, "", resource.name); err != nil {
			t.Fatal(err)
		}
	}
	return objects
}

func assertVolumeBudgetPlan(t *testing.T, f *preparationRegistryFixture, index int, uid string) {
	t.Helper()
	move, state := f.move(t, index), f.state(t, index)
	w := f.workloads[index]
	if move.UID != uid || move.ResourceVersion == "1" || !reflect.DeepEqual(move.Status.SourceCopy, state.CurrentCopy) ||
		move.Status.ClaimUID != string(w.Claim.UID) || move.Status.PersistentVolumeUID != string(w.PV.UID) || move.Status.ConsumerUID != string(w.Consumer.UID) {
		t.Fatalf("plan did not preserve exact durable identity: %+v", move)
	}
}

func assertVolumeBudgetConsumer(t *testing.T, f *preparationRegistryFixture, index int) {
	t.Helper()
	expected := f.workloads[index].Consumer
	pod, err := f.client.CoreV1().Pods(expected.Namespace).Get(f.ctx, expected.Name, metav1.GetOptions{})
	if err != nil || pod.UID != expected.UID || pod.DeletionTimestamp != nil || pod.Status.Phase != expected.Status.Phase {
		t.Fatalf("stalled source consumer changed: pod=%+v err=%v", pod, err)
	}
}

func assertVolumeBudgetHTTPCalls(t *testing.T, repository *volumeBudgetHTTPRepository) {
	t.Helper()
	for name, call := range repository.calls {
		if call.maximum.Load() != 1 || call.active.Load() != 0 || call.wrongErr.Load() != 0 {
			t.Fatalf("HTTP caller lane/error contract violated for %s: maximum=%d active=%d lostCause=%d", name, call.maximum.Load(), call.active.Load(), call.wrongErr.Load())
		}
	}
	if repository.unexpected.Load() != 0 {
		t.Fatalf("unexpected HTTP routes: %d", repository.unexpected.Load())
	}
}
