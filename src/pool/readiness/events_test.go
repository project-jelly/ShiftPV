package readiness

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func poolEventFixture() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool-a", "uid": "pool-uid", "generation": int64(1), "resourceVersion": "1"},
		"spec":     map[string]any{"nodeName": "node-a"},
	}}
}

func TestPoolWatchIgnoresOwnStatusAndOtherNodes(t *testing.T) {
	for _, scenario := range []string{"status", "generation", "replacement", "deletion", "release approval", "other node"} {
		t.Run(scenario, func(t *testing.T) {
			before := poolEventFixture()
			after := before.DeepCopy()
			after.SetResourceVersion("2")
			want := true
			switch scenario {
			case "status":
				after.Object["status"] = map[string]any{"observedGeneration": int64(1)}
				want = false
			case "generation":
				after.SetGeneration(2)
			case "replacement":
				after.SetUID("new-uid")
			case "deletion":
				now := metav1.Now()
				after.SetDeletionTimestamp(&now)
			case "release approval":
				after.SetAnnotations(map[string]string{volumeapi.PoolIdentityReleaseAnnotation: "pool-uid"})
			case "other node":
				before.Object["spec"] = map[string]any{"nodeName": "other"}
				after.Object["spec"] = map[string]any{"nodeName": "other"}
				after.SetGeneration(2)
				want = false
			}
			wake := make(chan struct{}, 1)
			handler := poolChangeHandler("node-a", wake)
			handler.OnUpdate(before, after)
			if (len(wake) == 1) != want {
				t.Fatalf("wake=%d want=%v", len(wake), want)
			}
		})
	}
	wake := make(chan struct{}, 1)
	handler := poolChangeHandler("node-a", wake)
	handler.OnAdd(poolEventFixture(), true)
	handler.OnDelete(cache.DeletedFinalStateUnknown{Key: "pool-a", Obj: poolEventFixture()})
	if len(wake) != 1 {
		t.Fatal("notifications did not coalesce")
	}
}

func TestPoolWatchConnectsAndWakesOnScanGeneration(t *testing.T) {
	pool := poolEventFixture()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, pool)
	stream := watch.NewRaceFreeFake()
	opened := make(chan struct{}, 1)
	client.PrependWatchReactor("shiftpvpools", func(ktesting.Action) (bool, watch.Interface, error) { opened <- struct{}{}; return true, stream, nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := WatchPoolChanges(ctx, client, "node-a")
	awaitScanEvent(t, opened)
	// client-go's watch-list starts with Added events and an end bookmark.
	stream.Add(pool.DeepCopy())
	bookmark := pool.DeepCopy()
	bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	stream.Action(watch.Bookmark, bookmark)
	awaitScanEvent(t, wake)
	updated := pool.DeepCopy()
	updated.SetGeneration(2)
	updated.SetResourceVersion("2")
	stream.Modify(updated)
	awaitScanEvent(t, wake)
}

func TestReadinessWakeDoesNotWaitForSafetyInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake, observed, stopped := make(chan struct{}, 1), make(chan struct{}, 2), make(chan error, 1)
	r := &Reconciler{
		NodeName: "node-a", Pools: &fakeRepository{pool: volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "node-a"}, currentUID: "pool-uid"}, Inspector: fakeInspector{},
		Interval: time.Hour, Wake: wake,
		Observe: func(volumeapi.Pool, Result, error) { observed <- struct{}{} },
	}
	go func() { stopped <- r.Run(ctx) }()
	awaitScanEvent(t, observed)
	wake <- struct{}{}
	awaitScanEvent(t, observed)
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconciler did not stop")
	}
}

func awaitScanEvent(t *testing.T, events <-chan struct{}) {
	t.Helper()
	select {
	case <-events:
	case <-time.After(3 * time.Second):
		t.Fatal("scan event did not arrive")
	}
}
