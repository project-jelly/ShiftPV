package measurement

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type probeFunc func(context.Context, volumeapi.Pool) (capacity.Filesystem, error)

func (f probeFunc) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
	return f(ctx, pool)
}

func poolObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool", "uid": "pool-uid", "resourceVersion": "1", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "node", "mountPath": "/mnt/pool", "capacity": map[string]any{"limit": "1Gi"}},
		"status": map[string]any{"capacityProbeSupported": true, "observedGeneration": int64(1), "lastProbeTime": time.Now().UTC().Format(time.RFC3339),
			"inventory":  map[string]any{"valid": true, "truncated": false, "copies": []any{}, "observedAt": time.Now().UTC().Format(time.RFC3339)},
			"conditions": []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(1), "reason": "PoolReady", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)}}},
	}}
}

func TestNodeRecoversPendingReadAfterRestartAndDoesNotLoopOnStatus(t *testing.T) {
	api := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, poolObject())
	pools := &volumeapi.Registry{Client: api}
	pool, err := pools.ReadyPoolForIdentity(context.Background(), "pool", "pool-uid", "node")
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := matchingEvidence(pool, pool)
	r, encoded, _ := newRequest(evidence)
	if err := pools.RequestPoolCapacityProbe(context.Background(), pool, encoded); err != nil {
		t.Fatal(err)
	}
	// The fake tracker does not implement watch-list initial Added events and
	// the end bookmark. Supply those, then forward persisted updates.
	stream := watch.NewRaceFreeFake()
	opened := make(chan struct{}, 1)
	api.PrependWatchReactor("shiftpvpools", func(ktesting.Action) (bool, watch.Interface, error) {
		opened <- struct{}{}
		return true, stream, nil
	})
	api.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		if err := api.Tracker().Update(volumeapi.PoolResource, object, ""); err != nil {
			return true, nil, err
		}
		stream.Modify(object.DeepCopy())
		return true, object, nil
	})
	var reads atomic.Int32
	node := &Node{NodeName: "node", Pools: pools, Probe: probeFunc(func(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
		reads.Add(1)
		return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 50}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx, api) }()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("Node watcher did not open")
	}
	initial, err := api.Resource(volumeapi.PoolResource).Get(ctx, pool.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream.Add(initial.DeepCopy())
	bookmark := initial.DeepCopy()
	bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	stream.Action(watch.Bookmark, bookmark)
	client := &Client{Pools: pools, Fallback: &fakeProbe{}, Timeout: time.Second}
	if _, err := client.await(ctx, pool, r); err != nil {
		stored, _ := pools.PoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName)
		t.Fatalf("pending request was lost across restart: reads=%d request=%q answer=%+v err=%v", reads.Load(), stored.CapacityProbeRequest, stored.Status.CapacityProbe, err)
	}
	if _, err := client.StatFSForPool(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := pools.SetPoolStatus(ctx, pool.Name, pool.UID, pool.NodeName, pool.Status); err != nil {
		t.Fatal(err)
	}
	current, err := pools.ReadyPoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName)
	if err != nil || current.Status.CapacityProbe == nil || current.Status.CapacityProbe.RequestID == r.ID {
		t.Fatalf("readiness clobbered new response: status=%+v err=%v", current.Status, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Node worker did not stop")
	}
	if reads.Load() != 2 {
		t.Fatalf("status writes caused a probe loop: reads=%d", reads.Load())
	}
}

func TestNodeDoesNotExecuteUnrelatedOrMalformedRequests(t *testing.T) {
	pool := testPool()
	pools := &fakePools{pool: pool}
	probe := &fakeProbe{stats: capacity.Filesystem{TotalBytes: 100, AvailableBytes: 50}}
	node := &Node{NodeName: pool.NodeName, Pools: pools, Probe: probe}
	if err := node.answer(context.Background(), poolKey{pool.Name, pool.UID}); err != nil || probe.calls != 0 {
		t.Fatalf("missing request executed: calls=%d err=%v", probe.calls, err)
	}
	evidence, _ := matchingEvidence(pool, pool)
	_, encoded, _ := newRequest(evidence)
	pools.pool.CapacityProbeRequest = encoded
	probe.err = fmt.Errorf("mount missing")
	if err := node.answer(context.Background(), poolKey{pool.Name, pool.UID}); err != nil {
		t.Fatal(err)
	}
	if pools.pool.Status.CapacityProbe.Error == "" {
		t.Fatal("read failure did not return an explicit error")
	}
	if err := node.answer(context.Background(), poolKey{pool.Name, pool.UID}); err != nil || probe.calls != 1 {
		t.Fatalf("same nonce executed again: calls=%d err=%v", probe.calls, err)
	}
	pools.pool.CapacityProbeRequest = encoded[:len(encoded)-1] + "bad"
	if err := node.answer(context.Background(), poolKey{pool.Name, pool.UID}); err != nil || probe.calls != 1 {
		t.Fatalf("malformed request reached reader: calls=%d err=%v", probe.calls, err)
	}
	var keys []poolKey
	handler := node.handler(func(key poolKey) { keys = append(keys, key) })
	old := poolObject()
	current := old.DeepCopy()
	current.SetAnnotations(map[string]string{volumeapi.PoolCapacityProbeRequestAnnotation: encoded})
	handler.UpdateFunc(old, current)
	handler.UpdateFunc(current, current.DeepCopy())
	other := current.DeepCopy()
	_ = unstructured.SetNestedField(other.Object, "other-node", "spec", "nodeName")
	handler.AddFunc(other)
	terminating := current.DeepCopy()
	stamp := metav1.Now()
	terminating.SetDeletionTimestamp(&stamp)
	handler.AddFunc(terminating)
	if len(keys) != 1 || keys[0].uid != pool.UID {
		t.Fatalf("unexpected work enqueued: %v", keys)
	}
}
