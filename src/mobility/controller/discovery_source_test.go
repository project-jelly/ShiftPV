package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func sourceDiscoveryFixture(t *testing.T) *preparationRegistryFixture {
	t.Helper()
	f := newPreparationRegistryFixture(t, 1)
	if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", f.moveNames[0]); err != nil {
		t.Fatal(err)
	}
	return f
}

type discoveryPoolFailure struct {
	Repository
	err   error
	calls int
}

func (r *discoveryPoolFailure) ObservePools(ctx context.Context) (volumeapi.PoolSnapshot, error) {
	r.calls++
	if r.calls == 1 {
		return volumeapi.PoolSnapshot{}, r.err
	}
	return r.Repository.ObservePools(ctx)
}

func TestSourcePoolObservationFailurePreservesIndependentWork(t *testing.T) {
	f := newPreparationRegistryFixture(t, 2)
	if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", f.moveNames[0]); err != nil {
		t.Fatal(err)
	}
	pod := f.workloads[0].Consumer.DeepCopy()
	pod.Labels = map[string]string{"shiftpv.io/managed": "true"}
	pod.Spec.NodeName = ""
	pod.Status.Phase = corev1.PodPending
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	if _, err := f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("source proof LIST unavailable")
	f.reconciler.Repository = &discoveryPoolFailure{Repository: f.registry, err: sentinel}
	publishedMove := false
	hints, err := f.reconciler.scanVolumeWork(f.ctx, func(work volumeWork) {
		if work.volumeID == f.volumeIDs[1] && len(work.moves) == 1 {
			publishedMove = true
		}
	}, newDiscoveryObservations(nil))
	if !errors.Is(err, sentinel) || !publishedMove || len(hints[f.volumeIDs[1]].moves) != 1 || len(hints[f.volumeIDs[0]].ownerPods) != 1 || hints[f.volumeIDs[0]].discover {
		t.Fatalf("uncertain discovery swallowed independent work: hints=%+v published=%v err=%v", hints, publishedMove, err)
	}
	if _, err := f.reconciler.advanceVolume(f.ctx, hints[f.volumeIDs[1]]); err != nil {
		t.Fatal(err)
	}
	if f.move(t, 1).Status.SourceCopy == nil {
		t.Fatal("existing Move could not advance after unrelated discovery failed")
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil || len(moves) != 1 {
		t.Fatalf("uncertain observation created a new Move: %+v err=%v", moves, err)
	}
	assertNoEviction(t, f.client)
}

func updateDiscoveryPool(t *testing.T, f *preparationRegistryFixture, name string, mutate func(*unstructured.Unstructured)) {
	t.Helper()
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	uid := pool.GetUID()
	mutate(pool)
	if pool.GetUID() != uid {
		// Inject a replacement incarnation directly, as fixture API updates
		// correctly reject changing the UID of an existing object.
		if err := f.dynamic.Tracker().Update(volumeapi.PoolResource, pool, ""); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := f.dynamic.Resource(volumeapi.PoolResource).Update(f.ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceInventoryArrivalRetriesDiscoveryBeforeDeadline(t *testing.T) {
	for _, missing := range []string{"copy", "inventory", "current backing"} {
		t.Run(missing, func(t *testing.T) {
			f := sourceDiscoveryFixture(t)
			now := time.Now().UTC()
			f.registry.Now = func() time.Time { return now }
			f.reconciler.Now = func() time.Time { return now }
			f.reconciler.Interval = 30 * time.Second
			pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "source-pool", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			original := pool.DeepCopy()
			updateDiscoveryPool(t, f, pool.GetName(), func(pool *unstructured.Unstructured) {
				switch missing {
				case "copy":
					unstructured.RemoveNestedField(pool.Object, "status", "inventory", "copies")
				case "inventory":
					unstructured.RemoveNestedField(pool.Object, "status", "inventory")
				case "current backing":
					pool.SetGeneration(pool.GetGeneration() + 1)
				}
			})
			d := f.reconciler.newVolumeDispatcher()
			defer d.queue.ShutDown()
			observations := newDiscoveryObservations(nil)
			before := f.state(t, 0)
			scanAndProcessDiscovery(t, f, d, observations)
			if result := observations.results[f.volumeIDs[0]]; result.reason != "SourceUnavailable" || result.err != nil || len(d.deferred) != 1 {
				t.Fatalf("source was not deferred: result=%+v delays=%+v", result, d.deferred)
			}
			now = now.Add(5 * time.Second)
			updateDiscoveryPool(t, f, pool.GetName(), func(pool *unstructured.Unstructured) {
				pool.Object["status"] = runtime.DeepCopyJSONValue(original.Object["status"])
				if missing == "current backing" {
					_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
					conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
					for _, condition := range conditions {
						condition.(map[string]any)["observedGeneration"] = pool.GetGeneration()
					}
					_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
				}
			})
			scanAndProcessDiscovery(t, f, d, observations)
			moves, err := f.registry.ListMoves(f.ctx)
			if err != nil || len(moves) != 1 || moves[0].Spec.VolumeID != f.volumeIDs[0] {
				t.Fatalf("fresh source proof did not retry before 30s: moves=%+v error=%v delays=%+v", moves, err, d.deferred)
			}
			after := f.state(t, 0)
			if after.UID != before.UID || after.OwnerNode != before.OwnerNode || !reflect.DeepEqual(after.CurrentCopy, before.CurrentCopy) {
				t.Fatal("advisory source proof changed serving authority")
			}
			assertNoEviction(t, f.client)
		})
	}
}

func TestSourceDiscoveryUnrelatedUpdatesPreserveDelay(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "source unavailable", true: "multiple consumers"}[present], func(t *testing.T) {
			testSourceDiscoveryUnrelatedUpdatesPreserveDelay(t, present)
		})
	}
}

func testSourceDiscoveryUnrelatedUpdatesPreserveDelay(t *testing.T, present bool) {
	f, _ := deferredRegistryFixture(t, 1)
	now := time.Now().UTC()
	started := now
	sourceCopy := *f.state(t, 0).CurrentCopy
	f.registry.Now = func() time.Time { return now }
	f.reconciler.Now = func() time.Time { return now }
	f.reconciler.Interval = 30 * time.Second
	if !present {
		updateDiscoveryPool(t, f, "source-pool", func(pool *unstructured.Unstructured) {
			unstructured.RemoveNestedField(pool.Object, "status", "inventory", "copies")
		})
	}
	d := f.reconciler.newVolumeDispatcher()
	defer d.queue.ShutDown()
	observations := newDiscoveryObservations(nil)
	scanAndProcessDiscovery(t, f, d, observations)
	f.dynamic.ClearActions()
	for _, scenario := range []string{"metadata and clocks", "other source copy", "destination inventory"} {
		now = now.Add(time.Second)
		name := "source-pool"
		if scenario == "destination inventory" {
			name = "destination-pool"
		}
		updateDiscoveryPool(t, f, name, func(pool *unstructured.Unstructured) {
			pool.SetAnnotations(map[string]string{"test/noise": scenario})
			_ = unstructured.SetNestedField(pool.Object, now.Format(time.RFC3339), "status", "inventory", "observedAt")
			_ = unstructured.SetNestedField(pool.Object, now.Format(time.RFC3339), "status", "lastProbeTime")
			if scenario != "metadata and clocks" {
				copy := sourceCopy
				copy.VolumeID, copy.VolumeUID, copy.CopyID = "other-volume", "other-uid", "other-copy"
				identity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
				if err != nil {
					t.Fatal(err)
				}
				copies, _, _ := unstructured.NestedSlice(pool.Object, "status", "inventory", "copies")
				copies = append(copies, map[string]any{"identity": identity, "present": true})
				_ = unstructured.SetNestedSlice(pool.Object, copies, "status", "inventory", "copies")
			}
		})
		scanAndProcessDiscovery(t, f, d, observations)
		current, remaining := d.snapshot(f.volumeIDs[0])
		if current.discover || remaining != 30*time.Second-now.Sub(started) {
			t.Fatalf("%s invalidated unchanged source proof: work=%+v remaining=%v", scenario, current, remaining)
		}
	}
	for _, action := range f.dynamic.Actions() {
		if action.GetResource() == volumeapi.VolumeResource && action.GetVerb() == "get" {
			t.Fatal("irrelevant update repeated the live preflight")
		}
	}
	assertNoEviction(t, f.client)
}

func TestSourceDiscoverySchedulingHintCannotReplaceLiveProof(t *testing.T) {
	f := sourceDiscoveryFixture(t)
	hints, err := f.reconciler.scanVolumeWork(f.ctx, nil, newDiscoveryObservations(nil))
	if err != nil || !hints[f.volumeIDs[0]].source.sourceAvailable {
		t.Fatalf("source was not initially observable: hints=%+v err=%v", hints, err)
	}
	updateDiscoveryPool(t, f, "source-pool", func(pool *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(pool.Object, false, "status", "inventory", "valid")
	})
	result, err := f.reconciler.advanceVolume(f.ctx, hints[f.volumeIDs[0]])
	if err != nil || result.reason != "SourceUnavailable" {
		t.Fatalf("worker did not reject stale scheduling proof: %+v err=%v", result, err)
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil || len(moves) != 0 {
		t.Fatalf("scheduling hint authorized a Move: %+v err=%v", moves, err)
	}
	assertNoEviction(t, f.client)
}

func TestUnknownSourceInventoryCannotCreateMove(t *testing.T) {
	for _, invalid := range []string{"invalid", "truncated", "stale", "future", "wrong Pool UID", "copy problem"} {
		t.Run(invalid, func(t *testing.T) {
			f := sourceDiscoveryFixture(t)
			now := time.Now().UTC()
			f.registry.Now = func() time.Time { return now }
			f.reconciler.Now = func() time.Time { return now }
			f.reconciler.Interval = 30 * time.Second
			updateDiscoveryPool(t, f, "source-pool", func(pool *unstructured.Unstructured) {
				switch invalid {
				case "invalid":
					_ = unstructured.SetNestedField(pool.Object, false, "status", "inventory", "valid")
				case "truncated":
					_ = unstructured.SetNestedField(pool.Object, true, "status", "inventory", "truncated")
				case "stale", "future":
					observed := now.Add(-time.Hour)
					if invalid == "future" {
						observed = now.Add(time.Hour)
					}
					_ = unstructured.SetNestedField(pool.Object, observed.Format(time.RFC3339), "status", "inventory", "observedAt")
				case "wrong Pool UID":
					pool.SetUID("replacement-pool-uid")
				case "copy problem":
					copies, _, _ := unstructured.NestedSlice(pool.Object, "status", "inventory", "copies")
					copies[0].(map[string]any)["problem"] = "IdentityMismatch"
					_ = unstructured.SetNestedSlice(pool.Object, copies, "status", "inventory", "copies")
				}
			})
			d := f.reconciler.newVolumeDispatcher()
			defer d.queue.ShutDown()
			observations := newDiscoveryObservations(nil)
			scanAndProcessDiscovery(t, f, d, observations)
			now = now.Add(30 * time.Second)
			scanAndProcessDiscovery(t, f, d, observations)
			moves, err := f.registry.ListMoves(f.ctx)
			if err != nil || len(moves) != 0 {
				t.Fatalf("uncertain source authorized discovery: moves=%+v error=%v", moves, err)
			}
			assertNoEviction(t, f.client)
		})
	}
}
