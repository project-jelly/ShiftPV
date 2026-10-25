package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
)

func scheduledDiscoveryWork() volumeWork {
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: id, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "source", Role: volume.RoleServing}
	return volumeWork{volumeID: id, volumeUID: copy.VolumeUID, discover: true,
		source: discoverySource{volumeUID: copy.VolumeUID, ownerNode: "source", nodeUID: "node-uid", copy: copy}}
}

func TestDiscoveryDelayDoesNotExtendOnEarlyWakeOrHideOtherWork(t *testing.T) {
	now := time.Now()
	r := &Reconciler{Interval: 30 * time.Second, Now: func() time.Time { return now }}
	d := r.newVolumeDispatcher()
	defer d.queue.ShutDown()
	work := scheduledDiscoveryWork()
	d.publish(work)
	result := discoveryResult{uid: work.volumeUID, reason: "MultipleConsumers"}
	if delay := d.deferDiscovery(work, result); delay != 30*time.Second {
		t.Fatal(delay)
	}
	now = now.Add(10 * time.Second)
	d.publish(volumeWork{volumeID: work.volumeID, moves: []moveReference{{"move", "move-uid"}}, ownerPods: []podReference{{"ns", "pod", "pod-uid"}}})
	current, remaining := d.snapshot(work.volumeID)
	if current.discover || remaining != 20*time.Second || len(current.moves) != 1 || len(current.ownerPods) != 1 {
		t.Fatalf("delay swallowed independent work: %+v remaining=%v", current, remaining)
	}
	if delay := d.deferDiscovery(current, result); delay != 0 {
		t.Fatal("early wake extended retry", delay)
	}
	now = now.Add(20 * time.Second)
	current, remaining = d.snapshot(work.volumeID)
	if !current.discover || remaining != 0 {
		t.Fatal("deadline did not permit a live retry", current, remaining)
	}
}

func TestDiscoveryDelayRejectsLateOrUncertainObservations(t *testing.T) {
	for _, change := range []string{"Volume UID", "owner", "copy", "Pool UID", "Node UID", "source proof", "active work", "removed", "API error", "successful observation", "unknown incarnation"} {
		t.Run(change, func(t *testing.T) {
			r := &Reconciler{Interval: time.Hour}
			d := r.newVolumeDispatcher()
			defer d.queue.ShutDown()
			work := scheduledDiscoveryWork()
			d.publish(work)
			result := discoveryResult{uid: work.volumeUID, reason: "MultipleConsumers"}
			d.deferDiscovery(work, result)
			next := work
			switch change {
			case "Volume UID":
				next.volumeUID = "new-uid"
				next.source.volumeUID = "new-uid"
				next.source.copy.VolumeUID = "new-uid"
			case "owner":
				next.source.ownerNode = "new-owner"
			case "copy":
				next.source.copy.CopyID = "new-copy"
			case "Pool UID":
				next.source.copy.PoolUID = "new-pool"
			case "Node UID":
				next.source.nodeUID = "new-node"
			case "source proof":
				next.source.sourceAvailable = !next.source.sourceAvailable
			case "active work":
				next.discover = false
			case "removed":
			case "API error":
				result.err = errors.New("observation unavailable")
			case "successful observation":
				result.reason = ""
			case "unknown incarnation":
				next.source.volumeUID = ""
				result.uid = ""
			}
			if change == "removed" {
				d.replace(map[string]volumeWork{})
			} else {
				d.replace(map[string]volumeWork{work.volumeID: next})
			}
			d.deferDiscovery(work, result)
			if len(d.deferred) != 0 {
				t.Fatalf("late/unknown observation retained a delay: %+v", d.deferred)
			}
			_, remaining := d.snapshot(work.volumeID)
			if remaining != 0 {
				t.Fatal("changed routing was postponed", remaining)
			}
		})
	}
}

type delayedRetryRecorder struct {
	workqueue.TypedRateLimitingInterface[string]
	delays []time.Duration
}

func (q *delayedRetryRecorder) AddAfter(id string, delay time.Duration) {
	q.delays = append(q.delays, delay)
	q.TypedRateLimitingInterface.AddAfter(id, delay)
}

func TestEarlyTimerForReusedNamePreservesNewIncarnationDeadline(t *testing.T) {
	now := time.Now()
	r := &Reconciler{Interval: 30 * time.Second, Now: func() time.Time { return now }}
	d := r.newVolumeDispatcher()
	defer d.queue.ShutDown()
	old := scheduledDiscoveryWork()
	d.publish(old)
	d.deferDiscovery(old, discoveryResult{uid: old.volumeUID, reason: "MultipleConsumers"})
	d.queue.AddAfter(old.volumeID, 30*time.Second)
	now = now.Add(10 * time.Second)
	next := old
	next.volumeUID, next.source.volumeUID, next.source.copy.VolumeUID = "replacement", "replacement", "replacement"
	d.replace(map[string]volumeWork{next.volumeID: next})
	d.deferDiscovery(next, discoveryResult{uid: next.volumeUID, reason: "MultipleConsumers"})
	d.queue.AddAfter(next.volumeID, 30*time.Second)
	// The delaying queue coalesces by reusable name and keeps the earlier
	// deadline. Model its old wake while the replacement is still waiting.
	now = now.Add(20 * time.Second)
	retries := &delayedRetryRecorder{TypedRateLimitingInterface: d.queue}
	d.queue = retries
	r.processVolume(context.Background(), d, next.volumeID, newDiscoveryObservations(nil))
	if len(retries.delays) != 1 || retries.delays[0] != 10*time.Second {
		t.Fatalf("old timer consumed the new incarnation's only retry: %v", retries.delays)
	}
}

func deferredRegistryFixture(t *testing.T, count int) (*preparationRegistryFixture, []*corev1.Pod) {
	t.Helper()
	f := newPreparationRegistryFixture(t, count)
	var extraPods []*corev1.Pod
	for index, name := range f.moveNames {
		if err := f.dynamic.Tracker().Delete(volumeapi.MoveResource, "", name); err != nil {
			t.Fatal(err)
		}
		extra := f.workloads[index].Consumer.DeepCopy()
		extra.Name += "-extra"
		extra.UID += "-extra"
		created, err := f.client.CoreV1().Pods(extra.Namespace).Create(f.ctx, extra, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		extraPods = append(extraPods, created)
	}
	return f, extraPods
}

func TestDiscoveryScanCarriesTheExactRoutingIncarnation(t *testing.T) {
	f, _ := deferredRegistryFixture(t, 1)
	node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.UID = "first-node"
	if _, err := f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observations := newDiscoveryObservations(nil)
	hints, err := f.reconciler.scanVolumeWork(f.ctx, nil, observations)
	if err != nil {
		t.Fatal(err)
	}
	work := hints[f.volumeIDs[0]]
	state := f.state(t, 0)
	if work.source.volumeUID != state.UID || work.source.ownerNode != state.OwnerNode || work.source.nodeUID != "first-node" || work.source.copy != *state.CurrentCopy {
		t.Fatalf("incomplete routing identity: %+v", work.source)
	}
	d := f.reconciler.newVolumeDispatcher()
	defer d.queue.ShutDown()
	d.replace(hints)
	d.deferDiscovery(work, discoveryResult{uid: state.UID, reason: "MultipleConsumers"})
	node.UID = "replacement-node"
	if _, err := f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	hints, err = f.reconciler.scanVolumeWork(f.ctx, nil, observations)
	if err != nil {
		t.Fatal(err)
	}
	d.replace(hints)
	current, remaining := d.snapshot(work.volumeID)
	if !current.discover || remaining != 0 || current.source.nodeUID != "replacement-node" {
		t.Fatal("node name reuse retained the old delay", current, remaining)
	}
}

func scanAndProcessDiscovery(t *testing.T, f *preparationRegistryFixture, d *volumeDispatcher, observations *discoveryObservations) {
	t.Helper()
	hints, err := f.reconciler.scanVolumeWork(f.ctx, d.publish, observations)
	if err != nil {
		t.Fatal(err)
	}
	d.replace(hints)
	for count := d.queue.Len(); count > 0; count-- {
		id, shutdown := d.queue.Get()
		if shutdown {
			t.Fatal("queue closed before work")
		}
		f.reconciler.processVolume(f.ctx, d, id, observations)
		d.queue.Done(id)
	}
}

func TestDeferredDiscoveryRechecksLiveEligibilityAtDeadline(t *testing.T) {
	for _, change := range []string{"eligible again", "binding changed", "API error", "restart"} {
		t.Run(change, func(t *testing.T) {
			f, extra := deferredRegistryFixture(t, 1)
			now := time.Now()
			f.reconciler.Now = func() time.Time { return now }
			f.reconciler.Interval = 30 * time.Second
			d := f.reconciler.newVolumeDispatcher()
			defer d.queue.ShutDown()
			observations := newDiscoveryObservations(nil)
			before := f.state(t, 0)
			scanAndProcessDiscovery(t, f, d, observations)
			if len(d.deferred) != 1 {
				t.Fatal("fixture did not defer")
			}
			if err := f.client.CoreV1().Pods(extra[0].Namespace).Delete(f.ctx, extra[0].Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if change == "binding changed" {
				pvc, err := f.client.CoreV1().PersistentVolumeClaims(f.workloads[0].Claim.Namespace).Get(f.ctx, f.workloads[0].Claim.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pvc.Spec.VolumeName = "foreign-pv"
				if _, err := f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(f.ctx, pvc, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if change == "API error" {
				f.client.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewInternalError(errors.New("uncertain PVC"))
				})
			}
			now = now.Add(10 * time.Second)
			if change == "restart" {
				// Scheduling is process-local. Reconstruct from durable state;
				// neither the old delay nor its former verdict survives restart.
				d.queue.ShutDown()
				d = f.reconciler.newVolumeDispatcher()
				defer d.queue.ShutDown()
				observations = newDiscoveryObservations(nil)
			}
			scanAndProcessDiscovery(t, f, d, observations)
			moves, _ := f.registry.ListMoves(f.ctx)
			if change != "restart" && len(moves) != 0 {
				t.Fatal("early wake bypassed deadline")
			}
			now = now.Add(20 * time.Second)
			scanAndProcessDiscovery(t, f, d, observations)
			moves, err := f.registry.ListMoves(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if (len(moves) == 1) != (change == "eligible again" || change == "restart") {
				t.Fatalf("live eligibility was not checked: %+v", moves)
			}
			after := f.state(t, 0)
			if after.UID != before.UID || after.OwnerNode != before.OwnerNode || !reflect.DeepEqual(after.CurrentCopy, before.CurrentCopy) {
				t.Fatal("discovery transferred owner/copy authority")
			}
			if change == "API error" && len(d.deferred) != 0 {
				t.Fatal("API uncertainty became a cached verdict")
			}
			assertNoEviction(t, f.client)
		})
	}
}

func TestDeferredDiscoveryDoesNotDelayAnExistingMoveInTheSameLane(t *testing.T) {
	f, extra := deferredRegistryFixture(t, 1)
	f.reconciler.Interval = time.Hour
	d := f.reconciler.newVolumeDispatcher()
	defer d.queue.ShutDown()
	observations := newDiscoveryObservations(nil)
	scanAndProcessDiscovery(t, f, d, observations)
	if len(d.deferred) != 1 {
		t.Fatal("fixture did not defer")
	}
	if err := f.client.CoreV1().Pods(extra[0].Namespace).Delete(f.ctx, extra[0].Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	move, err := f.registry.CreateMove(f.ctx, "manual-", volumeapi.MoveSpec{VolumeID: f.volumeIDs[0], SourceNode: "source", ProtocolVersion: volumeapi.MoveProtocolPreparedV1})
	if err != nil {
		t.Fatal(err)
	}
	d.publish(volumeWork{volumeID: f.volumeIDs[0], moves: []moveReference{{move.Name, move.UID}}})
	id, closed := d.queue.Get()
	if closed {
		t.Fatal("queue closed")
	}
	f.reconciler.processVolume(f.ctx, d, id, observations)
	d.queue.Done(id)
	current, err := f.registry.GetMove(f.ctx, move.Name)
	if err != nil || current.Status.SourceCopy == nil {
		t.Fatalf("active work was postponed by discovery: %+v %v", current, err)
	}
	assertNoEviction(t, f.client)
}

func TestDeferredDiscoveryTimerRetriesWithoutAnotherCoordinatorWake(t *testing.T) {
	f, extra := deferredRegistryFixture(t, 1)
	f.reconciler.Interval = 20 * time.Millisecond
	d := f.reconciler.newVolumeDispatcher()
	observations := newDiscoveryObservations(nil)
	hints, err := f.reconciler.scanVolumeWork(f.ctx, d.publish, observations)
	if err != nil {
		t.Fatal(err)
	}
	d.replace(hints)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.reconciler.volumeWorker(ctx, d, observations) }()
	t.Cleanup(func() {
		cancel()
		d.queue.ShutDown()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	})
	waitDispatcherCondition(t, "initial deferral was not recorded", func() bool { d.mu.Lock(); defer d.mu.Unlock(); return len(d.deferred) > 0 })
	if err := f.client.CoreV1().Pods(extra[0].Namespace).Delete(f.ctx, extra[0].Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitDispatcherCondition(t, "delay expired without a fresh retry", func() bool { moves, err := f.registry.ListMoves(f.ctx); return err == nil && len(moves) == 1 })
}

func TestDeferredDiscoveryCostAcrossRepeatedScans(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "previous_every_wake", true: "bounded_retry"}[enabled], func(t *testing.T) {
			f, _ := deferredRegistryFixture(t, 4)
			now := time.Now()
			f.reconciler.Now = func() time.Time { return now }
			f.reconciler.Interval = 30 * time.Second
			d := f.reconciler.newVolumeDispatcher()
			defer d.queue.ShutDown()
			if !enabled {
				d.retryAfter = 0
			}
			observations := newDiscoveryObservations(nil)
			f.client.ClearActions()
			f.dynamic.ClearActions()
			firstPassGets := 0
			firstPassPoolLists := 0
			for pass := range 20 {
				scanAndProcessDiscovery(t, f, d, observations)
				if pass == 0 {
					for _, a := range f.dynamic.Actions() {
						if a.GetVerb() == "get" && a.GetResource().Resource == "shiftpvvolumes" {
							firstPassGets++
						}
						if a.GetVerb() == "list" && a.GetResource() == volumeapi.PoolResource {
							firstPassPoolLists++
						}
					}
				}
				now = now.Add(time.Second)
			}
			counts := map[string]int{}
			writes := 0
			for _, a := range f.client.Actions() {
				counts["typed:"+a.GetVerb()+":"+a.GetResource().Resource]++
				if a.GetVerb() != "get" && a.GetVerb() != "list" {
					writes++
				}
			}
			for _, a := range f.dynamic.Actions() {
				counts["dynamic:"+a.GetVerb()+":"+a.GetResource().Resource]++
				if a.GetVerb() != "get" && a.GetVerb() != "list" {
					writes++
				}
			}
			if writes != 0 {
				t.Fatal("deferred discovery performed writes", writes)
			}
			expected := firstPassGets * 20
			if enabled {
				expected = firstPassGets
			}
			if firstPassGets == 0 || counts["dynamic:get:shiftpvvolumes"] != expected {
				t.Fatalf("reevaluation count=%d want=%d", counts["dynamic:get:shiftpvvolumes"], expected)
			}
			// Each complete scan adds one bulk source-proof observation, never
			// one per deferred Volume. Unchanged workers still skip preflight.
			expectedPoolLists := firstPassPoolLists * 20
			if enabled {
				expectedPoolLists = firstPassPoolLists + 19
			}
			if firstPassPoolLists != 5 || counts["dynamic:list:shiftpvpools"] != expectedPoolLists {
				t.Fatalf("source-proof observation was not batched: first=%d total=%d want=%d", firstPassPoolLists, counts["dynamic:list:shiftpvpools"], expectedPoolLists)
			}
			t.Logf("four unchanged deferred Volumes,20 complete scan/work cycles within one retry interval: typed=%d dynamic=%d total=%d counts=%v", len(f.client.Actions()), len(f.dynamic.Actions()), len(f.client.Actions())+len(f.dynamic.Actions()), counts)
			for _, result := range observations.results {
				if result.reason != "MultipleConsumers" || result.err != nil {
					t.Fatal("suppression erased the deferral diagnostic", result)
				}
			}
		})
	}
}
