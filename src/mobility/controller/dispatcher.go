package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const mobilityWorkers = 2

type moveReference struct{ name, uid string }
type podReference struct {
	namespace, name string
	uid             types.UID
}

// These references route work, never authorize it. Each writer re-reads its
// object in the Volume lane before deciding what is still allowed.
type volumeWork struct {
	volumeID  string
	volumeUID string
	discover  bool
	source    discoverySource
	moves     []moveReference
	ownerPods []podReference
}

type volumeDispatcher struct {
	mu         sync.Mutex
	hints      map[string]volumeWork
	queue      workqueue.TypedRateLimitingInterface[string]
	deferred   map[string]deferredDiscovery
	retryAfter time.Duration
	now        func() time.Time
}

func (d *volumeDispatcher) publish(work volumeWork) {
	d.mu.Lock()
	d.hints[work.volumeID] = mergeVolumeWork(d.hints[work.volumeID], work)
	d.mu.Unlock()
	d.queue.Add(work.volumeID)
}

func (d *volumeDispatcher) snapshot(id string) (volumeWork, time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	work := d.hints[id]
	work.moves = slices.Clone(work.moves)
	work.ownerPods = slices.Clone(work.ownerPods)
	if previous, exists := d.deferred[id]; exists {
		if work.discover && work.source == previous.source {
			if remaining := previous.until.Sub(d.now()); remaining > 0 {
				work.discover = false
				return work, remaining
			}
		}
		delete(d.deferred, id)
	}
	return work, 0
}

func (d *volumeDispatcher) replace(hints map[string]volumeWork) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hints = hints
	for id, previous := range d.deferred {
		if work := hints[id]; !work.discover || work.source != previous.source {
			delete(d.deferred, id)
		}
	}
}

func mergeVolumeWork(current, next volumeWork) volumeWork {
	current.volumeID = next.volumeID
	if next.volumeUID != "" {
		current.volumeUID = next.volumeUID
	}
	current.discover = current.discover || next.discover
	if next.discover {
		current.source = next.source
	}
	for _, ref := range next.moves {
		if !slices.Contains(current.moves, ref) {
			current.moves = append(current.moves, ref)
		}
	}
	for _, ref := range next.ownerPods {
		if !slices.Contains(current.ownerPods, ref) {
			current.ownerPods = append(current.ownerPods, ref)
		}
	}
	return current
}

func (r *Reconciler) reconcileInterval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return 2 * time.Second
}

func (r *Reconciler) newVolumeDispatcher() *volumeDispatcher {
	return &volumeDispatcher{
		hints:    map[string]volumeWork{},
		queue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		deferred: map[string]deferredDiscovery{}, retryAfter: r.reconcileInterval(), now: r.now,
	}
}

func (r *Reconciler) runVolumes(ctx context.Context) error {
	d := r.newVolumeDispatcher()
	observations := newDiscoveryObservations(r.ObserveDiscovery)
	var workers sync.WaitGroup
	for range mobilityWorkers {
		workers.Go(func() { r.volumeWorker(ctx, d, observations) })
	}
	// Cancellation unblocks queue.Get even when the scanner is in an API call.
	stopShutdown := context.AfterFunc(ctx, d.queue.ShutDown)
	defer func() {
		stopShutdown()
		d.queue.ShutDown()
		workers.Wait()
	}()
	ticker := time.NewTicker(r.reconcileInterval())
	defer ticker.Stop()
	for ctx.Err() == nil {
		hints, err := r.scanVolumeWork(ctx, d.publish, observations)
		if err == nil {
			d.replace(hints)
		} else if ctx.Err() == nil {
			klog.ErrorS(err, "scan ShiftPV mobility")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.Wake:
		case <-ticker.C:
		}
	}
	return nil
}

func (r *Reconciler) volumeWorker(ctx context.Context, d *volumeDispatcher, observations *discoveryObservations) {
	for {
		id, shutdown := d.queue.Get()
		if shutdown {
			return
		}
		if ctx.Err() == nil {
			r.processVolume(ctx, d, id, observations)
		}
		d.queue.Done(id)
	}
}

func (r *Reconciler) processVolume(ctx context.Context, d *volumeDispatcher, id string, observations *discoveryObservations) {
	work, remaining := d.snapshot(id)
	if remaining > 0 {
		// An older delayed entry for this reusable name may have fired before
		// the current source's deadline. Always preserve the remaining retry.
		d.queue.AddAfter(id, remaining)
	}
	result, err := r.advanceVolume(ctx, work)
	if delay := d.deferDiscovery(work, result); delay > 0 {
		d.queue.AddAfter(id, delay)
	}
	observations.record(id, result)
	switch {
	case errors.Is(err, errPoolBusy), errors.Is(err, ErrCapacityProbePending):
		d.queue.Forget(id)
		d.queue.AddAfter(id, r.reconcileInterval())
	case err != nil:
		klog.ErrorS(err, "reconcile ShiftPV mobility Volume", "volume", id)
		d.queue.AddRateLimited(id)
	default:
		d.queue.Forget(id)
	}
}

// ReconcileAll uses the same per-Volume path as Run, with one synchronous pass
// for callers that need its combined result. Do not run it alongside Run.
func (r *Reconciler) reconcileVolumePass(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	observations := newDiscoveryObservations(r.ObserveDiscovery)
	hints, scanErr := r.scanVolumeWork(ctx, nil, observations)
	ids := make([]string, 0, len(hints))
	for id := range hints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	failures := []error{scanErr}
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		result, err := r.advanceVolume(ctx, hints[id])
		observations.record(id, result)
		if err != nil {
			failures = append(failures, fmt.Errorf("volume %s: %w", id, err))
		}
	}
	return errors.Join(append(failures, ctx.Err())...)
}

func (r *Reconciler) advanceVolume(ctx context.Context, work volumeWork) (discoveryResult, error) {
	timeout := r.ReconcileTimeout
	if timeout <= 0 {
		timeout = DefaultReconcileTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var result discoveryResult
	var failures []error
	var pending error
	if work.discover && ctx.Err() == nil {
		created, observed, err := r.discoverVolume(ctx, work.volumeID)
		result = observed
		if result.uid == "" && err != nil {
			result = discoveryResult{uid: work.volumeUID, err: err}
		}
		failures = append(failures, err)
		if created.Name != "" && err == nil {
			work = mergeVolumeWork(work, volumeWork{volumeID: work.volumeID, moves: []moveReference{{created.Name, created.UID}}})
		}
	}
	for _, ref := range work.moves {
		if ctx.Err() != nil {
			break
		}
		err := r.advanceMoveReference(ctx, work.volumeID, ref)
		if errors.Is(err, errPoolBusy) || errors.Is(err, ErrCapacityProbePending) {
			pending = err
		} else if err != nil {
			failures = append(failures, fmt.Errorf("move %s: %w", ref.name, err))
		}
	}
	for _, ref := range work.ownerPods {
		if ctx.Err() != nil {
			break
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ref.namespace, Name: ref.name, UID: ref.uid}}
		if err := r.reconcileOwnerPlacementInVolume(ctx, work.volumeID, pod); err != nil {
			failures = append(failures, fmt.Errorf("restore Pod %s/%s owner placement: %w", ref.namespace, ref.name, err))
		}
	}
	if ctx.Err() != nil {
		return result, errors.Join(append(failures, pending, ctx.Err())...)
	}
	if err := errors.Join(failures...); err != nil {
		return result, err
	}
	return result, pending
}

func (r *Reconciler) advanceMoveReference(ctx context.Context, volumeID string, ref moveReference) error {
	move, err := r.Repository.GetMove(ctx, ref.name)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if move.UID != ref.uid || move.Spec.VolumeID != volumeID {
		return nil // A later scan must route the replacement under its own identity.
	}
	return r.reconcilePersistedMove(ctx, move)
}
