// Package creationcontroller retires unfinished creations whose recorded
// original claim has ended. Ready volumes remain under normal PV reclamation.
package creationcontroller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type Repository interface {
	Get(context.Context, string) (volumeapi.State, error)
	ListCreationIntents(context.Context) ([]volumeapi.CreationReference, error)
}

type ClaimRetirement interface {
	Retired(context.Context, string, volumeapi.State) (bool, error)
}

type Cancellation interface {
	Advance(context.Context, string, string) (bool, error)
}

type Reconciler struct {
	Volumes     Repository
	Claims      ClaimRetirement
	Cancel      Cancellation
	Interval    time.Duration
	WorkTimeout time.Duration
	Workers     int
}

type key struct{ name, uid string }

func candidate(state volumeapi.State) bool {
	// An exact creation-only CAS is already durable cancellation authority,
	// including an operator's explicit recovery of pre-provenance native work.
	if state.CancellingCreation() {
		return true
	}
	return state.CreationClaim != nil && state.CreationClaim.Valid() && state.RequestName == "pvc-"+state.CreationClaim.UID &&
		(state.UnstartedCreation() || state.CancellableCreation())
}

// Reconcile re-reads the exact Volume; discovery is never delete authority.
// A completion winner is preserved, even if the original PVC has since ended.
func (r *Reconciler) Reconcile(ctx context.Context, id, uid string) (bool, error) {
	state, err := r.Volumes.Get(ctx, id)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if state.UID != uid || !candidate(state) {
		return true, nil
	}
	if !state.CancellingCreation() {
		retired, err := r.Claims.Retired(ctx, id, state)
		if err != nil || !retired {
			return err == nil, err
		}
	}
	complete, err := r.Cancel.Advance(ctx, id, uid)
	if errors.Is(err, creation.ErrNotCancellable) {
		return true, nil
	}
	return complete, err
}

// Run uses a bounded per-Volume queue. An unavailable Node never occupies a
// synchronous RPC wait, and API stalls have per-item deadlines and fair workers.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.Volumes == nil || r.Claims == nil || r.Cancel == nil || r.Interval <= 0 || r.WorkTimeout <= 0 || r.Workers <= 0 {
		return fmt.Errorf("creation retirement reconciler is incompletely configured")
	}
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[key]())
	var workers sync.WaitGroup
	for range r.Workers {
		workers.Go(func() { r.work(ctx, queue) })
	}
	defer workers.Wait()
	defer queue.ShutDown()
	scan := func() {
		scanCtx, cancel := context.WithTimeout(ctx, r.WorkTimeout)
		defer cancel()
		intents, err := r.Volumes.ListCreationIntents(scanCtx)
		if err != nil {
			if ctx.Err() == nil {
				klog.Errorf("discover unfinished creations: %v", err)
			}
			return
		}
		for _, intent := range intents {
			queue.Add(key{name: intent.Name, uid: intent.UID})
		}
	}
	scan()
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			scan()
		}
	}
}

func (r *Reconciler) work(ctx context.Context, queue workqueue.TypedRateLimitingInterface[key]) {
	for {
		item, shutdown := queue.Get()
		if shutdown {
			return
		}
		workCtx, cancel := context.WithTimeout(ctx, r.WorkTimeout)
		complete, err := r.Reconcile(workCtx, item.name, item.uid)
		cancel()
		if err != nil && ctx.Err() == nil {
			klog.Errorf("retire unfinished creation %s/%s: %v", item.name, item.uid, err)
			queue.AddRateLimited(item)
		} else {
			queue.Forget(item)
			if !complete && ctx.Err() == nil {
				queue.AddAfter(item, time.Second)
			}
		}
		queue.Done(item)
	}
}
