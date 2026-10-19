package readiness

import (
	"context"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type poolWorkResult struct {
	allocation allocationObservation
	result     Result
	err        error
}

type poolWork struct {
	done   chan struct{}
	result poolWorkResult
}

// poolWork bounds the wait, not the syscall. Keep the UID occupied until the
// call really returns, so retries cannot accumulate blocked host operations.
func (r *Reconciler) poolWork(ctx context.Context, pool volumeapi.Pool, run func(context.Context) poolWorkResult) poolWorkResult {
	if err := ctx.Err(); err != nil {
		return poolWorkResult{err: err}
	}
	r.workMu.Lock()
	if r.inFlight == nil {
		r.inFlight = make(map[string]*poolWork)
	}
	if r.inFlight[pool.UID] != nil {
		r.workMu.Unlock()
		return poolWorkResult{err: fmt.Errorf("Pool %q previous observation is still running: %w", pool.Name, context.DeadlineExceeded)}
	}
	work := &poolWork{done: make(chan struct{})}
	r.inFlight[pool.UID] = work
	r.workMu.Unlock()
	timeout := r.PoolTimeout
	if timeout == 0 {
		timeout = defaultPoolTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	go func() {
		work.result = run(ctx)
		r.workMu.Lock()
		delete(r.inFlight, pool.UID)
		close(work.done)
		r.workMu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return poolWorkResult{err: ctx.Err()}
	case <-work.done:
		if err := ctx.Err(); err != nil {
			return poolWorkResult{err: err}
		}
		return work.result
	}
}

// A later pass starts a fresh observation after the worker exits. Observation
// callbacks belong to the caller, so a late worker cannot publish metrics.
func (r *Reconciler) observePool(ctx context.Context, pool volumeapi.Pool, allocation allocationObservation) (Result, error) {
	work := r.poolWork(ctx, pool, func(ctx context.Context) poolWorkResult {
		result, err := r.reconcilePool(ctx, pool, allocation)
		return poolWorkResult{result: result, err: err}
	})
	return work.result, work.err
}
