package readiness

import (
	"context"
	"fmt"
	"time"

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
	ctx, cancel := context.WithTimeout(ctx, r.workTimeout())
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

func (r *Reconciler) workTimeout() time.Duration {
	if r.PoolTimeout == 0 {
		return defaultPoolTimeout
	}
	return r.PoolTimeout
}

// Each run must bound its own wait through poolWork. The buffered channel lets
// a canceled pass finish without leaving a sender behind a blocked host call.
func parallelPoolWork(ctx context.Context, pools []volumeapi.Pool, run func(context.Context, volumeapi.Pool) poolWorkResult) map[string]poolWorkResult {
	type answer struct {
		uid    string
		result poolWorkResult
	}
	completed := make(chan answer, len(pools))
	for _, pool := range pools {
		go func() { completed <- answer{pool.UID, run(ctx, pool)} }()
	}
	results := make(map[string]poolWorkResult, len(pools))
	for range pools {
		answer := <-completed
		results[answer.uid] = answer.result
	}
	return results
}

// Existing storage observations are independent. New registrations keep the
// serialized, fresh peer recheck before approval, within one budget per pass.
func (r *Reconciler) observeSelectedPools(ctx context.Context, pools []volumeapi.Pool, isolation map[string]allocationObservation, only map[string]bool) []Observation {
	var registered, candidates []volumeapi.Pool
	for _, pool := range pools {
		if pool.NodeName != r.NodeName || only != nil && !only[pool.UID] {
			continue
		}
		if !pool.RegistrationApproved() && pool.DeletionTimestamp == nil && !pool.RegistrationRetired() {
			candidates = append(candidates, pool)
		} else {
			registered = append(registered, pool)
		}
	}
	results := parallelPoolWork(ctx, registered, func(ctx context.Context, pool volumeapi.Pool) poolWorkResult {
		result, err := r.observePool(ctx, pool, isolation[pool.UID])
		return poolWorkResult{result: result, err: err}
	})
	observations := make([]Observation, 0, len(registered)+len(candidates))
	for _, pool := range registered {
		work := results[pool.UID]
		observations = append(observations, Observation{Pool: pool, Result: work.result, Err: work.err})
	}
	ctx, cancel := context.WithTimeout(ctx, r.workTimeout())
	defer cancel()
	for _, pool := range candidates {
		result, err := r.observePool(ctx, pool, isolation[pool.UID])
		observations = append(observations, Observation{Pool: pool, Result: result, Err: err})
	}
	return observations
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
