package cleanupapi

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// WaitForAbsence waits only for the existing causal proof. A local timeout
// leaves the journal and its capacity hold intact; caller cancellation wins.
func (s *Store) WaitForAbsence(ctx context.Context, expected Cleanup, timeout time.Duration) (Cleanup, bool, error) {
	if timeout <= 0 {
		return expected, false, nil
	}
	current, settled := expected, false
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(pollCtx context.Context) (bool, error) {
		next, complete, err := s.ReconcileAbsence(pollCtx, current)
		if err == nil {
			current, settled = next, complete
		}
		return settled, err
	})
	if ctx.Err() != nil {
		return current, false, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return current, false, nil
	}
	return current, settled, err
}
