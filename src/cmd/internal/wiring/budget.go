package wiring

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/flowcontrol"
)

// NewAPIBudget preserves context cancellation and classifies local token-wait
// rejection as a timeout. client-go otherwise wraps an untyped x/time error
// when the next token would arrive after the request deadline.
func NewAPIBudget(qps float32, burst int) flowcontrol.RateLimiter {
	return apiBudget{flowcontrol.NewTokenBucketRateLimiter(qps, burst)}
}

type apiBudget struct{ flowcontrol.RateLimiter }

func (b apiBudget) Wait(ctx context.Context) error {
	err := b.RateLimiter.Wait(ctx)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return apierrors.NewTimeoutError(fmt.Sprintf("client API budget exhausted: %v", err), 0)
}
