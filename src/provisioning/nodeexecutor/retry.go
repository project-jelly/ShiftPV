package nodeexecutor

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type retryError struct{ error }

func (retryError) Retryable() bool { return true }
func (e retryError) Unwrap() error { return e.error }
func classifyError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		return retryError{err}
	}
	if errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) {
		return retryError{err}
	}
	return err
}
