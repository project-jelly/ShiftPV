package volumeapi

import (
	"context"
	"fmt"
	"math"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// RequestPoolScan advances the exact protected Pool's scan epoch and returns
// the API generation a later inventory must reach. Callers persist this fence;
// an ambiguous write or lost fence is retried with another epoch increment.
func (r *Registry) RequestPoolScan(ctx context.Context, target volume.CopyIdentity) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	var generation int64
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		resource := r.Client.Resource(PoolResource)
		pool, err := resource.Get(ctx, target.PoolName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(pool.GetUID()) != target.PoolUID || !slices.Contains(pool.GetFinalizers(), PoolProtectionFinalizer) {
			return fmt.Errorf("%w: Pool identity or protection changed", ErrStateConflict)
		}
		nodeName, _, err := unstructured.NestedString(pool.Object, "spec", "nodeName")
		if err != nil || nodeName != target.NodeName {
			return fmt.Errorf("%w: Pool node changed", ErrStateConflict)
		}
		epoch, found, err := unstructured.NestedInt64(pool.Object, "spec", "scanEpoch")
		if err != nil {
			return err
		}
		if !found {
			epoch = 0
		}
		if epoch < 0 || epoch == math.MaxInt64 {
			return fmt.Errorf("%w: Pool scan epoch is exhausted", ErrStateConflict)
		}
		if err := unstructured.SetNestedField(pool.Object, epoch+1, "spec", "scanEpoch"); err != nil {
			return err
		}
		previousGeneration := pool.GetGeneration()
		updated, err := resource.Update(ctx, pool, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		if updated.GetGeneration() <= previousGeneration || updated.GetGeneration() <= 0 {
			return fmt.Errorf("%w: Pool scan request did not advance generation", ErrStateConflict)
		}
		generation = updated.GetGeneration()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("request Pool scan: %w", err)
	}
	return generation, nil
}
