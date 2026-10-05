package measurement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type probeRegistry interface {
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	RequestPoolCapacityProbe(context.Context, volumeapi.Pool, string) error
}

type filesystemProbe interface {
	StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error)
}

// Client uses the authenticated Kubernetes API as request transport. Only the
// answer to this nonce can authorize allocation; ordinary status is no cache.
type Client struct {
	Pools       probeRegistry
	Fallback    filesystemProbe
	Timeout     time.Duration
	ObserveStep func(string, time.Duration)
	locks       capacity.Locker
}

func (c *Client) StatFSForPool(ctx context.Context, expected volumeapi.Pool) (capacity.Filesystem, error) {
	unlock := c.locks.Lock(expected.UID)
	defer unlock()
	started := time.Now()
	defer func() {
		duration := time.Since(started)
		klog.V(2).InfoS("ShiftPV provisioning step", "step", "live_capacity_probe", "duration", duration)
		if c.ObserveStep != nil {
			c.ObserveStep("live_capacity_probe", duration)
		}
	}()
	pool, err := c.Pools.ReadyPoolForIdentity(ctx, expected.Name, expected.UID, expected.NodeName)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	evidence, err := matchingEvidence(expected, pool)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if !pool.Status.CapacityProbeSupported {
		return c.Fallback.StatFSForPool(ctx, pool)
	}
	r, encoded, err := newRequest(evidence)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if err := c.Pools.RequestPoolCapacityProbe(ctx, pool, encoded); err != nil {
		return capacity.Filesystem{}, err
	}
	stats, err := c.await(ctx, pool, r)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		// A stopped/older Node cannot answer. The existing helper still reads
		// live space and verifies exact identity before any approval.
		return c.Fallback.StatFSForPool(ctx, pool)
	}
	return stats, err
}

func (c *Client) await(ctx context.Context, expected volumeapi.Pool, r request) (capacity.Filesystem, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var stats capacity.Filesystem
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(pollCtx context.Context) (bool, error) {
		pool, err := c.Pools.ReadyPoolForIdentity(pollCtx, expected.Name, expected.UID, expected.NodeName)
		if err != nil {
			return false, err
		}
		if _, err := matchingEvidence(expected, pool); err != nil {
			return false, err
		}
		answer := pool.Status.CapacityProbe
		if answer == nil || answer.RequestID != r.ID {
			return false, nil
		}
		if answer.Evidence != r.Evidence || pool.CapacityProbeRequest == "" {
			return false, fmt.Errorf("capacity probe answer evidence changed")
		}
		currentRequest, err := parseRequest(pool.CapacityProbeRequest)
		if err != nil || currentRequest != r {
			return false, fmt.Errorf("capacity probe request changed")
		}
		if answer.Error != "" {
			return false, retryableError{fmt.Errorf("Node capacity probe: %s", answer.Error)}
		}
		stats = capacity.Filesystem{TotalBytes: answer.TotalBytes, AvailableBytes: answer.AvailableBytes, AvailableInodes: answer.AvailableInodes}
		if stats.TotalBytes <= 0 || stats.AvailableBytes < 0 || stats.AvailableBytes > stats.TotalBytes || stats.AvailableInodes < 0 {
			return false, fmt.Errorf("Node capacity probe returned invalid space")
		}
		return true, nil
	})
	if ctx.Err() != nil {
		return capacity.Filesystem{}, ctx.Err()
	}
	return stats, err
}

func matchingEvidence(expected, current volumeapi.Pool) (string, error) {
	left, err := capacity.MeasurementEvidence(expected)
	if err != nil {
		return "", err
	}
	right, err := capacity.MeasurementEvidence(current)
	if err != nil || right != left {
		return "", fmt.Errorf("capacity probe Pool evidence changed")
	}
	return right, nil
}

type retryableError struct{ error }

func (retryableError) Retryable() bool { return true }
