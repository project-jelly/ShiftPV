package remote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

type Discovery interface {
	Find(context.Context, string) (*volumeapi.NodeExecutor, error)
	Endpoint(context.Context, volumeapi.NodeExecutor) (connection.Target, bool, error)
}
type Pools interface {
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
}
type Probe interface {
	StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error)
}
type RPC interface {
	GetCapacity(context.Context, connection.Target, *protocol.CapacityRequest) (*protocol.CapacityResponse, error)
}

type Client struct {
	RPC         RPC
	Discovery   Discovery
	Pools       Pools
	Fallback    Probe
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
	evidence, err := c.verify(ctx, expected)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	executor, err := c.Discovery.Find(ctx, expected.NodeName)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if executor == nil {
		return c.Fallback.StatFSForPool(ctx, expected)
	}
	target, supported, err := c.Discovery.Endpoint(ctx, *executor)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if !supported {
		return c.Fallback.StatFSForPool(ctx, expected)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return capacity.Filesystem{}, err
	}
	req := &protocol.CapacityRequest{ExecutorUid: executor.PodUID, NodeName: expected.NodeName, PoolName: expected.Name, PoolUid: expected.UID, Evidence: evidence, Nonce: hex.EncodeToString(nonce[:])}
	answer, err := c.read(ctx, target, req)
	if err != nil {
		if status.Code(err) == codes.DeadlineExceeded && ctx.Err() == nil {
			return c.Fallback.StatFSForPool(ctx, expected)
		}
		return capacity.Filesystem{}, err
	}
	if answer == nil || answer.ExecutorUid != executor.PodUID || answer.Evidence != evidence || answer.Nonce != req.Nonce {
		return capacity.Filesystem{}, fmt.Errorf("Node capacity response identity changed")
	}
	stats := capacity.Filesystem{TotalBytes: answer.TotalBytes, AvailableBytes: answer.AvailableBytes, AvailableInodes: answer.AvailableInodes}
	if stats.TotalBytes <= 0 || stats.AvailableBytes < 0 || stats.AvailableBytes > stats.TotalBytes || stats.AvailableInodes < 0 {
		return capacity.Filesystem{}, fmt.Errorf("invalid Node filesystem capacity")
	}
	if _, err := c.verify(ctx, expected); err != nil {
		return capacity.Filesystem{}, err
	}
	return stats, nil
}
func (c *Client) verify(ctx context.Context, expected volumeapi.Pool) (string, error) {
	current, err := c.Pools.ReadyPoolForIdentity(ctx, expected.Name, expected.UID, expected.NodeName)
	if err != nil {
		return "", err
	}
	left, err := capacity.MeasurementEvidence(expected)
	if err != nil {
		return "", err
	}
	right, err := capacity.MeasurementEvidence(current)
	if err != nil || left != right {
		return "", fmt.Errorf("Pool measurement evidence changed")
	}
	return right, nil
}
func (c *Client) read(ctx context.Context, target connection.Target, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	answer, err := c.RPC.GetCapacity(ctx, target, req)
	klog.V(2).InfoS("ShiftPV Node RPC", "operation", "CAPACITY", "pool", req.PoolName, "executorUID", req.ExecutorUid, "error", err)
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		return nil, retryError{err}
	}
	return answer, err
}

type retryError struct{ error }

func (retryError) Retryable() bool { return true }
func (e retryError) Unwrap() error { return e.error }
