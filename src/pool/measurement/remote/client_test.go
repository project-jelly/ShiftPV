package remote

import (
	"context"
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type discovery struct {
	identity  *volumeapi.NodeExecutor
	supported bool
	fail      error
}

func (d discovery) Find(context.Context, string) (*volumeapi.NodeExecutor, error) {
	return d.identity, d.fail
}
func (d discovery) Endpoint(context.Context, volumeapi.NodeExecutor) (connection.Target, bool, error) {
	return connection.Target{}, d.supported, d.fail
}

type pools struct {
	pool   volumeapi.Pool
	reads  int
	change bool
}

func (p *pools) ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	p.reads++
	value := p.pool
	if p.change && p.reads > 1 {
		value.Generation++
	}
	return value, nil
}

type fallback struct{ calls int }

func (p *fallback) StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
	p.calls++
	return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 30, AvailableInodes: 10}, nil
}

type rpc struct {
	calls  int
	change func(*protocol.CapacityResponse)
	fail   error
}

func (p *rpc) GetCapacity(_ context.Context, _ connection.Target, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	p.calls++
	answer := &protocol.CapacityResponse{ExecutorUid: req.ExecutorUid, Evidence: req.Evidence, Nonce: req.Nonce, TotalBytes: 100, AvailableBytes: 40, AvailableInodes: 10}
	if p.change != nil {
		p.change(answer)
	}
	return answer, p.fail
}
func fixture() (*Client, *pools, *rpc, *fallback) {
	p := &pools{pool: volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/a", Generation: 1}}
	r := &rpc{}
	f := &fallback{}
	return &Client{Pools: p, RPC: r, Fallback: f, Discovery: discovery{identity: &volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-uid", NodeName: "worker-a"}, supported: true}}, p, r, f
}
func TestCapacityDoesNotAcceptStaleOrUnrelatedReply(t *testing.T) {
	for _, test := range []struct {
		name       string
		change     func(*protocol.CapacityResponse)
		poolChange bool
	}{
		{name: "fresh"},
		{name: "nonce", change: func(r *protocol.CapacityResponse) { r.Nonce = "old" }},
		{name: "executor", change: func(r *protocol.CapacityResponse) { r.ExecutorUid = "retired" }},
		{name: "evidence", change: func(r *protocol.CapacityResponse) { r.Evidence = "old" }},
		{name: "over-total", change: func(r *protocol.CapacityResponse) { r.AvailableBytes = 101 }},
		{name: "negative-inodes", change: func(r *protocol.CapacityResponse) { r.AvailableInodes = -1 }},
		{name: "registration changed during read", poolChange: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, p, r, f := fixture()
			r.change = test.change
			p.change = test.poolChange
			stats, err := c.StatFSForPool(context.Background(), p.pool)
			if test.name == "fresh" {
				if err != nil || stats.AvailableBytes != 40 || p.reads != 2 {
					t.Fatalf("stats=%+v reads=%d error=%v", stats, p.reads, err)
				}
			} else if err == nil {
				t.Fatal("stale capacity authorized")
			}
			if r.calls != 1 || f.calls != 0 {
				t.Fatal("RPC authority failure switched backend")
			}
		})
	}
}
func TestReadonlyFallbackAndCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		discovery discovery
		err       error
		fallback  bool
	}{
		{name: "no resident node", fallback: true},
		{name: "old resident node", discovery: discovery{identity: &volumeapi.NodeExecutor{}}, fallback: true},
		{name: "read timeout", discovery: discovery{identity: &volumeapi.NodeExecutor{}, supported: true}, err: status.Error(codes.DeadlineExceeded, "timeout"), fallback: true},
		{name: "authorization", discovery: discovery{identity: &volumeapi.NodeExecutor{}, supported: true}, err: status.Error(codes.PermissionDenied, "denied")},
		{name: "unavailable", discovery: discovery{identity: &volumeapi.NodeExecutor{}, supported: true}, err: status.Error(codes.Unavailable, "offline")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, p, r, f := fixture()
			c.Discovery = test.discovery
			r.fail = test.err
			_, err := c.StatFSForPool(context.Background(), p.pool)
			if test.fallback {
				if err != nil || f.calls != 1 {
					t.Fatalf("fallback=%d err=%v", f.calls, err)
				}
			} else if err == nil || f.calls != 0 {
				t.Fatal("authority failure fell back")
			}
			if test.name == "unavailable" {
				var retryable interface{ Retryable() bool }
				if !errors.As(err, &retryable) || !retryable.Retryable() {
					t.Fatal("transient RPC failure is not retryable")
				}
			}
		})
	}
	c, p, r, f := fixture()
	r.fail = status.Error(codes.DeadlineExceeded, "timeout")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.StatFSForPool(ctx, p.pool)
	if err == nil || f.calls != 0 {
		t.Fatal("caller cancellation fell back")
	}
}
