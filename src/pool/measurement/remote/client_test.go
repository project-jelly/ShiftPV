package remote

import (
	"context"
	"errors"
	"testing"
	"time"

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
			c.FallbackOnUnavailable = true
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

func TestFallbackOnUnavailableIsOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, code := range []codes.Code{codes.DeadlineExceeded, codes.Unavailable, codes.Unimplemented, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Canceled, codes.Internal} {
			name := "default/" + code.String()
			if enabled {
				name = "opt-in/" + code.String()
			}
			t.Run(name, func(t *testing.T) {
				c, p, r, f := fixture()
				c.FallbackOnUnavailable = enabled
				r.fail = status.Error(code, "probe failed")
				stats, err := c.StatFSForPool(context.Background(), p.pool)
				wantFallback := code == codes.DeadlineExceeded || enabled && (code == codes.Unavailable || code == codes.Unimplemented)
				if wantFallback {
					if err != nil || stats.AvailableBytes != 30 || f.calls != 1 {
						t.Fatalf("fallback not used: stats=%+v calls=%d err=%v", stats, f.calls, err)
					}
				} else if !errors.Is(err, r.fail) || status.Code(err) != code || f.calls != 0 || stats != (capacity.Filesystem{}) {
					t.Fatalf("RPC failure changed or fell back: stats=%+v calls=%d err=%v", stats, f.calls, err)
				}
			})
		}
	}
}

type rpcFunc func(context.Context, connection.Target, *protocol.CapacityRequest) (*protocol.CapacityResponse, error)

func (f rpcFunc) GetCapacity(ctx context.Context, target connection.Target, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	return f(ctx, target, req)
}

type probeFunc func(context.Context, volumeapi.Pool) (capacity.Filesystem, error)

func (f probeFunc) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
	return f(ctx, pool)
}

type cancelingDiscovery struct {
	discovery
	cancel context.CancelFunc
}

func (d cancelingDiscovery) Find(ctx context.Context, node string) (*volumeapi.NodeExecutor, error) {
	d.cancel()
	return d.discovery.Find(ctx, node)
}

func TestFallbackNeverOutlivesCaller(t *testing.T) {
	for _, mode := range []string{"no resident", "unsupported endpoint", "deadline", "unavailable", "unimplemented"} {
		t.Run(mode, func(t *testing.T) {
			c, p, _, f := fixture()
			c.FallbackOnUnavailable = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "no resident" || mode == "unsupported endpoint" {
				d := discovery{}
				if mode == "unsupported endpoint" {
					d.identity = &volumeapi.NodeExecutor{}
				}
				c.Discovery = cancelingDiscovery{discovery: d, cancel: cancel}
			} else {
				code := map[string]codes.Code{"deadline": codes.DeadlineExceeded, "unavailable": codes.Unavailable, "unimplemented": codes.Unimplemented}[mode]
				c.RPC = rpcFunc(func(context.Context, connection.Target, *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
					cancel()
					return nil, status.Error(code, "transport stopped")
				})
			}
			stats, err := c.StatFSForPool(ctx, p.pool)
			if !errors.Is(err, context.Canceled) || f.calls != 0 || stats != (capacity.Filesystem{}) {
				t.Fatalf("caller cancellation escaped into fallback: stats=%+v calls=%d err=%v", stats, f.calls, err)
			}
		})
	}
}

func TestFallbackSharesWholeCallContext(t *testing.T) {
	c, p, _, _ := fixture()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Timeout = time.Millisecond
	c.RPC = rpcFunc(func(readCtx context.Context, _ connection.Target, _ *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
		<-readCtx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "RPC read budget expired")
	})
	calls := 0
	c.Fallback = probeFunc(func(fallbackCtx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
		calls++
		if fallbackCtx != ctx || fallbackCtx.Err() != nil || pool.UID != p.pool.UID {
			t.Fatalf("fallback restarted or replaced the caller budget: ctx=%v pool=%+v", fallbackCtx.Err(), pool)
		}
		return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 30}, nil
	})
	stats, err := c.StatFSForPool(ctx, p.pool)
	if err != nil || calls != 1 || stats.AvailableBytes != 30 {
		t.Fatalf("shared-budget fallback failed: stats=%+v calls=%d err=%v", stats, calls, err)
	}
}

func TestNonBlockingPoolIsolation(t *testing.T) {
	c, p, r, f := fixture()
	c.NonBlocking = true
	busyPool := p.pool
	busyPool.Name, busyPool.UID = "busy-pool", "busy-pool-uid"
	unlock := c.locks.Lock(busyPool.UID)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, err := c.StatFSForPool(context.Background(), busyPool)
		result <- err
	}()
	select {
	case err := <-result:
		var retryable interface{ Retryable() bool }
		if !errors.As(err, &retryable) || !retryable.Retryable() || p.reads != 0 || r.calls != 0 || f.calls != 0 {
			t.Fatalf("busy probe touched a transport or did not retry: reads=%d RPC=%d fallback=%d err=%v", p.reads, r.calls, f.calls, err)
		}
	case <-time.After(time.Second):
		t.Fatal("busy probe waited for the Pool lock")
	}
	if stats, err := c.StatFSForPool(context.Background(), p.pool); err != nil || stats.AvailableBytes != 40 {
		t.Fatalf("independent Pool failed: stats=%+v err=%v", stats, err)
	}
	unlock()
	unlock = nil
	p.pool = busyPool
	if stats, err := c.StatFSForPool(context.Background(), busyPool); err != nil || stats.AvailableBytes != 40 || r.calls != 2 {
		t.Fatalf("fresh retry failed after lock release: stats=%+v RPC=%d err=%v", stats, r.calls, err)
	}
}

func TestDefaultWaitsForPoolLock(t *testing.T) {
	c, p, r, f := fixture()
	unlock := c.locks.Lock(p.pool.UID)
	result := make(chan error, 1)
	go func() {
		_, err := c.StatFSForPool(context.Background(), p.pool)
		result <- err
	}()
	select {
	case err := <-result:
		unlock()
		t.Fatalf("default probe did not wait for lock: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-result:
		if err != nil || r.calls != 1 || f.calls != 0 {
			t.Fatalf("default RPC changed: RPC=%d fallback=%d err=%v", r.calls, f.calls, err)
		}
	case <-time.After(time.Second):
		t.Fatal("default probe did not resume after lock release")
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
