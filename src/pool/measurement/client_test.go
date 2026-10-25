package measurement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type fakePools struct {
	pool      volumeapi.Pool
	onRequest func(request)
	requests  int
	reads     int
	err       error
}

func (f *fakePools) ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	f.reads++
	return f.pool, f.err
}

func (f *fakePools) RequestPoolCapacityProbe(_ context.Context, _ volumeapi.Pool, encoded string) error {
	r, err := parseRequest(encoded)
	if err != nil {
		return err
	}
	f.requests++
	f.pool.CapacityProbeRequest = encoded
	if f.onRequest != nil {
		f.onRequest(r)
	}
	return nil
}

func (f *fakePools) RecordPoolCapacityProbe(_ context.Context, _ volumeapi.Pool, result volumeapi.PoolCapacityProbeResult) error {
	f.pool.Status.CapacityProbe = &result
	return f.err
}

type fakeProbe struct {
	calls int
	stats capacity.Filesystem
	err   error
}

func (f *fakeProbe) StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
	f.calls++
	return f.stats, f.err
}

func testPool() volumeapi.Pool {
	return volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool", Generation: 1,
		Status: volumeapi.PoolStatus{CapacityProbeSupported: true}}
}

func TestClientRequiresAnExactFreshAnswer(t *testing.T) {
	for _, mode := range []string{"fresh", "stale", "old-node", "bad-evidence", "changed-pool", "invalid-space", "node-error", "superseded", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			pool := testPool()
			pools := &fakePools{pool: pool}
			fallback := &fakeProbe{stats: capacity.Filesystem{TotalBytes: 100, AvailableBytes: 10}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pools.onRequest = func(r request) {
				answer := volumeapi.PoolCapacityProbeResult{RequestID: r.ID, Evidence: r.Evidence, TotalBytes: 100, AvailableBytes: 50}
				switch mode {
				case "stale":
					answer.RequestID = strings.Repeat("0", 32)
				case "bad-evidence":
					answer.Evidence = strings.Repeat("0", 64)
				case "changed-pool":
					pools.pool.Generation++
				case "invalid-space":
					answer.AvailableBytes = 101
				case "node-error":
					answer.Error = "mount missing"
				case "superseded":
					_, pools.pool.CapacityProbeRequest, _ = newRequest(r.Evidence)
				case "canceled":
					cancel()
				}
				pools.pool.Status.CapacityProbe = &answer
			}
			if mode == "old-node" {
				pools.pool.Status.CapacityProbeSupported = false
			}
			client := &Client{Pools: pools, Fallback: fallback, Timeout: 10 * time.Millisecond}
			stats, err := client.StatFSForPool(ctx, pool)
			switch mode {
			case "fresh":
				if err != nil || stats.AvailableBytes != 50 || fallback.calls != 0 || pools.requests != 1 {
					t.Fatalf("stats=%+v requests=%d fallback=%d err=%v", stats, pools.requests, fallback.calls, err)
				}
			case "stale", "old-node":
				if err != nil || stats.AvailableBytes != 10 || fallback.calls != 1 {
					t.Fatalf("cached answer approved allocation: stats=%+v fallback=%d err=%v", stats, fallback.calls, err)
				}
			default:
				if err == nil || fallback.calls != 0 {
					t.Fatalf("ambiguous answer accepted: stats=%+v fallback=%d err=%v", stats, fallback.calls, err)
				}
				if mode == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
				if mode == "node-error" {
					var retryable interface{ Retryable() bool }
					if !errors.As(err, &retryable) || !retryable.Retryable() {
						t.Fatalf("temporary read error cannot retry: %v", err)
					}
				}
			}
		})
	}
}

func TestRequestRejectsMalformedAndOversizedInput(t *testing.T) {
	for _, encoded := range []string{"", "{}", strings.Repeat("x", 513), `{"id":"00","evidence":"00"}`} {
		if _, err := parseRequest(encoded); err == nil {
			t.Fatalf("accepted %q", encoded)
		}
	}
}

func TestClientRejectsChangedInitialAuthority(t *testing.T) {
	expected := testPool()
	changed := expected
	changed.MountPath = "/mnt/other"
	pools := &fakePools{pool: changed}
	fallback := &fakeProbe{}
	client := &Client{Pools: pools, Fallback: fallback}
	if _, err := client.StatFSForPool(context.Background(), expected); err == nil || pools.requests != 0 || fallback.calls != 0 {
		t.Fatalf("changed authority reached execution: requests=%d fallback=%d err=%v", pools.requests, fallback.calls, err)
	}
}

func TestClientWithoutFallbackReturnsRetryable(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "unsupported"
		if supported {
			name = "answer timeout"
		}
		t.Run(name, func(t *testing.T) {
			pool := testPool()
			pool.Status.CapacityProbeSupported = supported
			pools := &fakePools{pool: pool}
			client := &Client{Pools: pools, Timeout: time.Millisecond}
			stats, err := client.StatFSForPool(context.Background(), pool)
			var retryable interface{ Retryable() bool }
			if !errors.As(err, &retryable) || !retryable.Retryable() || stats != (capacity.Filesystem{}) {
				t.Fatalf("missing resident probe must retry without allocation: stats=%+v err=%v", stats, err)
			}
			if supported && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("internal deadline identity lost: %v", err)
			}
			wantRequests := 0
			if supported {
				wantRequests = 1
			}
			if pools.requests != wantRequests {
				t.Fatalf("requests=%d want=%d", pools.requests, wantRequests)
			}
		})
	}
}

func TestClientNonBlockingPoolIsolation(t *testing.T) {
	pool := testPool()
	pools := &fakePools{pool: pool}
	pools.onRequest = func(r request) {
		pools.pool.Status.CapacityProbe = &volumeapi.PoolCapacityProbeResult{RequestID: r.ID, Evidence: r.Evidence, TotalBytes: 100, AvailableBytes: 50}
	}
	client := &Client{Pools: pools, NonBlocking: true}
	busyPool := pool
	busyPool.Name, busyPool.UID = "busy-pool", "busy-pool-uid"
	unlock := client.locks.Lock(busyPool.UID)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, err := client.StatFSForPool(context.Background(), busyPool)
		result <- err
	}()
	select {
	case err := <-result:
		var retryable interface{ Retryable() bool }
		if !errors.As(err, &retryable) || !retryable.Retryable() || pools.reads != 0 || pools.requests != 0 {
			t.Fatalf("busy probe touched authority or did not retry: reads=%d requests=%d err=%v", pools.reads, pools.requests, err)
		}
	case <-time.After(time.Second):
		t.Fatal("busy probe waited for the Pool lock")
	}
	if stats, err := client.StatFSForPool(context.Background(), pool); err != nil || stats.AvailableBytes != 50 {
		t.Fatalf("independent Pool failed: stats=%+v err=%v", stats, err)
	}
	unlock()
	unlock = nil
	pools.pool = busyPool
	if stats, err := client.StatFSForPool(context.Background(), busyPool); err != nil || stats.AvailableBytes != 50 || pools.requests != 2 {
		t.Fatalf("fresh retry failed after lock release: stats=%+v requests=%d err=%v", stats, pools.requests, err)
	}
}

func TestClientDefaultWaitsForPoolLock(t *testing.T) {
	pool := testPool()
	pool.Status.CapacityProbeSupported = false
	pools := &fakePools{pool: pool}
	fallback := &fakeProbe{stats: capacity.Filesystem{TotalBytes: 100}}
	client := &Client{Pools: pools, Fallback: fallback}
	unlock := client.locks.Lock(pool.UID)
	result := make(chan error, 1)
	go func() {
		_, err := client.StatFSForPool(context.Background(), pool)
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
		if err != nil || fallback.calls != 1 {
			t.Fatalf("default fallback changed: calls=%d err=%v", fallback.calls, err)
		}
	case <-time.After(time.Second):
		t.Fatal("default probe did not resume after lock release")
	}
}

func TestClientWithoutFallbackPreservesCallerCancellation(t *testing.T) {
	pool := testPool()
	pools := &fakePools{pool: pool}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pools.onRequest = func(request) { cancel() }
	client := &Client{Pools: pools, NonBlocking: true}
	stats, err := client.StatFSForPool(ctx, pool)
	var retryable interface{ Retryable() bool }
	if !errors.Is(err, context.Canceled) || errors.As(err, &retryable) || stats != (capacity.Filesystem{}) || pools.requests != 1 {
		t.Fatalf("caller cancellation became an internal retry: stats=%+v requests=%d err=%v", stats, pools.requests, err)
	}
}
