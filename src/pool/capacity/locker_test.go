package capacity

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockerSerializesSamePoolAndReleasesEntries(t *testing.T) {
	locker := &Locker{}
	unlock := locker.Lock("pool-a")
	acquired := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		release := locker.Lock("pool-a")
		close(acquired)
		release()
	}()
	select {
	case <-acquired:
		t.Fatal("same Pool lock was acquired concurrently")
	default:
	}
	unlock()
	wait.Wait()
	if len(locker.entries) != 0 {
		t.Fatalf("lock entries remain: %d", len(locker.entries))
	}
}

func TestLockerAllowsIndependentPools(t *testing.T) {
	locker := &Locker{}
	unlockA := locker.Lock("pool-a")
	unlockB := locker.Lock("pool-b")
	unlockB()
	unlockA()
}

func TestTryLockSharesEntryWithBlockingWaiter(t *testing.T) {
	locker := &Locker{}
	unlock, acquired := locker.TryLock("pool-a")
	if !acquired || unlock == nil {
		t.Fatal("uncontended TryLock failed")
	}
	unlock = sync.OnceFunc(unlock)
	defer unlock()
	acquiredByWaiter, releaseWaiter, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseBlockedWaiter := sync.OnceFunc(func() { close(releaseWaiter) })
	defer releaseBlockedWaiter()
	go func() {
		release := locker.Lock("pool-a")
		close(acquiredByWaiter)
		<-releaseWaiter
		release()
		close(finished)
	}()
	// Observe waiter registration, rather than assuming its goroutine has run.
	deadline := time.Now().Add(time.Second)
	for {
		locker.mu.Lock()
		refs := locker.entries["pool-a"].refs
		locker.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			unlock()
			releaseBlockedWaiter()
			<-finished
			t.Fatal("blocking waiter did not register")
		}
		runtime.Gosched()
	}
	for range 100 {
		if release, ok := locker.TryLock("pool-a"); ok || release != nil {
			t.Fatal("TryLock entered a held Pool")
		}
	}
	independent, ok := locker.TryLock("pool-b")
	if !ok {
		t.Fatal("a busy Pool blocked another Pool")
	}
	independent()
	unlock()
	<-acquiredByWaiter
	if release, ok := locker.TryLock("pool-a"); ok || release != nil {
		t.Fatal("TryLock entered the blocking waiter's Pool")
	}
	releaseBlockedWaiter()
	<-finished
	if len(locker.entries) != 0 {
		t.Fatalf("failed TryLock leaked references: %+v", locker.entries)
	}
}

func TestMixedLockAndTryLockPreserveExclusionAcrossEntryReuse(t *testing.T) {
	locker := &Locker{}
	var active atomic.Int32
	var workers sync.WaitGroup
	for worker := range 12 {
		workers.Go(func() {
			for range 500 {
				var unlock func()
				if worker%2 == 0 {
					unlock = locker.Lock("pool")
				} else {
					for unlock == nil {
						unlock, _ = locker.TryLock("pool")
						if unlock == nil {
							runtime.Gosched()
						}
					}
				}
				if active.Add(1) != 1 {
					t.Error("Lock and TryLock granted the same Pool concurrently")
				}
				runtime.Gosched()
				active.Add(-1)
				unlock()
			}
		})
	}
	workers.Wait()
	if len(locker.entries) != 0 {
		t.Fatalf("lock entries remain after mixed users: %+v", locker.entries)
	}
}
