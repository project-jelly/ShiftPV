package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestPreparationPoolBusyPreservesPlacementAndServingSource(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	for range 3 {
		if err := f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
		f.schedulePlacements(t)
	}
	move, state := f.move(t, 0), f.state(t, 0)
	placement, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil || placement.Spec.NodeName != "destination" || move.Status.Preparation != nil {
		t.Fatalf("fixture did not reach scheduled pre-grant placement: pod=%+v move=%+v err=%v", placement, move, err)
	}
	assertPoolBusy(t, f.reconciler.PoolLocks, "destination-pool-uid", func() error {
		return f.reconciler.reconcilePreparation(f.ctx, move)
	})
	if got := f.move(t, 0); !reflect.DeepEqual(got, move) {
		t.Fatalf("busy admission changed Move: before=%+v after=%+v", move, got)
	}
	if got := f.state(t, 0); !reflect.DeepEqual(got, state) {
		t.Fatalf("busy admission changed serving source: before=%+v after=%+v", state, got)
	}
	current, err := f.client.CoreV1().Pods("system").Get(f.ctx, placement.Name, metav1.GetOptions{})
	if err != nil || current.UID != placement.UID || current.DeletionTimestamp != nil {
		t.Fatalf("busy admission discarded placement: pod=%+v err=%v", current, err)
	}
	assertPreparationOriginals(t, f)
	assertPreparationReserved(t, f, 0)
	if err := f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	if f.move(t, 0).Status.Preparation == nil {
		t.Fatal("fresh retry did not grant preparation after lock release")
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedConversionPoolBusyRetainsHoldWithoutDiagnosticReplay(t *testing.T) {
	f, move := preparationAtCapacity(t)
	state := f.state(t, 0)
	usageCalls, statCalls := 0, 0
	f.reconciler.CapacityProbe = measuredCapacityProbe{
		usage: func(context.Context) (int64, error) { usageCalls++; return 2 << 20, nil },
		stat: func() (poolcapacity.Filesystem, error) {
			statCalls++
			return poolcapacity.Filesystem{AvailableBytes: 10 << 30}, nil
		},
	}
	assertPoolBusy(t, f.reconciler.PoolLocks, move.Status.Preparation.Pool.UID, func() error {
		return f.reconciler.reconcileMove(f.ctx, move)
	})
	stored := f.move(t, 0)
	if usageCalls != 1 || statCalls != 0 || stored.Status.SourceBytes != 2<<20 {
		t.Fatalf("measurement boundary changed: usage=%d statfs=%d status=%+v", usageCalls, statCalls, stored.Status)
	}
	if stored.Status.Phase != move.Status.Phase || stored.Status.Reason != move.Status.Reason ||
		stored.Status.CapacityReason != move.Status.CapacityReason || stored.Status.CapacityApproved ||
		stored.Status.IncomingCopy != nil || stored.Status.DestinationCopy != nil ||
		!reflect.DeepEqual(stored.Status.Preparation, move.Status.Preparation) {
		t.Fatalf("busy conversion changed hold/phase or recorded an error: %+v", stored.Status)
	}
	if got := f.state(t, 0); !reflect.DeepEqual(got, state) {
		t.Fatalf("busy conversion changed source: %+v", got)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	// A fresh attempt reuses the successfully persisted source measurement.
	if err := f.reconciler.reconcileMove(f.ctx, stored); err != nil {
		t.Fatal(err)
	}
	if usageCalls != 1 || statCalls != 1 || !f.move(t, 0).Status.CapacityApproved {
		t.Fatalf("retry did not resume conversion: usage=%d statfs=%d move=%+v", usageCalls, statCalls, f.move(t, 0))
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestLegacyCapacityPoolBusyDoesNotBecomeCapacityDenial(t *testing.T) {
	r, repo, move := capacityCostFixture()
	move.Status.Phase = "WaitingForCapacity"
	repo.moves[0] = move
	r.CapacityProbe = fakeMoveCapacityProbe{usage: 32 << 20, stats: poolcapacity.Filesystem{AvailableBytes: 128 << 20}}
	assertPoolBusy(t, r.PoolLocks, "destination-uid", func() error {
		previous := move.Status
		err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"})
		if err != nil {
			return r.recordMoveError(context.Background(), &move, previous, "ActionFailed", "capacity admission", err)
		}
		return nil
	})
	stored := repo.moves[0]
	if stored.Status.SourceBytes != 32<<20 || stored.Status.Phase != "WaitingForCapacity" ||
		stored.Status.Reason != "" || stored.Status.CapacityReason != "" || stored.Status.CapacityApproved {
		t.Fatalf("busy legacy admission changed its phase or denial: %+v", stored.Status)
	}
	if err := r.ensureCapacity(context.Background(), &stored, observation{DestinationNode: "destination"}); err != nil || !stored.Status.CapacityApproved {
		t.Fatalf("fresh legacy retry did not approve: status=%+v err=%v", stored.Status, err)
	}
}

// The deadline only detects a forbidden blocking wait; successful cases return
// while this goroutine still owns the Pool lock, then explicitly retry later.
func assertPoolBusy(t *testing.T, locks *poolcapacity.Locker, pool string, run func() error) {
	t.Helper()
	unlock := locks.Lock(pool)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	result := make(chan error, 1)
	go func() { result <- run() }()
	select {
	case err := <-result:
		if !errors.Is(err, errPoolBusy) {
			t.Fatalf("held Pool must return errPoolBusy without waiting: %v", err)
		}
	case <-time.After(time.Second):
		unlock()
		locked = false
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("Pool operation did not stop after releasing a blocked test")
		}
		t.Fatal("Pool operation blocked on a busy lock")
	}
}
