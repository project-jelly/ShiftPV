package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestPreparedCapacityProbePendingPreservesJournalAndReservation(t *testing.T) {
	f, move := preparationAtCapacity(t)
	state := f.state(t, 0)
	pending := true
	usageCalls, probeCalls := 0, 0
	f.reconciler.CapacityProbe = measuredCapacityProbe{
		usage: func(context.Context) (int64, error) { usageCalls++; return 2 << 20, nil },
		stat: func() (poolcapacity.Filesystem, error) {
			probeCalls++
			if pending {
				return poolcapacity.Filesystem{}, errors.Join(ErrCapacityProbePending, context.DeadlineExceeded)
			}
			return poolcapacity.Filesystem{AvailableBytes: 10 << 30}, nil
		},
	}
	f.reconciler.SourceUsage = f.reconciler.CapacityProbe.(SourceUsage)
	work := volumeWork{volumeID: move.Spec.VolumeID, moves: []moveReference{{move.Name, move.UID}}}
	_, err := f.reconciler.advanceVolume(f.ctx, work)
	if !errors.Is(err, ErrCapacityProbePending) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatcher lost pending probe identity: %v", err)
	}
	stored := f.move(t, 0)
	if stored.Status.Phase != move.Status.Phase || stored.Status.Reason != move.Status.Reason ||
		stored.Status.CapacityReason != move.Status.CapacityReason || stored.Status.CapacityApproved ||
		stored.Status.IncomingCopy != nil || !reflect.DeepEqual(stored.Status.Preparation, move.Status.Preparation) ||
		!reflect.DeepEqual(f.state(t, 0), state) || usageCalls != 1 || probeCalls != 1 {
		t.Fatalf("pending probe changed authority or replayed diagnostics: move=%+v usage=%d probes=%d", stored.Status, usageCalls, probeCalls)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	pending = false
	if _, err := f.reconciler.advanceVolume(f.ctx, work); err != nil {
		t.Fatal(err)
	}
	if !f.move(t, 0).Status.CapacityApproved || usageCalls != 1 || probeCalls != 2 {
		t.Fatalf("fresh probe did not resume the same measured transaction: %+v", f.move(t, 0).Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedCapacityProbePendingDoesNotRecordDenial(t *testing.T) {
	r, repo, move := capacityCostFixture()
	move.Status.Phase = "WaitingForCapacity"
	repo.moves[0] = move
	r.CapacityProbe = measuredCapacityProbe{
		usage: func(context.Context) (int64, error) { return 32 << 20, nil },
		stat: func() (poolcapacity.Filesystem, error) {
			return poolcapacity.Filesystem{}, ErrCapacityProbePending
		},
	}
	r.SourceUsage = r.CapacityProbe.(SourceUsage)
	previous := move.Status
	err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"})
	err = r.recordMoveError(context.Background(), &move, previous, "ActionFailed", "capacity admission", err)
	stored := repo.moves[0]
	if !errors.Is(err, ErrCapacityProbePending) || stored.Status.SourceBytes != 32<<20 ||
		stored.Status.Phase != "WaitingForCapacity" || stored.Status.Reason != "" ||
		stored.Status.CapacityReason != "" || stored.Status.CapacityApproved {
		t.Fatalf("pending prepared probe changed phase, denial or approval: %+v err=%v", stored.Status, err)
	}
}
