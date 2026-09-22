package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type measuredCapacityProbe struct {
	usage func(context.Context) (int64, error)
	stat  func() (poolcapacity.Filesystem, error)
}

func (p measuredCapacityProbe) VolumeUsage(ctx context.Context, _, _ string) (int64, error) {
	return p.usage(ctx)
}
func (p measuredCapacityProbe) StatFS(context.Context, string) (poolcapacity.Filesystem, error) {
	return p.stat()
}

func TestCapacityMeasurementDoesNotHoldDestinationAndRechecksReservations(t *testing.T) {
	r, repo, move := capacityCostFixture()
	r.CapacityProbe = measuredCapacityProbe{
		usage: func(ctx context.Context) (int64, error) {
			acquired := make(chan func())
			go func() {
				unlock := r.PoolLocks.Lock("destination")
				select {
				case acquired <- unlock:
				case <-ctx.Done():
					unlock()
				}
			}()
			select {
			case unlock := <-acquired:
				// A competing create can spend capacity while the source is measured.
				repo.volumes["another-volume"] = volumeapi.State{OwnerNode: "destination", CapacityBytes: 120 << 20}
				unlock()
			case <-ctx.Done():
				return 0, errors.New("source measurement held destination lock")
			}
			return 32 << 20, nil
		},
		stat: func() (poolcapacity.Filesystem, error) {
			return poolcapacity.Filesystem{AvailableBytes: 128 << 20}, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.ensureCapacity(ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if move.Status.CapacityApproved || move.Status.CapacityReason != "DestinationReservationLimit" {
		t.Fatalf("stale reservations accepted: %+v", move.Status)
	}
}

func TestCapacityRetryReusesPersistedSourceMeasurement(t *testing.T) {
	r, repo, move := capacityCostFixture()
	calls, statCalls := 0, 0
	r.CapacityProbe = measuredCapacityProbe{
		usage: func(context.Context) (int64, error) { calls++; return 32 << 20, nil },
		stat: func() (poolcapacity.Filesystem, error) {
			statCalls++
			if statCalls == 1 {
				return poolcapacity.Filesystem{}, errors.New("temporary probe failure")
			}
			return poolcapacity.Filesystem{AvailableBytes: 128 << 20}, nil
		},
	}
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err == nil {
		t.Fatal("missing probe failure")
	}
	// Reconstruct the Move as on controller restart; do not rely on the mutated argument.
	move = repo.moves[0]
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !move.Status.CapacityApproved {
		t.Fatalf("usage calls=%d approved=%v", calls, move.Status.CapacityApproved)
	}
}

func capacityCostFixture() (*Reconciler, *memoryRepository, volumeapi.Move) {
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: "source"}}
	repo := &memoryRepository{
		volumes: map[string]volumeapi.State{id: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CapacityBytes: 32 << 20}},
		pools:   []volumeapi.Pool{{Name: "source", UID: "source-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "128Mi"}, {Name: "destination", UID: "destination-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: "128Mi"}},
		moves:   []volumeapi.Move{move},
	}
	r := newTestReconciler(fake.NewSimpleClientset(), repo)
	r.PoolLocks = &poolcapacity.Locker{}
	return r, repo, move
}
