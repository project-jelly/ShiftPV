package capacity

import (
	"math"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestReservedBytesForPoolCountsVolumeOwnerCapacity(t *testing.T) {
	volumes := map[string]volumeapi.State{
		"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: 64},
	}
	got, err := ReservedBytesForPool(volumes, nil, "pool-a")
	if err != nil || got != 64 {
		t.Fatalf("reserved=%d err=%v", got, err)
	}
}

func TestReservedBytesForPoolCountsActivePrecommitDestinationHold(t *testing.T) {
	volumes := map[string]volumeapi.State{
		"v": {OwnerNode: "a", ActiveMove: "m", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: 64},
	}
	moves := []volumeapi.Move{{
		Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v", SourceNode: "a"},
		Status: volumeapi.MoveStatus{DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
	}}
	got, err := ReservedBytesForPool(volumes, moves, "pool-b")
	if err != nil || got != 64 {
		t.Fatalf("reserved=%d err=%v", got, err)
	}
}

func TestReservedBytesForPoolCountsPostcommitRetainedSourceHold(t *testing.T) {
	volumes := map[string]volumeapi.State{
		"v": {OwnerNode: "b", ActiveMove: "m", CurrentCopy: &volume.CopyIdentity{NodeName: "b", PoolUID: "pool-b"}, CapacityBytes: 64},
	}
	moves := []volumeapi.Move{{
		Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v", SourceNode: "a"},
		Status: volumeapi.MoveStatus{DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
	}}
	got, err := ReservedBytesForPool(volumes, moves, "pool-a")
	if err != nil || got != 64 {
		t.Fatalf("reserved=%d err=%v", got, err)
	}
}

func TestReservedBytesForPoolSkipsCompletedMoves(t *testing.T) {
	volumes := map[string]volumeapi.State{
		"v": {OwnerNode: "b", CurrentCopy: &volume.CopyIdentity{NodeName: "b", PoolUID: "pool-b"}, CapacityBytes: 64},
	}
	moves := []volumeapi.Move{{
		Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v", SourceNode: "a"},
		Status: volumeapi.MoveStatus{Phase: "Succeeded", CleanupPhase: "Completed", DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
	}}
	got, err := ReservedBytesForPool(volumes, moves, "pool-a")
	if err != nil || got != 0 {
		t.Fatalf("reserved=%d err=%v", got, err)
	}
}

func TestReservedBytesForPoolRetainsSucceededMoveWithoutCleanupProof(t *testing.T) {
	volumes := map[string]volumeapi.State{
		"v": {OwnerNode: "b", ActiveMove: "m", CurrentCopy: &volume.CopyIdentity{NodeName: "b", PoolUID: "pool-b"}, CapacityBytes: 64},
	}
	moves := []volumeapi.Move{{
		Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v", SourceNode: "a"},
		Status: volumeapi.MoveStatus{Phase: "Succeeded", DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
	}}
	got, err := ReservedBytesForPool(volumes, moves, "pool-a")
	if err != nil || got != 64 {
		t.Fatalf("reserved=%d err=%v", got, err)
	}
}

func TestReservedBytesForPoolRejectsContradictoryCapacityState(t *testing.T) {
	for name, test := range map[string]struct {
		volumes map[string]volumeapi.State
		moves   []volumeapi.Move
		want    string
	}{
		"missing owner": {
			volumes: map[string]volumeapi.State{"v": {CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: 64}},
			want:    "incomplete capacity owner identity",
		},
		"missing capacity": {
			volumes: map[string]volumeapi.State{"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}}},
			want:    "incomplete capacity owner identity",
		},
		"missing current copy": {
			volumes: map[string]volumeapi.State{"v": {OwnerNode: "a", CapacityBytes: 64}},
			want:    "incomplete capacity owner identity",
		},
		"missing Pool UID": {
			volumes: map[string]volumeapi.State{"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a"}, CapacityBytes: 64}},
			want:    "incomplete capacity owner identity",
		},
		"copy owner mismatch": {
			volumes: map[string]volumeapi.State{"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "b", PoolUID: "pool-a"}, CapacityBytes: 64}},
			want:    "incomplete capacity owner identity",
		},
		"approved move without volume": {
			moves: []volumeapi.Move{{
				Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v"},
				Status: volumeapi.MoveStatus{DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
			}},
			want: "no volume state",
		},
		"approved inactive move": {
			volumes: map[string]volumeapi.State{"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: 64}},
			moves: []volumeapi.Move{{
				Name: "m", Spec: volumeapi.MoveSpec{VolumeID: "v"},
				Status: volumeapi.MoveStatus{DestinationNode: "b", DestinationPoolUID: "pool-b", SourceCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityApproved: true},
			}},
			want: "incomplete capacity hold identity",
		},
		"overflow": {
			volumes: map[string]volumeapi.State{
				"v": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: math.MaxInt64},
				"w": {OwnerNode: "a", CurrentCopy: &volume.CopyIdentity{NodeName: "a", PoolUID: "pool-a"}, CapacityBytes: 1},
			},
			want: "overflows",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReservedBytesForPool(test.volumes, test.moves, "pool-a")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err=%v, want %q", err, test.want)
			}
		})
	}
}
