package controller

import (
	"context"
	"math"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type fakeMoveCapacityProbe struct {
	usage int64
	stats poolcapacity.Filesystem
}

func (f fakeMoveCapacityProbe) StatFS(context.Context, string) (poolcapacity.Filesystem, error) {
	return f.stats, nil
}

func (f fakeMoveCapacityProbe) VolumeUsage(context.Context, string, string) (int64, error) {
	return f.usage, nil
}

func (f fakeMoveCapacityProbe) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	return f.StatFS(ctx, pool.NodeName)
}

func (f fakeMoveCapacityProbe) VolumeUsageForCopy(ctx context.Context, copy volume.CopyIdentity) (int64, error) {
	return f.VolumeUsage(ctx, copy.NodeName, copy.VolumeID)
}

func TestEnsureCapacityApprovesOrBlocksBeforeCopy(t *testing.T) {
	for name, test := range map[string]struct {
		limit     string
		usage     int64
		available int64
		want      string
	}{
		"approved":            {limit: "128Mi", usage: 32 << 20, available: 64 << 20},
		"logical reservation": {limit: "16Mi", usage: 8 << 20, available: 64 << 20, want: "DestinationReservationLimit"},
		"physical space":      {limit: "128Mi", usage: 80 << 20, available: 64 << 20, want: "DestinationFilesystemSpace"},
	} {
		t.Run(name, func(t *testing.T) {
			volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
			move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}}
			repository := &memoryRepository{
				volumes: map[string]volumeapi.State{volumeID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CapacityBytes: 32 << 20}},
				pools: []volumeapi.Pool{
					{Name: "source", UID: "source-pool-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "128Mi"},
					{Name: "destination", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: test.limit},
				},
				moves: []volumeapi.Move{move},
			}
			client := fake.NewSimpleClientset()
			reconciler := newTestReconciler(client, repository, func(r *Reconciler) {
				r.CapacityProbe = fakeMoveCapacityProbe{usage: test.usage, stats: poolcapacity.Filesystem{AvailableBytes: test.available}}
				r.PoolLocks = &poolcapacity.Locker{}
			})
			if err := reconciler.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
				t.Fatal(err)
			}
			if move.Status.CapacityApproved != (test.want == "") || move.Status.CapacityReason != test.want || move.Status.SourceBytes != test.usage {
				t.Fatalf("capacity status = %+v", move.Status)
			}
			wantPoolUID := ""
			if test.want == "" {
				wantPoolUID = "destination-pool-uid"
			}
			if move.Status.DestinationPoolUID != wantPoolUID {
				t.Fatalf("destination Pool UID = %q, want %q", move.Status.DestinationPoolUID, wantPoolUID)
			}
			if _, err := client.BatchV1().Jobs("system").Get(context.Background(), namesFor(move.Name).CopyJob, metav1.GetOptions{}); err == nil {
				t.Fatal("capacity admission created a copy Job")
			}
		})
	}
}

func TestEnsureCapacityRejectsNonPositiveSourceUsage(t *testing.T) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{volumeID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CapacityBytes: 32 << 20}},
		pools: []volumeapi.Pool{
			{Name: "source", UID: "source-pool-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "128Mi"},
			{Name: "destination", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: "128Mi"},
		},
		moves: []volumeapi.Move{move},
	}
	reconciler := newTestReconciler(fake.NewSimpleClientset(), repository, func(r *Reconciler) {
		r.CapacityProbe = fakeMoveCapacityProbe{usage: 0, stats: poolcapacity.Filesystem{AvailableBytes: 64 << 20}}
		r.PoolLocks = &poolcapacity.Locker{}
	})
	err := reconciler.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"})
	if err == nil {
		t.Fatal("unmeasured source usage was approved")
	}
	if !strings.Contains(err.Error(), "source volume usage must be positive") {
		t.Fatalf("error = %v", err)
	}
	if move.Status.CapacityApproved || move.Status.CapacityReason != "" || move.Status.SourceBytes != 0 {
		t.Fatalf("capacity status = %+v", move.Status)
	}
}

func TestDestinationCapacityRetainsRecoveredMoveUntilSettled(t *testing.T) {
	currentID := "shiftpv-0123456789abcdef0123456789abcdef"
	recoveredID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	current := volumeapi.Move{Name: "move-current", Spec: volumeapi.MoveSpec{VolumeID: currentID, SourceNode: "source"}}
	recovered := volumeapi.Move{
		Name: "move-recovered", Spec: volumeapi.MoveSpec{VolumeID: recoveredID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{
			Phase: "Blocked", DestinationNode: "destination", DestinationPoolUID: "destination-uid", CapacityApproved: true,
			SourceBytes: 32 << 20, RecoveryPhase: "Recovered",
		},
	}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{
			currentID:   {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: current.Name, CapacityBytes: 32 << 20},
			recoveredID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: recovered.Name, CapacityBytes: 32 << 20},
		},
		pools: []volumeapi.Pool{
			{Name: "source", UID: "source-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "64Mi"},
			{Name: "destination", UID: "destination-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: "64Mi"},
		},
		moves: []volumeapi.Move{current, recovered},
	}
	sourceState, _ := repository.Get(context.Background(), recoveredID)
	repository.moves[1].Status.SourceCopy = sourceState.CurrentCopy
	client := fake.NewSimpleClientset()
	reconciler := &Reconciler{Client: client, Repository: repository, Namespace: "system"}
	requested, logicalReserved, physicalPending, limit, err := reconciler.destinationCapacityForPool(context.Background(), current, repository.pools[1])
	if err != nil {
		t.Fatal(err)
	}
	if requested != 32<<20 || logicalReserved != 32<<20 || physicalPending != 32<<20 || limit != 64<<20 {
		t.Fatalf("capacity = requested=%d logical=%d physical=%d limit=%d", requested, logicalReserved, physicalPending, limit)
	}
}

func TestDestinationCapacityIgnoresMoveAfterVolumeAndReservationDeletion(t *testing.T) {
	currentID := "shiftpv-0123456789abcdef0123456789abcdef"
	current := volumeapi.Move{Name: "move-current", Spec: volumeapi.MoveSpec{VolumeID: currentID, SourceNode: "source"}}
	deleted := volumeapi.Move{
		Name: "move-deleted", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-deleted", SourceNode: "source"},
		Status: volumeapi.MoveStatus{
			Phase: "Succeeded", CleanupPhase: "Completed", DestinationNode: "destination", CapacityApproved: true, SourceBytes: 32 << 20,
		},
	}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{
			currentID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: current.Name, CapacityBytes: 32 << 20},
		},
		pools: []volumeapi.Pool{
			{Name: "source", UID: "source-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "64Mi"},
			{Name: "destination", UID: "destination-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: "64Mi"},
		},
		moves: []volumeapi.Move{current, deleted},
	}
	reconciler := &Reconciler{
		Client:     fake.NewSimpleClientset(),
		Repository: repository, Namespace: "system",
	}
	requested, logicalReserved, physicalPending, _, err := reconciler.destinationCapacityForPool(context.Background(), current, repository.pools[1])
	if err != nil {
		t.Fatalf("deleted volume's Move blocked destination admission: %v", err)
	}
	if requested != 32<<20 || logicalReserved != 0 || physicalPending != 0 {
		t.Fatalf("capacity = requested=%d logical=%d physical=%d", requested, logicalReserved, physicalPending)
	}
}

func TestDestinationCapacityRejectsMoveWithReservationButNoVolume(t *testing.T) {
	currentID := "shiftpv-0123456789abcdef0123456789abcdef"
	orphanID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	current := volumeapi.Move{Name: "move-current", Spec: volumeapi.MoveSpec{VolumeID: currentID, SourceNode: "source"}}
	incomplete := volumeapi.Move{
		Name: "move-incomplete", Spec: volumeapi.MoveSpec{VolumeID: orphanID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{DestinationNode: "destination", CapacityApproved: true, SourceBytes: 1},
	}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{
			currentID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: current.Name, CapacityBytes: 8 << 20},
		},
		pools: []volumeapi.Pool{
			{Name: "source", UID: "source-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "64Mi"},
			{Name: "destination", UID: "destination-uid", NodeName: "destination", MountPath: "/destination", CapacityLimit: "64Mi"},
		},
		moves: []volumeapi.Move{current, incomplete},
	}
	reconciler := &Reconciler{
		Client:     fake.NewSimpleClientset(),
		Repository: repository, Namespace: "system",
	}
	_, _, _, _, err := reconciler.destinationCapacityForPool(context.Background(), current, repository.pools[1])
	if err == nil || !strings.Contains(err.Error(), "has no volume state") {
		t.Fatalf("incomplete move was not rejected: %v", err)
	}
}

func TestDestinationCapacityRejectsPhysicalPendingOverflow(t *testing.T) {
	currentID := "shiftpv-0123456789abcdef0123456789abcdef"
	firstID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secondID := "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	current := volumeapi.Move{Name: "move-current", Spec: volumeapi.MoveSpec{VolumeID: currentID, SourceNode: "source"}}
	first := volumeapi.Move{
		Name: "move-first", Spec: volumeapi.MoveSpec{VolumeID: firstID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{DestinationNode: "destination", CapacityApproved: true, SourceBytes: math.MaxInt64},
	}
	second := volumeapi.Move{
		Name: "move-second", Spec: volumeapi.MoveSpec{VolumeID: secondID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{DestinationNode: "destination", CapacityApproved: true, SourceBytes: 1},
	}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{
			currentID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: current.Name, CapacityBytes: 1},
			firstID:   {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: first.Name, CapacityBytes: 1},
			secondID:  {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: second.Name, CapacityBytes: 1},
		},
		pools: []volumeapi.Pool{{Name: "destination", UID: "destination-uid", NodeName: "destination", CapacityLimit: "1Ti"}, {Name: "source", UID: "source-uid", NodeName: "source"}},
		moves: []volumeapi.Move{current, first, second},
	}
	for i := 1; i < len(repository.moves); i++ {
		state, _ := repository.Get(context.Background(), repository.moves[i].Spec.VolumeID)
		repository.moves[i].Status.SourceCopy = state.CurrentCopy
		repository.moves[i].Status.DestinationPoolUID = "destination-uid"
	}
	reconciler := &Reconciler{Repository: repository}
	_, _, _, _, err := reconciler.destinationCapacityForPool(context.Background(), current, repository.pools[0])
	if err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("physical pending overflow was not rejected: %v", err)
	}
}

func TestEnsureCapacityFailsClosedWhenDestinationPoolLimitIsMissing(t *testing.T) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	move := volumeapi.Move{Name: "move-test", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{
			volumeID: {Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CapacityBytes: 32 << 20},
		},
		pools: []volumeapi.Pool{
			{Name: "source", UID: "source-uid", NodeName: "source", MountPath: "/source", CapacityLimit: "64Mi"},
			{Name: "destination", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination"},
		},
		moves: []volumeapi.Move{move},
	}
	reconciler := &Reconciler{
		Client:     fake.NewSimpleClientset(),
		Repository: repository, Namespace: "system",
		CapacityProbe: fakeMoveCapacityProbe{usage: 1, stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}},
		PoolLocks:     &poolcapacity.Locker{},
	}
	err := reconciler.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"})
	if err == nil || !strings.Contains(err.Error(), "capacity limit") {
		t.Fatalf("missing Pool limit did not fail closed: %v", err)
	}
	if move.Status.CapacityApproved {
		t.Fatal("missing Pool limit approved destination capacity")
	}
}
