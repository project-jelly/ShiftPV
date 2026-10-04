package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type poolAwareCapacityProbe struct {
	measured *volume.CopyIdentity
	probed   []string
}

func (p *poolAwareCapacityProbe) VolumeUsageForCopy(_ context.Context, copy volume.CopyIdentity) (int64, error) {
	p.measured = &copy
	return 32 << 20, nil
}
func (p *poolAwareCapacityProbe) StatFSForPool(_ context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	p.probed = append(p.probed, pool.UID)
	return poolcapacity.Filesystem{AvailableBytes: 128 << 20}, nil
}

func TestMoveCapacitySelectsIndependentPoolOnDestinationNode(t *testing.T) {
	r, repo, move := capacityCostFixture()
	repo.pools[1].Name = "a-full"
	repo.pools = append(repo.pools, volumeapi.Pool{Name: "b-free", UID: "free-uid", NodeName: "destination", MountPath: "/free", CapacityLimit: "128Mi"},
		volumeapi.Pool{Name: "c-other", UID: "other-uid", NodeName: "destination", MountPath: "/other", CapacityLimit: "1Ti", PoolGroup: "other"})
	repo.volumes["existing"] = volumeapi.State{OwnerNode: "destination", CapacityBytes: 128 << 20}
	probe := &poolAwareCapacityProbe{}
	r.CapacityProbe = probe
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if !move.Status.CapacityApproved || move.Status.DestinationPoolUID != "free-uid" {
		t.Fatalf("admission selected wrong Pool: %+v", move.Status)
	}
	if probe.measured == nil || probe.measured.PoolUID != "source-uid" {
		t.Fatalf("source usage identity = %+v", probe.measured)
	}
	// Replaying approval must neither double-count its hold nor select another Pool.
	probe.probed = nil
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if len(probe.probed) != 0 || move.Status.DestinationPoolUID != "free-uid" {
		t.Fatal("approved hold was reallocated")
	}
	// A recreated Pool with spare space cannot inherit the approved UID.
	repo.pools[2].UID = "replacement-uid"
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err == nil {
		t.Fatal("recreated Pool inherited approval")
	}
	if !move.Status.CapacityApproved || move.Status.DestinationPoolUID != "free-uid" {
		t.Fatal("failed retry released durable hold")
	}
}

func TestMoveCapacityHoldsRemainScopedToExactPool(t *testing.T) {
	r, repo, current := capacityCostFixture()
	other := volumeapi.Pool{Name: "another", UID: "another-uid", NodeName: "destination", MountPath: "/another", CapacityLimit: "128Mi"}
	repo.pools = append(repo.pools, other)
	heldID := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	held := volumeapi.Move{Name: "held", Spec: volumeapi.MoveSpec{VolumeID: heldID, SourceNode: "source"}, Status: volumeapi.MoveStatus{
		DestinationNode: "destination", DestinationPoolUID: other.UID, CapacityApproved: true, SourceBytes: 100 << 20,
	}}
	repo.volumes[heldID] = volumeapi.State{OwnerNode: "source", ActiveMove: held.Name, CapacityBytes: 100 << 20}
	state, _ := repo.Get(context.Background(), heldID)
	held.Status.SourceCopy = state.CurrentCopy
	repo.moves = append(repo.moves, held)
	_, logical, physical, _, err := r.destinationCapacityForPool(context.Background(), current, repo.pools[1])
	if err != nil || logical != 0 || physical != 0 {
		t.Fatalf("other Pool's hold leaked: logical=%d physical=%d err=%v", logical, physical, err)
	}
	_, logical, physical, _, err = r.destinationCapacityForPool(context.Background(), current, other)
	if err != nil || logical != 100<<20 || physical != 100<<20 {
		t.Fatalf("selected Pool's hold missing: logical=%d physical=%d err=%v", logical, physical, err)
	}
}

func TestMoveResourcesUsePinnedPoolsAmongSameNodeRegistrations(t *testing.T) {
	w := newMobilityWalk()
	state, _ := w.repository.Get(w.ctx, w.volumeID)
	w.move.Status.SourceCopy = state.CurrentCopy
	w.repository.volumes[w.volumeID] = state
	w.move.Status.DestinationNode = "destination"
	w.move.Status.DestinationPoolUID = "destination-pool-uid"
	// Put unrelated roots first: the selected Pool must never depend on order.
	w.repository.pools = append([]volumeapi.Pool{
		{Name: "source-extra", UID: "source-extra-uid", NodeName: "source", MountPath: "/wrong-source"},
		{Name: "destination-extra", UID: "destination-extra-uid", NodeName: "destination", MountPath: "/wrong-destination"},
	}, w.repository.pools...)
	if err := w.reconciler.prepareMoveCopyIdentities(w.ctx, &w.move); err != nil {
		t.Fatal(err)
	}
	names := namesFor(w.move.Name)
	if err := w.reconciler.ensureSourcePod(w.ctx, w.move, names); err != nil {
		t.Fatal(err)
	}
	pod, err := w.client.CoreV1().Pods("system").Get(w.ctx, names.SourcePod, metav1.GetOptions{})
	if err != nil || pod.Spec.Volumes[0].HostPath.Path != "/source-pool" {
		t.Fatalf("wrong source mount: pod=%+v err=%v", pod, err)
	}
	if err := w.reconciler.ensureCopyJob(w.ctx, w.move, names); err != nil {
		t.Fatal(err)
	}
	job, err := w.client.BatchV1().Jobs("system").Get(w.ctx, names.CopyJob, metav1.GetOptions{})
	if err != nil || job.Spec.Template.Spec.Volumes[0].HostPath.Path != "/destination-pool" {
		t.Fatalf("wrong destination mount: job=%+v err=%v", job, err)
	}
	recovery, err := w.reconciler.verifyOwnerJob(w.ctx, w.move, names, "verify", "source")
	if err != nil || recovery.Spec.Template.Spec.Volumes[0].HostPath.Path != "/source-pool" {
		t.Fatalf("wrong recovery mount: job=%+v err=%v", recovery, err)
	}
	pool, err := rollbackDestinationPool(w.move, identifiedTestPools(w.repository.pools))
	if err != nil || pool.UID != "destination-pool-uid" {
		t.Fatalf("wrong rollback Pool: pool=%+v err=%v", pool, err)
	}
}

func TestMoveObservationFollowsPinnedPoolReadiness(t *testing.T) {
	w := newMobilityWalk()
	state, _ := w.repository.Get(w.ctx, w.volumeID)
	w.move.Status.SourceCopy = state.CurrentCopy
	w.repository.volumes[w.volumeID] = state
	w.move.Status.DestinationNode = "destination"
	w.move.Status.DestinationPoolUID = "destination-pool-uid"
	w.move.Status.CapacityApproved = true
	w.repository.pools = append(w.repository.pools, volumeapi.Pool{Name: "extra", UID: "extra-uid", NodeName: "destination", MountPath: "/extra"})
	w.repository.readyPoolsConfigured = true
	// Another Ready Pool on the same node cannot authorize this transaction.
	w.repository.readyPools = []volumeapi.Pool{w.repository.pools[0], w.repository.pools[2]}
	observed, err := w.reconciler.observe(w.ctx, w.move)
	if err != nil || !observed.FSM.DestinationUnavailable {
		t.Fatalf("unrelated Ready Pool accepted: observed=%+v err=%v", observed.FSM, err)
	}
	w.repository.readyPools = append(w.repository.readyPools, w.repository.pools[1])
	observed, err = w.reconciler.observe(w.ctx, w.move)
	if err != nil || observed.FSM.DestinationUnavailable {
		t.Fatalf("pinned Ready Pool rejected: observed=%+v err=%v", observed.FSM, err)
	}
	if len(observed.CandidateNodes) != 1 || observed.CandidateNodes[0] != "destination" {
		t.Fatalf("duplicate node candidates: %v", observed.CandidateNodes)
	}
}

type approvalFailureRepository struct {
	*memoryRepository
	accepted bool
	writes   int
}

func (r *approvalFailureRepository) SetMoveStatus(ctx context.Context, name, uid string, status volumeapi.MoveStatus) error {
	if !status.CapacityApproved {
		return r.memoryRepository.SetMoveStatus(ctx, name, uid, status)
	}
	r.writes++
	if r.accepted {
		if err := r.memoryRepository.SetMoveStatus(ctx, name, uid, status); err != nil {
			return err
		}
	}
	return fmt.Errorf("capacity status timeout")
}

func TestCapacityApprovalFailureIsNotReplayedByDiagnostics(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%v", accepted), func(t *testing.T) {
			w := newMobilityWalk()
			w.observeAndLock(t)
			state := w.repository.volumes[w.volumeID]
			state.CapacityBytes = 32 << 20
			w.repository.volumes[w.volumeID] = state
			w.ensurePlacementOnDestination(t)
			w.move.Status.Phase = string(fsm.PhaseWaitingForCapacity)
			w.move.Status.CapacityApproved = false
			w.move.Status.DestinationPoolUID = ""
			w.move.Status.DestinationNode = "destination"
			w.repository.moves[0] = w.move
			for i := range w.repository.pools {
				w.repository.pools[i].CapacityLimit = "128Mi"
			}
			w.reconciler.CapacityProbe = fakeMoveCapacityProbe{usage: 1, stats: poolcapacity.Filesystem{AvailableBytes: 128 << 20}}
			w.reconciler.PoolLocks = &poolcapacity.Locker{}
			repository := &approvalFailureRepository{memoryRepository: w.repository, accepted: accepted}
			w.reconciler.Repository = repository
			err := w.reconciler.reconcileMove(w.ctx, w.move)
			if !errors.Is(err, errCapacityApprovalPersistence) || repository.writes != 1 {
				t.Fatalf("approval was replayed outside its lock: writes=%d err=%v", repository.writes, err)
			}
			if w.repository.moves[0].Status.CapacityApproved != accepted {
				t.Fatalf("ambiguous response changed durable approval: %+v", w.repository.moves[0].Status)
			}
		})
	}
}
