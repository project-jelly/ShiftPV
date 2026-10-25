package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
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

func TestMoveCapacityConvertsExactHoldAmongSameNodePools(t *testing.T) {
	r, repo, move := capacityCostFixture()
	repo.pools[1].Name = "a-full"
	repo.pools = append(repo.pools, volumeapi.Pool{Name: "b-free", UID: "free-uid", NodeName: "destination", MountPath: "/free", CapacityLimit: "128Mi"},
		volumeapi.Pool{Name: "c-other", UID: "other-uid", NodeName: "destination", MountPath: "/other", CapacityLimit: "1Ti", PoolGroup: "other"})
	repo.volumes["existing"] = volumeapi.State{OwnerNode: "destination", CapacityBytes: 128 << 20}
	// Logical admission already chose the independent Pool; execution must use that exact hold.
	prepareCapacityMove(repo, &move, repo.pools[2])
	probe := &poolAwareCapacityProbe{}
	r.CapacityProbe = probe
	r.SourceUsage = r.CapacityProbe.(SourceUsage)
	if err := r.ensureCapacity(context.Background(), &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	if !move.Status.CapacityApproved || move.Status.DestinationPoolUID != "free-uid" {
		t.Fatalf("admission selected wrong Pool: %+v", move.Status)
	}
	if probe.measured == nil || probe.measured.PoolUID != "source-uid" {
		t.Fatalf("source usage identity = %+v", probe.measured)
	}
	// Approval is a one-time transition; subsequent observations keep its exact Pool.
	probe.probed = nil
	eligible, err := r.destinationPools(context.Background(), move, "destination")
	if err != nil || len(eligible) != 1 || eligible[0].UID != "free-uid" || len(probe.probed) != 0 {
		t.Fatalf("approved Pool was reallocated: %+v %v", eligible, err)
	}
	repo.pools[2].UID = "replacement-uid"
	if _, err := r.destinationPools(context.Background(), move, "destination"); err == nil {
		t.Fatal("recreated Pool inherited approval")
	}
	if !move.Status.CapacityApproved || move.Status.DestinationPoolUID != "free-uid" {
		t.Fatal("failed observation released durable hold")
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
	if err != nil || logical != 32<<20 || physical != 0 {
		t.Fatalf("other Pool's hold leaked: logical=%d physical=%d err=%v", logical, physical, err)
	}
	states, err := repo.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	logical, err = poolcapacity.ReservedBytesForPool(states, repo.moves, other.UID)
	if err != nil {
		t.Fatal(err)
	}
	physical, err = pendingPhysicalBytes(current, other, states, repo.moves)
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
	if err := prepareMoveCopyIdentitiesForPool(&w.move, w.repository.pools[3]); err != nil {
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
	w.move = preparedFixtureMove(w.move)
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

func TestCapacityApprovalFailureIsNotReplayedByDiagnostics(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%v", accepted), func(t *testing.T) {
			f, move := preparationAtCapacity(t)
			failure := errors.New("approval response lost or rejected")
			writes := 0
			f.dynamic.PrependReactor("update", "shiftpvmoves", func(action ktesting.Action) (bool, runtime.Object, error) {
				object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				approved, _, _ := unstructured.NestedBool(object.Object, "status", "capacityApproved")
				if action.GetSubresource() != "status" || !approved {
					return false, nil, nil
				}
				writes++
				if !accepted {
					return true, nil, failure
				}
				return false, nil, nil
			})
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				approved, _, _ := unstructured.NestedBool(object.Object, "status", "capacityApproved")
				if resource == volumeapi.MoveResource && approved {
					return failure
				}
				return nil
			}
			err := f.reconciler.reconcileMove(f.ctx, move)
			if !errors.Is(err, errCapacityApprovalPersistence) || !errors.Is(err, failure) || writes != 1 {
				t.Fatalf("approval was replayed outside its lock: writes=%d err=%v", writes, err)
			}
			persisted := f.move(t, 0)
			if persisted.Status.CapacityApproved != accepted {
				t.Fatalf("ambiguous response changed approval: %+v", persisted.Status)
			}
			if accepted {
				persisted.Status.RecoveryPhase = recoveryRetiring
				if err := validRollbackIntent(persisted); err != nil {
					t.Fatal(err)
				}
			} else if !noDestinationEffectIntent(persisted) || persisted.Status.Preparation == nil {
				t.Fatal("rejected approval changed the logical hold or execution plan")
			}
		})
	}
}

func (p *poolAwareCapacityProbe) AdvanceSourceUsage(ctx context.Context, _ volumeapi.Move, copy volume.CopyIdentity) (int64, bool, error) {
	measured, err := p.VolumeUsageForCopy(ctx, copy)
	return measured, true, err
}
func (p *poolAwareCapacityProbe) FinalizeSourceUsage(context.Context, volumeapi.Move) (bool, error) {
	return true, nil
}
