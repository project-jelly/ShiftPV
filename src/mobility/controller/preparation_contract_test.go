package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// A status-write fence is too late: the legacy executor can evict, lock or
// delete before writing status. Exercise the public pass with all writes wired.
func TestUnknownProtocolRetainsEveryLegacyEffectBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		status volumeapi.MoveStatus
	}{
		{"uninitialized", volumeapi.MoveStatus{}},
		{"pending", volumeapi.MoveStatus{Phase: string(fsm.PhasePending)}},
		{"locking", volumeapi.MoveStatus{Phase: string(fsm.PhaseLocking)}},
		{"recovery", volumeapi.MoveStatus{Phase: string(fsm.PhaseBlocked)}},
		{"terminal cleanup", volumeapi.MoveStatus{Phase: string(fsm.PhaseSucceeded), CleanupPhase: cleanupapi.PhaseRunning}},
		{"terminal finalizer", volumeapi.MoveStatus{Phase: string(fsm.PhaseSucceeded), CleanupPhase: cleanupapi.PhaseCompleted}},
		{"recovered hold", volumeapi.MoveStatus{Phase: string(fsm.PhaseBlocked), RecoveryPhase: recoveryRecovered,
			CapacityReason: "RecoverySettled", Preparation: &volumeapi.MovePreparation{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, repo, client := preflightFixture()
			repo.moves = []volumeapi.Move{{Name: "prepared", UID: "prepared-uid", Finalizers: []string{volumeapi.MoveProtectionFinalizer},
				Spec:   volumeapi.MoveSpec{VolumeID: preflightVolume, SourceNode: "source", ProtocolVersion: "future-v2", Recovery: "ResumeOwner"},
				Status: test.status}}
			writes := &forbidPreparedEffects{Repository: repo}
			cleanup := &recordingCleanupOperator{}
			r.Repository, r.CleanupOperator = writes, cleanup
			for range 2 { // Relisting must not silently downgrade the record.
				err := r.ReconcileAll(context.Background())
				if err == nil || !strings.Contains(err.Error(), "unsupported protocol") {
					t.Fatalf("expected unsupported protocol diagnostic, got %v", err)
				}
			}
			if writes.calls != 0 || cleanup.reclaims != 0 {
				t.Fatalf("legacy effects: repository=%d cleanup=%d", writes.calls, cleanup.reclaims)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("unexpected Kubernetes effect: %#v", action)
				}
			}
		})
	}
}

func TestUnknownMoveProtocolDoesNotBlockIndependentLegacyMove(t *testing.T) {
	r, repo, _ := preflightFixture()
	repo.moves = []volumeapi.Move{
		{Name: "future", UID: "future-uid", Spec: volumeapi.MoveSpec{VolumeID: "future-volume", ProtocolVersion: "future-v2"}},
		{Name: "legacy", UID: "legacy-uid", Spec: volumeapi.MoveSpec{VolumeID: preflightVolume, SourceNode: "source"}},
	}
	if err := r.ReconcileAll(context.Background()); err == nil || !strings.Contains(err.Error(), "future-v2") {
		t.Fatalf("expected unsupported record diagnostic, got %v", err)
	}
	if len(repo.moves) != 2 || repo.moves[1].Status.Phase != string(fsm.PhasePending) || repo.moves[0].Status.Phase != "" {
		t.Fatalf("independent legacy progress or protocol fence lost: %+v", repo.moves)
	}
}

func TestUnsettledOrUnknownProtocolBlocksOnlyItsVolume(t *testing.T) {
	for _, test := range []struct {
		name, protocol string
		preparation    *volumeapi.MovePreparation
	}{
		{name: "preparing", protocol: volumeapi.MoveProtocolPreparedV1},
		{name: "future", protocol: "future-v2"},
		{name: "hold without protocol", preparation: &volumeapi.MovePreparation{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, client, repo, pod := ownerPlacementFixture()
			move := volumeapi.Move{Name: "retained", UID: "retained-uid",
				Spec: volumeapi.MoveSpec{VolumeID: ownerPlacementVolumeID, SourceNode: "source", ProtocolVersion: test.protocol},
				Status: volumeapi.MoveStatus{Phase: string(fsm.PhaseBlocked), RecoveryPhase: recoveryRecovered,
					CapacityReason: "RecoverySettled", Preparation: test.preparation}}
			if test.protocol == volumeapi.MoveProtocolPreparedV1 {
				move.Status = volumeapi.MoveStatus{Phase: string(fsm.PhasePreparing)}
			}
			repo.moves = []volumeapi.Move{move}
			if !activeMoveVolumes(repo.moves, repo.volumes)[ownerPlacementVolumeID] {
				t.Fatal("discovery may create a second Move over an unsettled or unsupported protocol")
			}
			if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
				t.Fatal(err)
			}
			stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil || !hasPlacementHold(stored) {
				t.Fatalf("unsettled or unsupported protocol released placement: pod=%+v err=%v", stored, err)
			}
			repo.moves[0].Spec.VolumeID = "independent-volume"
			if err := r.reconcileOwnerPlacements(context.Background()); err != nil {
				t.Fatal(err)
			}
			stored, err = client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil || hasPlacementHold(stored) {
				t.Fatalf("independent hold did not recover: pod=%+v err=%v", stored, err)
			}
		})
	}
}

func TestPreparationCountsLogicalCapacityWithoutPhysicalExecutionHold(t *testing.T) {
	r, repo, current := capacityCostFixture()
	id := "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	source, _, _ := testCopyIdentities(id, "source", "destination")
	source.PoolName, source.PoolUID = "source", "source-uid"
	prepared := volumeapi.Move{Name: "prepared", UID: "prepared-uid",
		Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: "source", ProtocolVersion: volumeapi.MoveProtocolPreparedV1},
		Status: volumeapi.MoveStatus{SourceCopy: &source, SourceBytes: 100 << 20,
			PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid", ClaimNamespace: "app", ClaimName: "claim", ClaimUID: "claim-uid",
			ConsumerName: "consumer", ConsumerUID: "consumer-uid", PlacementPodUID: "placement-uid",
			Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: "destination", UID: "destination-uid"}, NodeName: "destination", ReservedBytes: 16 << 20}}}
	repo.moves = append(repo.moves, prepared)
	repo.volumes[id] = volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: "source",
		CurrentCopy: &source, ActiveMove: prepared.Name, CapacityBytes: prepared.Status.Preparation.ReservedBytes}
	_, logical, physical, _, err := r.destinationCapacityForPool(context.Background(), current, repo.pools[1])
	if err != nil || logical != 16<<20 || physical != 0 {
		t.Fatalf("preparation accounting: logical=%d physical=%d err=%v", logical, physical, err)
	}
}

type forbidPreparedEffects struct {
	Repository
	calls int
}

func (r *forbidPreparedEffects) reject() error {
	r.calls++
	return fmt.Errorf("unexpected legacy repository write")
}
func (r *forbidPreparedEffects) CompareAndSetState(context.Context, string, string, string, string, volumeapi.State) error {
	return r.reject()
}
func (r *forbidPreparedEffects) CompareAndSetSourceState(context.Context, string, volumeapi.State, volumeapi.State) error {
	return r.reject()
}
func (r *forbidPreparedEffects) CompareAndSetMoveStatus(context.Context, volumeapi.Move, volumeapi.MoveStatus) (volumeapi.Move, error) {
	return volumeapi.Move{}, r.reject()
}
func (r *forbidPreparedEffects) RequestPoolScan(context.Context, volume.CopyIdentity) (int64, error) {
	return 0, r.reject()
}
func (r *forbidPreparedEffects) CreateMove(context.Context, string, volumeapi.MoveSpec) (volumeapi.Move, error) {
	return volumeapi.Move{}, r.reject()
}
func (r *forbidPreparedEffects) AddMoveFinalizer(context.Context, string, string) error {
	return r.reject()
}
func (r *forbidPreparedEffects) RemoveMoveFinalizer(context.Context, string, string) error {
	return r.reject()
}
func (r *forbidPreparedEffects) DeleteMove(context.Context, string, string) error {
	return r.reject()
}
func (r *forbidPreparedEffects) SetMoveStatus(context.Context, string, string, volumeapi.MoveStatus) error {
	return r.reject()
}
