package uninstall

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func poolPreparationFixture() (volumeapi.Move, volumeapi.Pool, volumeapi.Pool) {
	source := &volume.CopyIdentity{InstallationID: "installation", PoolName: "source", PoolUID: "source-uid", NodeName: "node-a",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing}
	target := volumeapi.Pool{Name: "destination", UID: "destination-uid", NodeName: "node-b"}
	other := volumeapi.Pool{Name: "other", UID: "other-uid", NodeName: target.NodeName}
	move := volumeapi.Move{Name: "move", UID: "move-uid", Spec: volumeapi.MoveSpec{
		ProtocolVersion: volumeapi.MoveProtocolPreparedV1, VolumeID: source.VolumeID, SourceNode: source.NodeName},
		Status: volumeapi.MoveStatus{Phase: "Pending", SourceCopy: source, CandidateNodes: []string{target.NodeName},
			PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid", ClaimNamespace: "app", ClaimName: "claim", ClaimUID: "claim-uid",
			ConsumerName: "consumer", ConsumerUID: "consumer-uid", PlacementPodUID: "placement-uid",
			Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: target.Name, UID: target.UID}, NodeName: target.NodeName, ReservedBytes: 64}}}
	return move, target, other
}

func TestPoolDeleteProtectsPreparationWithoutBlockingOtherPoolOnNode(t *testing.T) {
	for _, phase := range []string{"Pending", "Blocked", "Succeeded"} {
		t.Run(phase, func(t *testing.T) {
			move, target, other := poolPreparationFixture()
			move.Status.Phase = phase
			if phase == "Succeeded" {
				move.Status.CleanupPhase = "Completed" // Contradictory proof must not discard the hold.
			}
			now := time.Date(2026, time.October, 15, 0, 0, 0, 0, time.UTC)
			for _, pool := range []*volumeapi.Pool{&target, &other} {
				pool.Generation = 1
				pool.Status = volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{
					ObservedAt: metav1.NewTime(now), Valid: true}}
			}
			repository := &memoryRepository{pools: []volumeapi.Pool{target, other}, moves: []volumeapi.Move{move}}
			checker := &Checker{Client: fake.NewClientset(), Volumes: repository, Cleanups: cleanupRepository{},
				StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system", Now: func() time.Time { return now }}
			for _, pool := range []volumeapi.Pool{target, other} {
				report, err := checker.CheckPoolDeleteAfter(context.Background(), pool.Name, types.UID(pool.UID), now.Add(-time.Second))
				wantBlocked := pool.UID == target.UID
				if err != nil || report.Safe() == wantBlocked {
					t.Fatalf("pool=%s safe=%t blockers=%+v err=%v", pool.Name, report.Safe(), report.Blockers, err)
				}
				if wantBlocked && (len(report.Blockers) != 1 || report.Blockers[0].Kind != "ShiftPVMove") {
					t.Fatalf("target was not protected by its preparation: %+v", report.Blockers)
				}
			}
			report, err := checker.Check(context.Background())
			if err != nil || report.Safe() || len(report.Blockers) != 1 || report.Blockers[0].Kind != "ShiftPVMove" {
				t.Fatalf("driver removal ignored preparation: %+v err=%v", report, err)
			}
		})
	}
}

func TestPreparationPoolDeleteProtectionSurvivesContradictions(t *testing.T) {
	for name, mutate := range map[string]func(*volumeapi.Move){
		"wrong target name":             func(m *volumeapi.Move) { m.Status.Preparation.Pool.Name = "contradictory-name" },
		"wrong target node":             func(m *volumeapi.Move) { m.Status.Preparation.NodeName = "contradictory-node" },
		"missing target name":           func(m *volumeapi.Move) { m.Status.Preparation.Pool.Name = "" },
		"missing target uid":            func(m *volumeapi.Move) { m.Status.Preparation.Pool.UID = "" },
		"missing target identity":       func(m *volumeapi.Move) { m.Status.Preparation.Pool = volumeapi.PoolReference{} },
		"missing target node and uid":   func(m *volumeapi.Move) { m.Status.Preparation.NodeName, m.Status.Preparation.Pool.UID = "", "" },
		"legacy move with preparation":  func(m *volumeapi.Move) { m.Spec.ProtocolVersion = "" },
		"unknown move with preparation": func(m *volumeapi.Move) { m.Spec.ProtocolVersion = "prepared-v2" },
		"recovery claims settled": func(m *volumeapi.Move) {
			m.Status.Phase, m.Status.RecoveryPhase, m.Status.CapacityReason = "Blocked", "Recovered", "RecoverySettled"
		},
		"both reservation forms": func(m *volumeapi.Move) { m.Status.CapacityApproved = true },
	} {
		t.Run(name, func(t *testing.T) {
			move, target, _ := poolPreparationFixture()
			mutate(&move)
			checker := &Checker{Volumes: &memoryRepository{moves: []volumeapi.Move{move}}}
			for _, pool := range []*volumeapi.Pool{&target, nil} {
				blockers, err := checker.moveBlockers(context.Background(), pool)
				if err != nil || len(blockers) != 1 || blockers[0].Name != move.Name {
					t.Fatalf("contradiction erased preparation blocker: %+v err=%v", blockers, err)
				}
			}
		})
	}
}

func TestPreparationProtectsBothSourceAndTargetPoolsOnOneNode(t *testing.T) {
	move, target, other := poolPreparationFixture()
	target.NodeName, other.NodeName, move.Status.Preparation.NodeName = "node-a", "node-a", "node-a"
	move.Status.CandidateNodes = []string{"node-a"}
	source := volumeapi.Pool{Name: move.Status.SourceCopy.PoolName, UID: move.Status.SourceCopy.PoolUID, NodeName: "node-a"}
	for _, pool := range []volumeapi.Pool{source, target, other} {
		want := pool.UID != other.UID
		if got := moveUsesPool(move, pool); got != want {
			t.Fatalf("pool=%s blocked=%t want=%t", pool.Name, got, want)
		}
	}
}
