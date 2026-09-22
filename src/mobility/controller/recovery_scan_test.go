package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func observeRollbackGeneration(repo *memoryRepository) {
	pool := &repo.pools[1]
	pool.Status.ObservedGeneration = pool.Generation
	for i := range pool.Status.Conditions {
		pool.Status.Conditions[i].ObservedGeneration = pool.Generation
	}
}

func TestRollbackScanRejectsPreRetiringInventoryDespiteClockSkew(t *testing.T) {
	for _, offset := range []time.Duration{2 * time.Second, -2 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			ctx := context.Background()
			r, repo, _, _ := rollbackRecoveryFixture(t, nil)
			repo.moves[0].Status.RollbackRequiredGeneration = 0
			move := repo.moves[0]
			retiring, _ := time.Parse(time.RFC3339Nano, move.Status.LastTransitionTime)
			// Scan really began one second before Retiring; the node uses another clock.
			repo.pools[1].Status.Inventory.ObservedAt = metav1.NewTime(retiring.Add(-time.Second + offset))
			repo.pools[1].Status.LastProbeTime = repo.pools[1].Status.Inventory.ObservedAt
			r.Now = func() time.Time { return retiring.Add(3 * time.Second) }
			state, _ := repo.Get(ctx, move.Spec.VolumeID)
			if done, err := r.settleRecoveryArtifacts(ctx, &move, state); done || err != nil {
				t.Fatalf("open fence: done=%v err=%v", done, err)
			}
			required := repo.moves[0].Status.RollbackRequiredGeneration
			if required != 2 || !repo.moves[0].Status.CapacityApproved {
				t.Fatalf("unfenced scan released hold: %+v", repo.moves[0].Status)
			}
			// Reconstruct the reconciler and reload durable state as after a restart.
			restarted := &Reconciler{Repository: repo, Cleanups: r.Cleanups, Now: r.Now, PoolReadinessStaleAfter: time.Minute}
			move = repo.moves[0]
			if done, err := restarted.settleRecoveryArtifacts(ctx, &move, state); done || err == nil {
				t.Fatalf("old scan accepted after restart: done=%v err=%v", done, err)
			}
			if repo.pools[1].Generation != required || !repo.moves[0].Status.CapacityApproved {
				t.Fatal("retry changed fence or released hold")
			}
			// A later scan proves causality even if its node clock is behind Retiring.
			observeRollbackGeneration(repo)
			repo.pools[1].Status.Inventory.ObservedAt = metav1.NewTime(retiring.Add(time.Second + offset))
			repo.pools[1].Status.LastProbeTime = repo.pools[1].Status.Inventory.ObservedAt
			move = repo.moves[0]
			if done, err := restarted.settleRecoveryArtifacts(ctx, &move, state); done || err != nil {
				t.Fatalf("new scan rejected: done=%v err=%v", done, err)
			}
			if repo.moves[0].Status.CapacityApproved {
				t.Fatal("causally fenced absence did not settle hold")
			}
			if _, err := r.Cleanups.Get(ctx, cleanupapiAuthority(move)); !errors.Is(err, cleanupapi.ErrNoJournal) {
				t.Fatalf("absence fabricated a receipt: %v", err)
			}
		})
	}
}

func TestRollbackScanRequiresCurrentExactPoolObservation(t *testing.T) {
	for _, scenario := range []string{"concurrent scan", "replaced pool", "unprotected pool", "partial inventory", "published transaction"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
			repo.moves[0].Status.RollbackRequiredGeneration = 0
			move := repo.moves[0]
			state, _ := repo.Get(ctx, move.Spec.VolumeID)
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil {
				t.Fatal(err)
			}
			observeRollbackGeneration(repo)
			switch scenario {
			case "concurrent scan":
				repo.pools[1].Generation++
			case "replaced pool":
				repo.pools[1].UID = "replacement-pool"
			case "unprotected pool":
				repo.pools[1].Finalizers = nil
				r.Repository = &rawRollbackPools{repo}
			case "partial inventory":
				repo.pools[1].Status.Inventory.Truncated = true
			case "published transaction":
				repo.pools[1].Status.Inventory.Copies = []volumeapi.CopyObservation{{Identity: &incoming, Present: true, Published: true}}
			}
			move = repo.moves[0]
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err == nil {
				t.Fatal("unsafe observation accepted")
			}
			if !repo.moves[0].Status.CapacityApproved {
				t.Fatal("unsafe observation released hold")
			}
			if scenario == "concurrent scan" {
				observeRollbackGeneration(repo)
				move = repo.moves[0]
				if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil {
					t.Fatal(err)
				}
				if repo.moves[0].Status.CapacityApproved {
					t.Fatal("newer confirmed generation did not settle")
				}
			}
		})
	}
}

type rollbackFaultRepository struct {
	*memoryRepository
	scanFault   bool
	statusFault bool
	apply       bool
}

func (r *rollbackFaultRepository) RequestPoolScan(ctx context.Context, target volume.CopyIdentity) (int64, error) {
	generation, err := r.memoryRepository.RequestPoolScan(ctx, target)
	if r.scanFault {
		r.scanFault = false
		return 0, errors.New("scan response lost")
	}
	return generation, err
}
func (r *rollbackFaultRepository) SetMoveStatus(ctx context.Context, name, uid string, next volumeapi.MoveStatus) error {
	if r.statusFault {
		r.statusFault = false
		if r.apply {
			if err := r.memoryRepository.SetMoveStatus(ctx, name, uid, next); err != nil {
				return err
			}
		}
		return errors.New("status response lost")
	}
	return r.memoryRepository.SetMoveStatus(ctx, name, uid, next)
}

func TestRollbackScanRecoversAmbiguousWritesWithoutReusingOldProof(t *testing.T) {
	for _, scenario := range []string{"scan response lost", "status not applied", "status response lost"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			r, repo, _, _ := rollbackRecoveryFixture(t, nil)
			repo.moves[0].Status.RollbackRequiredGeneration = 0
			fault := &rollbackFaultRepository{memoryRepository: repo, scanFault: scenario == "scan response lost", statusFault: scenario != "scan response lost", apply: scenario == "status response lost"}
			r.Repository = fault
			move := repo.moves[0]
			state, _ := repo.Get(ctx, move.Spec.VolumeID)
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err == nil {
				t.Fatal("fault was not injected")
			}
			if !repo.moves[0].Status.CapacityApproved {
				t.Fatal("ambiguous write released hold")
			}
			observeRollbackGeneration(repo) // This scan may precede the retried request.
			move = repo.moves[0]
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil {
				t.Fatal(err)
			}
			if scenario == "status response lost" {
				if repo.moves[0].Status.CapacityApproved || repo.pools[1].Generation != 2 {
					t.Fatal("persisted fence was not reused")
				}
				return
			}
			if !repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.RollbackRequiredGeneration != 3 {
				t.Fatal("unrecorded request was not replaced with a new fence")
			}
			move = repo.moves[0]
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err == nil {
				t.Fatal("earlier scan satisfied new fence")
			}
			observeRollbackGeneration(repo)
			move = repo.moves[0]
			if _, err := r.settleRecoveryArtifacts(ctx, &move, state); err != nil {
				t.Fatal(err)
			}
			if repo.moves[0].Status.CapacityApproved {
				t.Fatal("new scan did not settle hold")
			}
		})
	}
}

// The ordinary fixture repairs finalizers; this repository exposes the exact
// observed Pool for protection-loss tests.
type rawRollbackPools struct{ *memoryRepository }

func (r *rawRollbackPools) Pools(context.Context) ([]volumeapi.Pool, error) { return r.pools, nil }

func TestRollbackScanWaitsForDurableRetiringIntent(t *testing.T) {
	r, repo, _, _ := rollbackRecoveryFixture(t, nil)
	move := repo.moves[0]
	move.Status.RecoveryPhase = recoveryVerifying
	move.Status.RollbackRequiredGeneration = 0
	if _, err := r.ensureRollbackScan(context.Background(), &move); !errors.Is(err, errRecoveryCleanupNeedsReview) {
		t.Fatalf("pre-Retiring fence was accepted: %v", err)
	}
	if repo.pools[1].Generation != 1 || !repo.moves[0].Status.CapacityApproved {
		t.Fatal("pre-Retiring intent changed durable state")
	}
}
