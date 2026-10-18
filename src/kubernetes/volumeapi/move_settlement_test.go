package volumeapi

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func destinationSettlementFixture() (Move, State, Pool) {
	move := preparedMoveFixture()
	move.Status = approvedPreparation(move)
	move.Status.Phase, move.Status.RecoveryPhase, move.Status.RecoveryOwner = "Blocked", "Retiring", "destination"
	move.Status.SettlementRequested, move.Status.RollbackRequiredGeneration = true, 2
	copy := move.Status.DestinationCopy
	state := State{UID: copy.VolumeUID, Phase: PhaseBlocked, OwnerNode: copy.NodeName, ActiveMove: move.Name, CurrentCopy: copy}
	pool := Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, Generation: 2,
		Status: PoolStatus{ObservedGeneration: 2, Inventory: &PoolInventory{Valid: true, Copies: []CopyObservation{{Identity: copy, Present: true}}}}}
	return move, state, pool
}

func TestUnpublishedSettlementRequiresExactDurableAuthority(t *testing.T) {
	for _, scenario := range []string{"valid", "no intent", "ordinary recovery", "before verification", "wrong owner", "new move", "new Volume UID", "different copy", "published", "deleting", "cleanup", "no scan fence", "missing source", "missing destination", "changed transaction", "same node", "same Pool"} {
		t.Run(scenario, func(t *testing.T) {
			move, state, _ := destinationSettlementFixture()
			switch scenario {
			case "no intent":
				move.Status.SettlementRequested = false
			case "ordinary recovery":
				move.Status.SettlementRequested, move.Spec.Recovery = false, "ResumeOwner"
			case "before verification":
				move.Status.RecoveryPhase = "Verifying"
			case "wrong owner":
				state.OwnerNode = "source"
			case "new move":
				state.ActiveMove = "other-move"
			case "new Volume UID":
				state.UID = "new-volume-uid"
			case "different copy":
				copy := *state.CurrentCopy
				copy.CopyID = "other-copy"
				state.CurrentCopy = &copy
			case "published":
				state.PublishedNodes = []string{state.OwnerNode}
			case "deleting":
				state.Deleting = true
			case "cleanup":
				state.HasCleanup = true
			case "no scan fence":
				move.Status.RollbackRequiredGeneration = 0
			case "missing source":
				move.Status.SourceCopy = nil
			case "missing destination":
				move.Status.DestinationCopy = nil
			case "changed transaction":
				move.Status.PromotionOperationID = "other-operation"
			case "same node":
				move.Spec.SourceNode = move.Status.DestinationNode
			case "same Pool":
				move.Status.SourceCopy.PoolUID = move.Status.DestinationPoolUID
			}
			if got := MoveRetiresUnpublishedSource(move, state); got != (scenario == "valid") {
				t.Fatalf("authority accepted=%v for %s", got, scenario)
			}
		})
	}
}

func TestUnpublishedSettlementInventoryCannotUseOldOrAmbiguousProof(t *testing.T) {
	for _, scenario := range []string{"valid", "old generation", "pending scan", "different Pool", "different node", "missing inventory", "invalid inventory", "partial inventory", "inventory error", "missing copy", "absent copy", "published copy", "problem", "duplicate", "foreign incarnation"} {
		t.Run(scenario, func(t *testing.T) {
			move, _, pool := destinationSettlementFixture()
			switch scenario {
			case "old generation":
				pool.Generation, pool.Status.ObservedGeneration = 1, 1
			case "pending scan":
				pool.Status.ObservedGeneration = 1
			case "different Pool":
				pool.UID = "new-pool-uid"
			case "different node":
				pool.NodeName = "other-node"
			case "missing inventory":
				pool.Status.Inventory = nil
			case "invalid inventory":
				pool.Status.Inventory.Valid = false
			case "partial inventory":
				pool.Status.Inventory.Truncated = true
			case "inventory error":
				pool.Status.Inventory.Message = "scanner failed"
			case "missing copy":
				pool.Status.Inventory.Copies = nil
			case "absent copy":
				pool.Status.Inventory.Copies[0].Present = false
			case "published copy":
				pool.Status.Inventory.Copies[0].Published = true
			case "problem":
				pool.Status.Inventory.Copies[0].Problem = "identity changed"
			case "duplicate":
				pool.Status.Inventory.Copies = append(pool.Status.Inventory.Copies, pool.Status.Inventory.Copies[0])
			case "foreign incarnation":
				foreign := *move.Status.DestinationCopy
				foreign.CopyID = "foreign-copy"
				pool.Status.Inventory.Copies = append(pool.Status.Inventory.Copies, CopyObservation{Identity: &foreign, Present: true})
			}
			if got := PoolHasUnpublishedCopy(pool, move.Status.DestinationCopy, 2); got != (scenario == "valid") {
				t.Fatalf("inventory accepted=%v for %s", got, scenario)
			}
		})
	}
}

func TestSettlementIntentSurvivesStatusWritersAndHasNoLegacyDefault(t *testing.T) {
	ctx := context.Background()
	move, _, _ := destinationSettlementFixture()
	object := preparedMoveObject(move)
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	registry := &Registry{Client: client}
	next := move.Status
	next.Message = "writer update"
	stored, err := registry.CompareAndSetMoveStatus(ctx, move, next)
	if err != nil || !stored.Status.SettlementRequested {
		t.Fatalf("intent was not retained: %+v %v", stored.Status, err)
	}
	next = stored.Status
	next.SettlementRequested = false
	if _, err := registry.CompareAndSetMoveStatus(ctx, stored, next); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("writer erased settlement intent: %v", err)
	}
	live, _ := registry.GetMove(ctx, move.Name)
	if !live.Status.SettlementRequested {
		t.Fatal("rejected writer removed intent")
	}
	legacy := preparedMoveObject(preparedMoveFixture())
	if _, found, _ := unstructured.NestedBool(legacy.Object, "status", "settlementRequested"); found {
		t.Fatal("ordinary status was backfilled with a settlement field")
	}
	if err := unstructured.SetNestedField(object.Object, "true", "status", "settlementRequested"); err != nil {
		t.Fatal(err)
	}
	if _, err := moveFrom(object); err == nil {
		t.Fatal("malformed settlement intent was accepted")
	}
	unapproved := preparedMoveFixture()
	next = unapproved.Status
	next.Phase, next.Preparation, next.SettlementRequested = "Blocked", nil, true
	if err := validateSettlementTransition(unapproved, next); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("unapproved hold bypassed cancellation: %v", err)
	}
}
