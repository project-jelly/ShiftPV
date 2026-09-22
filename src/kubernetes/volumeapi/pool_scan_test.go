package volumeapi

import (
	"context"
	"errors"
	"math"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func scanPoolFixture() (*unstructured.Unstructured, volume.CopyIdentity) {
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "destination", "uid": "pool-uid", "generation": int64(4), "finalizers": []any{PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "node-b", "scanEpoch": int64(2)},
	}}
	return pool, volume.CopyIdentity{PoolName: "destination", PoolUID: "pool-uid", NodeName: "node-b"}
}

func TestRequestPoolScanRetriesConflictsAndReturnsPersistedGeneration(t *testing.T) {
	pool, target := scanPoolFixture()
	client := fake.NewSimpleDynamicClient(runtime.NewScheme(), pool)
	updates := 0
	client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(PoolResource.GroupResource(), pool.GetName(), errors.New("concurrent scan"))
		}
		next := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		next.SetGeneration(next.GetGeneration() + 1)
		return false, nil, nil
	})
	registry := &Registry{Client: client}
	for _, expected := range []int64{5, 6} {
		got, err := registry.RequestPoolScan(context.Background(), target)
		if err != nil || got != expected {
			t.Fatalf("generation=%d expected=%d err=%v", got, expected, err)
		}
	}
	stored, err := client.Resource(PoolResource).Get(context.Background(), target.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	epoch, _, _ := unstructured.NestedInt64(stored.Object, "spec", "scanEpoch")
	if updates != 3 || epoch != 4 {
		t.Fatalf("updates=%d epoch=%d", updates, epoch)
	}
}

func TestRequestPoolScanRejectsChangedIdentityAndInvalidEpoch(t *testing.T) {
	for _, scenario := range []string{"uid", "node", "protection", "exhausted", "negative", "malformed", "no generation advance"} {
		t.Run(scenario, func(t *testing.T) {
			pool, target := scanPoolFixture()
			switch scenario {
			case "uid":
				pool.SetUID("replaced")
			case "node":
				_ = unstructured.SetNestedField(pool.Object, "other", "spec", "nodeName")
			case "protection":
				pool.SetFinalizers(nil)
			case "exhausted":
				_ = unstructured.SetNestedField(pool.Object, int64(math.MaxInt64), "spec", "scanEpoch")
			case "negative":
				_ = unstructured.SetNestedField(pool.Object, int64(-1), "spec", "scanEpoch")
			case "malformed":
				_ = unstructured.SetNestedField(pool.Object, "bad", "spec", "scanEpoch")
			}
			registry := &Registry{Client: fake.NewSimpleDynamicClient(runtime.NewScheme(), pool)}
			if got, err := registry.RequestPoolScan(context.Background(), target); got != 0 || err == nil {
				t.Fatalf("generation=%d err=%v", got, err)
			}
		})
	}
}

func TestRollbackGenerationSurvivesStatusWritesAndCannotBeReplaced(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move", "uid": "move-uid"},
		"spec":     map[string]any{"volumeID": "volume", "sourceNode": "source"},
	}}
	registry := &Registry{Client: fake.NewSimpleDynamicClient(runtime.NewScheme(), object)}
	ctx := context.Background()
	status := MoveStatus{Phase: "Blocked", RecoveryPhase: "Retiring", CapacityApproved: true, RollbackRequiredGeneration: 5}
	if err := registry.SetMoveStatus(ctx, "move", "move-uid", status); err != nil {
		t.Fatal(err)
	}
	move, err := registry.GetMove(ctx, "move")
	if err != nil || move.Status.RollbackRequiredGeneration != 5 {
		t.Fatalf("roundtrip=%+v err=%v", move.Status, err)
	}
	move.Status.RecoveryMessage = "waiting for scan"
	if err := registry.SetMoveStatus(ctx, "move", "move-uid", move.Status); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []int64{0, 4, 6, -1} {
		stale := move.Status
		stale.RollbackRequiredGeneration = generation
		if err := registry.SetMoveStatus(ctx, "move", "move-uid", stale); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("replacement %d accepted: %v", generation, err)
		}
	}
}
