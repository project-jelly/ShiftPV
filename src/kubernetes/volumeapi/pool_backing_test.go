package volumeapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func existingBackingPoolFixture(now time.Time) Pool {
	pool := executionPoolFixture(now)
	pool.Generation = 1
	for _, kind := range []string{PoolConditionReady, PoolConditionCapacityIndependent} {
		condition := meta.FindStatusCondition(pool.Status.Conditions, kind)
		condition.Status, condition.Reason = metav1.ConditionFalse, "CapacityPeerUnproven"
	}
	return pool
}

func TestCapacityBackingLegacyFallbackDoesNotOverrideOwnEvidence(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		own         *metav1.Condition
		independent metav1.ConditionStatus
		generation  int64
		backing     bool
		allocation  bool
	}{
		{"legacy success", nil, metav1.ConditionTrue, 1, true, true},
		{"legacy failure", nil, metav1.ConditionFalse, 1, false, false},
		{"legacy unknown", nil, metav1.ConditionUnknown, 1, false, false},
		{"legacy outdated", nil, metav1.ConditionTrue, 2, false, false},
		{"own overrides failed peer", &metav1.Condition{Status: metav1.ConditionTrue, ObservedGeneration: 1}, metav1.ConditionFalse, 1, true, false},
		{"own failure overrides old success", &metav1.Condition{Status: metav1.ConditionFalse, ObservedGeneration: 1}, metav1.ConditionTrue, 1, false, false},
		{"own unknown overrides old success", &metav1.Condition{Status: metav1.ConditionUnknown, ObservedGeneration: 1}, metav1.ConditionTrue, 1, false, false},
		{"own outdated overrides old success", &metav1.Condition{Status: metav1.ConditionTrue, ObservedGeneration: 0}, metav1.ConditionTrue, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := existingBackingPoolFixture(now)
			meta.RemoveStatusCondition(&pool.Status.Conditions, PoolConditionCapacityBackingVerified)
			independent := meta.FindStatusCondition(pool.Status.Conditions, PoolConditionCapacityIndependent)
			independent.Status, independent.ObservedGeneration = tc.independent, tc.generation
			if tc.own != nil {
				condition := *tc.own
				condition.Type = PoolConditionCapacityBackingVerified
				pool.Status.Conditions = append(pool.Status.Conditions, condition)
			}
			if ready, _ := pool.CapacityBackingReady(); ready != tc.backing {
				t.Fatalf("own backing=%v want=%v", ready, tc.backing)
			}
			if ready, _ := pool.capacityReadyAt(pool.Generation); ready != tc.allocation {
				t.Fatalf("allocation evidence=%v want=%v", ready, tc.allocation)
			}
			pool.Status.CapacityUnit = nil
			if ready, _ := pool.CapacityBackingReady(); ready {
				t.Fatal("condition without its exact anchor admitted backing")
			}
		})
	}
}

func TestPeerEvidenceUsesOwnBackingWithoutCircularIndependence(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	a, b := existingBackingPoolFixture(now), existingBackingPoolFixture(now)
	b.Name, b.UID, b.MountPath = "other", "other-uid", "/other"
	b.Status.CapacityUnit.Device = "8:2"
	b.Status.CapacityUnit.Extents[0].Start = 200
	if ready, reason := PoolCapacityIndependent(a, []Pool{a, b}); !ready {
		t.Fatalf("healthy own evidence was poisoned by old independence verdicts: %s", reason)
	}
	meta.FindStatusCondition(b.Status.Conditions, PoolConditionCapacityBackingVerified).Status = metav1.ConditionFalse
	if ready, reason := PoolCapacityIndependent(a, []Pool{a, b}); ready || reason != "CapacityPeerUnproven" {
		t.Fatalf("lost peer backing admitted new allocation: %v %s", ready, reason)
	}
}

func TestInventoryPoolRequiresExactCurrentCompleteEvidence(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		change func(*Pool)
		want   bool
	}{
		{"peer unavailable", func(*Pool) {}, true},
		{"placement limit invalid", func(p *Pool) { p.CapacityLimit = "invalid" }, true},
		{"terminating protected Pool", func(p *Pool) { p.DeletionTimestamp = &metav1.Time{Time: now} }, true},
		{"missing protection", func(p *Pool) { p.Finalizers = nil }, false},
		{"own backing failed", func(p *Pool) {
			meta.FindStatusCondition(p.Status.Conditions, PoolConditionCapacityBackingVerified).Status = metav1.ConditionFalse
		}, false},
		{"own generation outdated", func(p *Pool) {
			meta.FindStatusCondition(p.Status.Conditions, PoolConditionCapacityBackingVerified).ObservedGeneration = 0
		}, false},
		{"scan requested", func(p *Pool) { p.Generation++ }, false},
		{"stale probe", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(-2 * time.Minute)) }, false},
		{"future probe", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(time.Minute)) }, false},
		{"missing inventory", func(p *Pool) { p.Status.Inventory = nil }, false},
		{"invalid inventory", func(p *Pool) { p.Status.Inventory.Valid = false }, false},
		{"truncated inventory", func(p *Pool) { p.Status.Inventory.Truncated = true }, false},
		{"inventory problem", func(p *Pool) { p.Status.Inventory.Message = "CopyObservationProblem" }, false},
		{"stale inventory", func(p *Pool) { p.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(-2 * time.Minute)) }, false},
		{"future inventory", func(p *Pool) { p.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(time.Minute)) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := existingBackingPoolFixture(now)
			tc.change(&pool)
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, backingPoolObject(t, pool))
			registry := &Registry{Client: client, Now: func() time.Time { return now }, PoolReadinessStaleAfter: time.Minute}
			_, err := registry.InventoryPoolForIdentity(context.Background(), pool.Name, pool.UID, pool.NodeName)
			if (err == nil) != tc.want {
				t.Fatalf("inventory error=%v want ready=%v", err, tc.want)
			}
			if err != nil && !errors.Is(err, ErrPoolNotReady) {
				t.Fatalf("unexpected rejection: %v", err)
			}
			if _, err := registry.InventoryPoolForIdentity(context.Background(), pool.Name, "old-uid", pool.NodeName); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("replaced Pool accepted: %v", err)
			}
			if _, err := registry.InventoryPoolForIdentity(context.Background(), pool.Name, pool.UID, "other-node"); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("wrong node accepted: %v", err)
			}
			if _, err := registry.ReadyPoolForIdentity(context.Background(), pool.Name, pool.UID, pool.NodeName); err == nil {
				t.Fatal("existing-copy evidence authorized a new allocation")
			}
		})
	}
}

func backingPoolObject(t *testing.T, pool Pool) *unstructured.Unstructured {
	t.Helper()
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pool.Status)
	if err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": pool.Name, "uid": pool.UID, "generation": pool.Generation},
		"spec": map[string]any{"nodeName": pool.NodeName, "poolGroup": pool.PoolGroup, "mountPath": pool.MountPath,
			"capacityPolicy": pool.CapacityPolicy, "capacity": map[string]any{"limit": pool.CapacityLimit}},
		"status": status,
	}}
	object.SetFinalizers(pool.Finalizers)
	object.SetDeletionTimestamp(pool.DeletionTimestamp)
	return object
}

func TestIndependentPeerFailurePreservesExistingBacking(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	pool := existingBackingPoolFixture(now)
	if ready, reason := pool.CleanupReadyAt(now, time.Minute); !ready {
		t.Fatalf("approved cleanup blocked by unrelated peer: %s", reason)
	}
	if ready, reason := pool.ExecutionReadyAt(now, time.Minute); !ready {
		t.Fatalf("approved native execution blocked by unrelated peer: %s", reason)
	}
	if ready, _ := poolReady(pool, now, time.Minute); ready {
		t.Fatal("new allocation ignored the unproven peer")
	}
}
