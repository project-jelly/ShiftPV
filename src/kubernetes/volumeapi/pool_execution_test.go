package volumeapi

import (
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func executionPoolFixture(now time.Time) Pool {
	conditions := []metav1.Condition{}
	for _, kind := range []string{PoolConditionReady, PoolConditionAccessible, PoolConditionWritable, PoolConditionCapacityReadable, PoolConditionCapacityIndependent, PoolConditionCapacityBackingVerified} {
		conditions = append(conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "OK"})
	}
	return Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/pool", PoolGroup: DefaultPoolGroup,
		Generation: 2, CapacityPolicy: PoolCapacityPolicyFixedBlock, CapacityLimit: "1Gi", Finalizers: []string{PoolProtectionFinalizer},
		Status: PoolStatus{RegistrationApproved: true, ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now), Conditions: conditions,
			CapacityUnit: &PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}},
			Inventory:    &PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)},
		},
	}
}

func TestExecutionReadinessPreservesEvidenceWhileScanIsPending(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		change  func(*Pool)
		allowed bool
	}{
		{"scan pending", func(*Pool) {}, true},
		{"inventory invalid", func(p *Pool) { p.Status.Inventory.Valid = false }, true},
		{"inventory missing", func(p *Pool) { p.Status.Inventory = nil }, true},
		{"placement limit changed", func(p *Pool) { p.CapacityLimit = "invalid" }, true},
		{"terminating", func(p *Pool) { p.DeletionTimestamp = &metav1.Time{Time: now} }, true},
		{"missing capacity anchor", func(p *Pool) { p.Status.CapacityUnit = nil }, false},
		{"peer capacity rejected", func(p *Pool) { p.Status.Conditions[4].Status = metav1.ConditionFalse }, true},
		{"placement not ready", func(p *Pool) { p.Status.Conditions[0].Status = metav1.ConditionFalse }, true},
		{"capacity backing rejected", func(p *Pool) { p.Status.Conditions[5].Status = metav1.ConditionFalse }, false},
		{"mixed capacity generation", func(p *Pool) { p.Status.Conditions[5].ObservedGeneration = 2 }, false},
		{"failed directory", func(p *Pool) { p.Status.Conditions[1].Status = metav1.ConditionFalse }, false},
		{"unknown directory", func(p *Pool) { p.Status.Conditions[1].Status = metav1.ConditionUnknown }, false},
		{"missing observation", func(p *Pool) { p.Status.ObservedGeneration = 0 }, false},
		{"future generation", func(p *Pool) { p.Status.ObservedGeneration = 3 }, false},
		{"stale observation", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(-time.Hour)) }, false},
		{"future observation", func(p *Pool) { p.Status.LastProbeTime = metav1.NewTime(now.Add(time.Hour)) }, false},
		{"missing mount anchor", func(p *Pool) { p.MountPolicy = PoolMountPolicyRequireMountPoint }, false},
		{"invalid path", func(p *Pool) { p.MountPath = "/"; p.CapacityLimit = "invalid" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := executionPoolFixture(now)
			tc.change(&p)
			if ready, reason := p.ExecutionReadyAt(now, time.Minute); ready != tc.allowed {
				t.Fatalf("execution ready=%v reason=%s want=%v", ready, reason, tc.allowed)
			}
			if ready, _ := poolReady(p, now, time.Minute); ready {
				t.Fatal("old observation admitted a new reservation")
			}
			if ready, _ := p.CleanupReadyAt(now, time.Minute); ready {
				t.Fatal("old observation authorized Helper execution or rollback inventory selection")
			}
		})
	}
}
