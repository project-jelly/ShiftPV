//go:build linux || darwin

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPoolCollectionEvidenceDecisions(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		change func(*volumeapi.Pool)
		want   poolCollectionDecision
	}{
		{"fresh", func(*volumeapi.Pool) {}, collectEligible},
		{"invalid path", func(p *volumeapi.Pool) { p.MountPath = "relative" }, rejectPoolConfiguration},
		{"deleting", func(p *volumeapi.Pool) { stamp := metav1.NewTime(now); p.DeletionTimestamp = &stamp }, waitForPoolEvidence},
		{"generation changed", func(p *volumeapi.Pool) { p.Generation++ }, waitForPoolEvidence},
		{"not ready", func(p *volumeapi.Pool) { p.Status.Conditions[0].Status = metav1.ConditionFalse }, waitForPoolEvidence},
		{"missing inventory", func(p *volumeapi.Pool) { p.Status.Inventory = nil }, waitForPoolEvidence},
		{"invalid inventory", func(p *volumeapi.Pool) { p.Status.Inventory.Valid = false }, waitForPoolEvidence},
		{"truncated inventory", func(p *volumeapi.Pool) { p.Status.Inventory.Truncated = true }, waitForPoolEvidence},
		{"failed scan", func(p *volumeapi.Pool) { p.Status.Inventory.Message = "scan failed" }, waitForPoolEvidence},
		{"missing timestamp", func(p *volumeapi.Pool) { p.Status.Inventory.ObservedAt = metav1.Time{} }, waitForPoolEvidence},
		{"future scan", func(p *volumeapi.Pool) { p.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(time.Second)) }, waitForPoolEvidence},
		{"stale scan", func(p *volumeapi.Pool) { p.Status.Inventory.ObservedAt = metav1.NewTime(now.Add(-time.Hour)) }, waitForPoolEvidence},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := readyPool(now)
			tc.change(&pool)
			if got := decidePoolCollection(pool, now); got != tc.want {
				t.Fatalf("decision=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestCollectorProgressAfterFreshEvidence(t *testing.T) {
	now := time.Now().UTC()
	unready := readyPool(now)
	unready.Status.Inventory.Valid = false
	healthy := readyPool(now)
	healthy.Name, healthy.UID, healthy.MountPath = "healthy", "healthy-uid", "/healthy"
	repo := &fakeRepository{pools: []volumeapi.Pool{unready, healthy}}
	collected := map[string]int{}
	c := &Collector{NodeName: "worker-a", HostRoot: "/host", Repository: repo, Retention: DefaultRetention, Now: func() time.Time { return now }, VerifyPool: func(context.Context, volumeapi.Pool) error { return nil }}
	c.Collect = func(_ context.Context, _ string, pool ownership.PoolIdentity, _ time.Time, _ time.Duration, _ func(context.Context, volume.CopyIdentity) (bool, error)) (int, error) {
		collected[pool.PoolUID]++
		return 0, nil
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if collected["pool-uid"] != 0 || collected["healthy-uid"] != 1 {
		t.Fatalf("one Pool's missing evidence blocked its healthy peer: %v", collected)
	}
	repo.pools[0].Status.Inventory.Valid = true
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if collected["pool-uid"] != 1 || collected["healthy-uid"] != 2 {
		t.Fatalf("fresh evidence did not resume collection: %v", collected)
	}
}
