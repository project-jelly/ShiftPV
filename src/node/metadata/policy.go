//go:build linux || darwin

package metadata

import (
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type poolCollectionDecision uint8

const (
	waitForPoolEvidence poolCollectionDecision = iota
	rejectPoolConfiguration
	collectEligible
)

// Eligibility is a snapshot decision, not permission to unlink records.
// Collector and ownership recheck API authority and physical absence later.
func decidePoolCollection(pool volumeapi.Pool, now time.Time) poolCollectionDecision {
	if !pool.BackingConfigurationCheck().OK {
		return rejectPoolConfiguration
	}
	ready, _ := pool.ReadyAt(now, volumeapi.DefaultPoolReadinessStaleAfter)
	inv := pool.Status.Inventory
	if pool.DeletionTimestamp != nil || !ready || inv == nil || !inv.Valid || inv.Truncated || inv.Message != "" || inv.ObservedAt.IsZero() || inv.ObservedAt.Time.After(now) || now.Sub(inv.ObservedAt.Time) > volumeapi.DefaultPoolReadinessStaleAfter {
		return waitForPoolEvidence
	}
	return collectEligible
}
