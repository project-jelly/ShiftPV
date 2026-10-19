package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInvalidInventoryDoesNotInventUnsafeCopies(t *testing.T) {
	for name, inventory := range map[string]*volumeapi.PoolInventory{
		"missing":         nil,
		"invalid empty":   {Valid: false},
		"truncated empty": {Valid: true, Truncated: true},
	} {
		t.Run(name, func(t *testing.T) {
			c, inv := fixture()
			inv.pools = inv.pools[:1]
			inv.pools[0].Status.Inventory = inventory
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			body := output(t, c.Exporter)
			contains(t, body, `shiftpv_copy_observations{pool="pool-a",state="NeedsReview"} 0`)
			if inventory != nil && inventory.Truncated {
				contains(t, body, `shiftpv_pool_inventory_truncated{node="a",pool="pool-a"} 1`)
			} else {
				contains(t, body, `shiftpv_pool_inventory_valid{node="a",pool="pool-a"} 0`)
			}
		})
	}
}

func TestIntentionalCopyAbsenceKeepsLifecycleClassification(t *testing.T) {
	for name, phase := range map[string]string{"initial creation": volumeapi.PhaseNodeCreating, "deletion": volumeapi.PhaseDeleting, "active move": volumeapi.PhaseMoving} {
		t.Run(name, func(t *testing.T) {
			c, inv := fixture()
			inv.pools = inv.pools[:1]
			state := inv.volumes["v"]
			state.Phase = phase
			want := "InFlight"
			if phase != volumeapi.PhaseMoving {
				state.ActiveMove, inv.moves = "", nil
			}
			if phase == volumeapi.PhaseDeleting {
				want = "CleanupTarget"
			}
			inv.volumes["v"] = state
			inv.pools[0].Status.Inventory = &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: state.CurrentCopy, Present: false}}}
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			contains(t, output(t, c.Exporter),
				`shiftpv_copy_observations{pool="pool-a",state="Missing"} 0`,
				fmt.Sprintf(`shiftpv_copy_observations{pool="pool-a",state="%s"} 1`, want))
		})
	}
}

func TestExpectedCurrentCopyMissingRequiresCompleteFreshScan(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name      string
		inventory *volumeapi.PoolInventory
		outdated  bool
		want      int
	}{
		{"complete empty", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}, false, 1},
		{"no scan", nil, false, 0},
		{"invalid", &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now)}, false, 0},
		{"truncated", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now), Truncated: true}, false, 0},
		{"scan error", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now), Message: "incomplete"}, false, 0},
		{"stale", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now.Add(-time.Hour))}, false, 0},
		{"future", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now.Add(time.Hour))}, false, 0},
		{"unclocked", &volumeapi.PoolInventory{Valid: true}, false, 0},
		{"outdated generation", &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, inv := fixture()
			inv.moves = nil
			inv.pools = inv.pools[:1]
			state := inv.volumes["v"]
			state.Phase, state.ActiveMove = volumeapi.PhaseReady, ""
			inv.volumes["v"] = state
			inv.pools[0].Generation, inv.pools[0].Status.ObservedGeneration = 1, 1
			if test.outdated {
				inv.pools[0].Generation++
			}
			inv.pools[0].Status.Inventory = test.inventory
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			contains(t, output(t, c.Exporter), fmt.Sprintf(`shiftpv_copy_observations{pool="pool-a",state="Missing"} %d`, test.want))
		})
	}
}

func TestExpectedCopyAbsenceDoesNotInventOrDuplicateLoss(t *testing.T) {
	for _, scenario := range []string{"older scan", "scan after creation", "invalid receipt time", "replaced Pool", "already observed missing", "present", "unsafe observation"} {
		t.Run(scenario, func(t *testing.T) {
			c, inv := fixture()
			inv.moves = nil
			inv.pools = inv.pools[:1]
			state := inv.volumes["v"]
			state.Phase, state.ActiveMove = volumeapi.PhaseReady, ""
			now := time.Now()
			inventory := &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}
			inv.pools[0].Generation, inv.pools[0].Status.ObservedGeneration = 1, 1
			want := 0
			switch scenario {
			case "older scan":
				inventory.ObservedAt = metav1.NewTime(now.Add(-time.Minute))
				state.CreationReceipt = &volumeapi.CreationReceipt{ObservedAt: now.Add(-time.Second).Format(time.RFC3339Nano)}
			case "scan after creation":
				state.CreationReceipt = &volumeapi.CreationReceipt{ObservedAt: now.Add(-time.Second).Format(time.RFC3339Nano)}
				want = 1
			case "invalid receipt time":
				state.CreationReceipt = &volumeapi.CreationReceipt{ObservedAt: "unknown"}
			case "replaced Pool":
				inv.pools[0].UID = "replacement-pool-uid"
			case "already observed missing":
				inventory.Copies = []volumeapi.CopyObservation{{Identity: state.CurrentCopy, Present: false}}
				want = 1
			case "present":
				inventory.Copies = []volumeapi.CopyObservation{{Identity: state.CurrentCopy, Present: true}}
			case "unsafe observation":
				inventory.Copies = []volumeapi.CopyObservation{{Identity: state.CurrentCopy, Problem: "IdentityMismatch"}}
			}
			inv.volumes["v"] = state
			inv.pools[0].Status.Inventory = inventory
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			contains(t, output(t, c.Exporter), fmt.Sprintf(`shiftpv_copy_observations{pool="pool-a",state="Missing"} %d`, want))
		})
	}
}

func TestIncompleteInventoryStillReportsActualUnsafeCopies(t *testing.T) {
	c, inv := fixture()
	inv.pools = inv.pools[:1]
	orphan := *inv.volumes["v"].CurrentCopy
	orphan.CopyID = "unclaimed"
	inv.pools[0].Status.Inventory = &volumeapi.PoolInventory{Truncated: true, Copies: []volumeapi.CopyObservation{
		{Marker: "unsafe", Present: true, Problem: "UnrecordedPath"},
		{Marker: "unclaimed", Identity: &orphan, Present: true},
	}}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter),
		`shiftpv_copy_observations{pool="pool-a",state="NeedsReview"} 1`,
		`shiftpv_copy_observations{pool="pool-a",state="OrphanPreserved"} 1`,
		`shiftpv_pool_inventory_truncated{node="a",pool="pool-a"} 1`)
}
