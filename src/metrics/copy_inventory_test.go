package metrics

import (
	"context"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
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
