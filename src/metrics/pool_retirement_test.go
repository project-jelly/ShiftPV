package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

func TestRetiredPoolMetricsKeepHealthyPeerFilesystemObservation(t *testing.T) {
	e := New("filesystem")
	retired := volumeapi.Pool{Name: "retired", UID: "retired-uid", NodeName: "node", RetirementApproval: "retired-uid"}
	result := readiness.Result{CapacityReadable: readiness.Check{Known: true, OK: true}}
	result.Filesystem.AvailableBytes = 42
	e.ObservePools([]readiness.Observation{
		{Pool: retired, Err: errors.New("old failed storage observation")},
		{Pool: volumeapi.Pool{Name: "healthy", NodeName: "node"}, Result: result},
	}, nil)
	body := output(t, e)
	contains(t, body, `shiftpv_metrics_snapshot_success{source="filesystem"} 1`, `shiftpv_pool_filesystem_available_bytes{node="node",pool="healthy"} 42`)
	if strings.Contains(body, `pool="retired"`) {
		t.Fatal("retirement emitted a fictional filesystem value")
	}
	c, inv := fixture()
	inv.pools[0].RetirementApproval = inv.pools[0].UID
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), `shiftpv_pool_registration_retired{node="a",pool="pool-a"} 1`, `shiftpv_pool_ready{node="a",pool="pool-a"} 0`)
}
