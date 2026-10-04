package metrics

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

type blockedInspector struct {
	entered, release chan struct{}
	calls            atomic.Int32
}

func (i *blockedInspector) Inspect(volumeapi.Pool) readiness.Result {
	if i.calls.Add(1) == 1 {
		close(i.entered)
	}
	<-i.release
	return readiness.Result{CapacityReadable: readiness.Check{Known: true, OK: true}}
}

type localPool struct{}

func (localPool) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return []volumeapi.Pool{{Name: "pool", UID: "pool-uid", NodeName: "node"}}, nil
}
func (localPool) SetPoolStatus(context.Context, string, string, string, volumeapi.PoolStatus) error {
	return nil
}

func TestBlockedProbeDoesNotBlockScrapeOrSpawnProbes(t *testing.T) {
	e := New("filesystem")
	inspector := &blockedInspector{entered: make(chan struct{}), release: make(chan struct{})}
	r := &readiness.Reconciler{NodeName: "node", Pools: localPool{}, Inspector: inspector, Interval: time.Millisecond, ObserveAll: e.ObservePools}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		close(inspector.release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("probe did not terminate")
		}
	}()
	<-inspector.entered
	for n := 0; n < 100; n++ {
		_ = output(t, e)
	}
	if inspector.calls.Load() != 1 {
		t.Fatal("scrapes or timer started additional blocked probes")
	}
	contains(t, output(t, e), "shiftpv_metrics_snapshot_success{source=\"filesystem\"} 0", "shiftpv_metrics_snapshot_last_success_timestamp_seconds{source=\"filesystem\"} 0")
}
