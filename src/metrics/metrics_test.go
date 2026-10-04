package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type inventory struct {
	pools   []volumeapi.Pool
	volumes map[string]volumeapi.State
	moves   []volumeapi.Move
	fail    string
	calls   int
}

type cleanupInventory struct {
	items []cleanupapi.Cleanup
	err   error
}

func (i cleanupInventory) List(context.Context) ([]cleanupapi.Cleanup, error) { return i.items, i.err }

func (i *inventory) ListPools(context.Context) ([]volumeapi.Pool, error) {
	i.calls++
	if i.fail == "pools" {
		return nil, errors.New("pools")
	}
	return i.pools, nil
}
func (i *inventory) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	i.calls++
	if i.fail == "volumes" {
		return nil, errors.New("volumes")
	}
	return i.volumes, nil
}
func (i *inventory) ListMoves(context.Context) ([]volumeapi.Move, error) {
	i.calls++
	if i.fail == "moves" {
		return nil, errors.New("moves")
	}
	return i.moves, nil
}

func fixture() (*Controller, *inventory) {
	inv := &inventory{pools: []volumeapi.Pool{{Name: "pool-a", NodeName: "a", CapacityLimit: "1Gi"}, {Name: "pool-b", NodeName: "b", CapacityLimit: "1Gi"}},
		volumes: map[string]volumeapi.State{"v": {UID: "volume-uid", CapacityBytes: 64, OwnerNode: "a", Phase: "Moving", ActiveMove: "move"}},
		moves:   []volumeapi.Move{{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "v", SourceNode: "a"}, Status: volumeapi.MoveStatus{Phase: "Copying", DestinationNode: "b", CapacityApproved: true}}},
	}
	return &Controller{Exporter: New("metadata"), Inventory: inv, Interval: time.Millisecond}, inv
}

func output(t testing.TB, e *Exporter) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	e.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("metrics HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}
func contains(t testing.TB, body string, expected ...string) {
	t.Helper()
	for _, s := range expected {
		if !strings.Contains(body, s) {
			t.Fatalf("missing %q in:\n%s", s, body)
		}
	}
}

func TestControllerAccountingAndMoveLifecycle(t *testing.T) {
	c, inv := fixture()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := output(t, c.Exporter)
	contains(t, body,
		"shiftpv_pool_reserved_bytes{node=\"a\",pool=\"pool-a\"} 64",
		"shiftpv_pool_reserved_bytes{node=\"b\",pool=\"pool-b\"} 64",
		"shiftpv_moves{phase=\"Copying\"} 1",
		"shiftpv_volumes{phase=\"Moving\"} 1")
	inv.volumes["v"] = volumeapi.State{UID: "volume-uid", CapacityBytes: 64, OwnerNode: "b", Phase: "Ready", ActiveMove: "move"}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_pool_reserved_bytes{node=\"b\",pool=\"pool-b\"} 64")
	inv.volumes["v"] = volumeapi.State{UID: "volume-uid", CapacityBytes: 64, OwnerNode: "b", Phase: "Ready"}
	inv.moves[0].Status.Phase = "Blocked"
	inv.moves[0].Status.RecoveryPhase = "Recovered"
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_moves{phase=\"Blocked\"} 0")
	inv.pools = nil
	inv.volumes = nil
	inv.moves = nil
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	body = output(t, c.Exporter)
	if strings.Contains(body, "pool_reserved_bytes{") {
		t.Fatal("deleted Pool series retained")
	}
	contains(t, body, "shiftpv_volumes{phase=\"Ready\"} 0")
}

func TestMetadataFailuresPreserveLastSuccess(t *testing.T) {
	for _, failure := range []string{"pools", "volumes", "moves", "active-link"} {
		t.Run(failure, func(t *testing.T) {
			c, inv := fixture()
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			previous := c.Exporter.Cache.groups["metadata"].lastSuccess
			inv.fail = failure
			if failure == "active-link" {
				inv.moves = nil
			}
			if c.Refresh(context.Background()) == nil {
				t.Fatal("expected snapshot error")
			}
			contains(t, output(t, c.Exporter), "shiftpv_metrics_snapshot_success{source=\"metadata\"} 0", "shiftpv_moves{phase=\"Copying\"} 1")
			if !previous.Equal(c.Exporter.Cache.groups["metadata"].lastSuccess) {
				t.Fatal("failure refreshed timestamp")
			}
		})
	}
}

func TestCompletingMoveHasBoundedMetricPhase(t *testing.T) {
	c, inv := fixture()
	inv.moves[0].Status.Phase = "Completing"
	inv.volumes["v"] = volumeapi.State{UID: "volume-uid", CapacityBytes: 64, OwnerNode: "b", Phase: "Ready", ActiveMove: "move"}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_moves{phase=\"Completing\"} 1", "shiftpv_moves{phase=\"Unknown\"} 0")
	inv.volumes["v"] = volumeapi.State{UID: "volume-uid", CapacityBytes: 64, OwnerNode: "b", Phase: "Ready"}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_moves{phase=\"Completing\"} 1")
	delete(inv.volumes, "v")
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_moves{phase=\"Completing\"} 1")
	inv.moves[0].Status.Phase = "Succeeded"
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_moves{phase=\"Completing\"} 0")
}

func TestInvalidAccountingKeepsNumbersAndUnknownInitial(t *testing.T) {
	c, inv := fixture()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	inv.volumes["missing"] = volumeapi.State{OwnerNode: "a"}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_pool_accounting_valid{node=\"a\",pool=\"pool-a\"} 0", "shiftpv_pool_reserved_bytes{node=\"a\",pool=\"pool-a\"} 64")
	fresh := New("metadata")
	c.Exporter = fresh
	inv.pools[1].CapacityLimit = "invalid"
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := output(t, fresh)
	if strings.Contains(body, "pool_reserved_bytes{") {
		t.Fatal("unknown capacity reported")
	}
	contains(t, body, "shiftpv_pool_accounting_valid{node=\"b\",pool=\"pool-b\"} 0")
}

func TestPoolFreshness(t *testing.T) {
	c, inv := fixture()
	p := &inv.pools[0]
	p.Generation = 2
	p.Status = volumeapi.PoolStatus{ObservedGeneration: 2, LastProbeTime: metav1.Now(), Conditions: []metav1.Condition{{Type: "Ready", Status: "True", ObservedGeneration: 2}}}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_pool_ready{node=\"a\",pool=\"pool-a\"} 1")
	p.Status.LastProbeTime = metav1.NewTime(time.Now().Add(-time.Hour))
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), "shiftpv_pool_ready{node=\"a\",pool=\"pool-a\"} 0")
}

func TestNodeSnapshotAndDiscovery(t *testing.T) {
	e := New("filesystem", "discovery")
	body := output(t, e)
	if strings.Contains(body, "pool_filesystem_available_bytes{") {
		t.Fatal("unobserved filesystem reported")
	}
	pool := volumeapi.Pool{Name: "pool", NodeName: "node"}
	result := readiness.Result{CapacityReadable: readiness.Check{OK: true}, Filesystem: capacity.Filesystem{TotalBytes: 100, AvailableBytes: 50, AvailableInodes: 10}}
	e.ObservePool(pool, result, nil)
	stamp := e.Cache.groups["filesystem"].lastSuccess
	e.ObservePool(pool, readiness.Result{}, nil)
	contains(t, output(t, e), "shiftpv_pool_filesystem_available_bytes{node=\"node\",pool=\"pool\"} 50", "shiftpv_metrics_snapshot_success{source=\"filesystem\"} 0")
	e.ObservePool(pool, result, errors.New("API error"))
	if !stamp.Equal(e.Cache.groups["filesystem"].lastSuccess) {
		t.Fatal("failed probe advanced timestamp")
	}
	result.Filesystem.AvailableBytes = 0
	e.ObservePool(pool, result, nil)
	contains(t, output(t, e), "shiftpv_pool_filesystem_available_bytes{node=\"node\",pool=\"pool\"} 0")
	e.ObserveDiscovery(map[string]int{"DisruptionBudgetDenied": 2, "unbounded-uid": 3}, nil)
	e.ObserveDiscovery(nil, errors.New("API"))
	body = output(t, e)
	contains(t, body, "shiftpv_mobility_deferred_volumes{reason=\"DisruptionBudgetDenied\"} 2", "shiftpv_mobility_deferred_volumes{reason=\"Unknown\"} 3", "shiftpv_metrics_snapshot_success{source=\"discovery\"} 0")
	if strings.Contains(body, "unbounded-uid") {
		t.Fatal("unbounded reason")
	}
	e.ObserveDiscovery(nil, nil)
	contains(t, output(t, e), "shiftpv_mobility_deferred_volumes{reason=\"Unknown\"} 0")
	e.ObservePool(volumeapi.Pool{}, readiness.Result{}, nil)
	if strings.Contains(output(t, e), "pool_filesystem_available_bytes{") {
		t.Fatal("removed Pool retained")
	}
	contains(t, output(t, e), "shiftpv_metrics_snapshot_success{source=\"filesystem\"} 0")
	empty := New("filesystem")
	empty.ObservePool(volumeapi.Pool{}, readiness.Result{}, nil)
	contains(t, output(t, empty), "shiftpv_metrics_snapshot_last_success_timestamp_seconds{source=\"filesystem\"} 0")
}

func TestNodeFilesystemSnapshotIncludesEveryPool(t *testing.T) {
	e := New("filesystem")
	observations := []readiness.Observation{
		{Pool: volumeapi.Pool{Name: "pool-a", NodeName: "node"}, Result: readiness.Result{
			CapacityReadable: readiness.Check{OK: true}, Filesystem: capacity.Filesystem{AvailableBytes: 10},
		}},
		{Pool: volumeapi.Pool{Name: "pool-b", NodeName: "node"}, Result: readiness.Result{
			CapacityReadable: readiness.Check{OK: true}, Filesystem: capacity.Filesystem{AvailableBytes: 20},
		}},
	}
	e.ObservePools(observations, nil)
	contains(t, output(t, e),
		"shiftpv_pool_filesystem_available_bytes{node=\"node\",pool=\"pool-a\"} 10",
		"shiftpv_pool_filesystem_available_bytes{node=\"node\",pool=\"pool-b\"} 20")
	e.ObservePools(observations[:1], nil)
	if strings.Contains(output(t, e), "pool=\"pool-b\"") {
		t.Fatal("removed Pool retained in filesystem snapshot")
	}
}

func TestCSIInterceptor(t *testing.T) {
	e := New()
	wantErr := status.Error(codes.ResourceExhausted, "private-volume-name")
	for _, method := range []string{"/csi.v1.Controller/CreateVolume", "/csi.v1.Controller/DeleteVolume", "/csi.v1.Node/NodePublishVolume", "/csi.v1.Node/NodeUnpublishVolume", "/csi.v1.Identity/Probe"} {
		response, err := e.intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return "unchanged", wantErr })
		if response != "unchanged" || err != wantErr {
			t.Fatal("RPC semantics changed")
		}
	}
	body := output(t, e)
	contains(t, body, "shiftpv_csi_requests_total{code=\"ResourceExhausted\",method=\"CreateVolume\"} 1", "shiftpv_csi_request_duration_seconds_count{method=\"NodePublishVolume\"} 1")
	if strings.Contains(body, "private-volume-name") || strings.Contains(body, "method=\"Probe\"") {
		t.Fatal("unbounded/unselected RPC")
	}
	if (*Exporter)(nil).ServerOptions() != nil || len(e.ServerOptions()) != 1 {
		t.Fatal("disabled wiring")
	}
	problems, err := testutil.GatherAndLint(e.Registry)
	if err != nil || len(problems) != 0 {
		t.Fatalf("metric lint: %v %v", problems, err)
	}
}

func TestConcurrentCachedScrapesHaveNoExternalIO(t *testing.T) {
	c, inv := fixture()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := inv.calls
	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			families, err := c.Exporter.Registry.Gather()
			if err != nil || len(families) == 0 {
				t.Errorf("gather: %v", err)
			}
		}()
	}
	for n := 0; n < 100; n++ {
		c.Exporter.ObserveDiscovery(map[string]int{"DisruptionBudgetDenied": n}, nil)
	}
	wg.Wait()
	if inv.calls != calls {
		t.Fatal("scrape performed API I/O")
	}
}

func TestCardinalityBoundedByPoolsAndEnums(t *testing.T) {
	c, inv := fixture()
	inv.volumes = make(map[string]volumeapi.State)
	inv.moves = nil
	for n := 0; n < 1000; n++ {
		inv.volumes[fmt.Sprintf("unique-volume-%d", n)] = volumeapi.State{CapacityBytes: 1, OwnerNode: "a", Phase: fmt.Sprintf("unique-phase-%d", n)}
	}
	for n := 0; n < 10000; n++ {
		inv.moves = append(inv.moves, volumeapi.Move{Name: fmt.Sprintf("historic-%d", n), Status: volumeapi.MoveStatus{Phase: "Blocked"}})
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := output(t, c.Exporter)
	if strings.Contains(body, "unique-") || strings.Contains(body, "historic-") {
		t.Fatal("identity leaked into labels")
	}
	contains(t, body, "shiftpv_volumes{phase=\"Unknown\"} 1000", "shiftpv_moves{phase=\"Blocked\"} 0")
	if strings.Count(body, "\n") > 100 {
		t.Fatal("cardinality grew with Volumes/Moves")
	}
}

func TestEndpointLifecycleAndBindFailure(t *testing.T) {
	e := New("metadata")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.serve(ctx, listener) }()
	response, err := http.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	if e.Serve(ctx, listener.Addr().String()) == nil {
		t.Fatal("occupied port accepted")
	}
	e.Start(ctx, listener.Addr().String())
	_, rpcErr := e.intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/CreateVolume"}, func(context.Context, any) (any, error) { return nil, nil })
	if rpcErr != nil {
		t.Fatal("bind failure affected CSI")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server shutdown timed out")
	}
}

func TestControllerLoopStopsAndRejectsInvalidInterval(t *testing.T) {
	c, _ := fixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Run(ctx)
	c.Interval = 0
	c.Run(context.Background())
}

func TestCachedScrapeLatency(t *testing.T) {
	c, inv := fixture()
	for n := 0; n < 99; n++ {
		id := fmt.Sprintf("v-%d", n)
		inv.volumes[id] = volumeapi.State{CapacityBytes: 64, OwnerNode: "a", Phase: "Ready"}
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	durations := make([]time.Duration, 100)
	for n := range durations {
		started := time.Now()
		_ = output(t, c.Exporter)
		durations[n] = time.Since(started)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("2 Pools / 100 Volumes cached scrape p99=%s", durations[98])
	if durations[98] > 100*time.Millisecond {
		t.Fatalf("cached scrape exceeded 100ms: %s", durations[98])
	}
}

func BenchmarkCachedScrape(b *testing.B) {
	c, _ := fixture()
	if err := c.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	handler := c.Exporter.Handler()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}
}

func TestDedicatedMetricFamilyContract(t *testing.T) {
	c, _ := fixture()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	e := c.Exporter
	e.ObservePool(volumeapi.Pool{Name: "pool", NodeName: "node"}, readiness.Result{CapacityReadable: readiness.Check{OK: true}}, nil)
	e.ObserveDiscovery(nil, nil)
	_, _ = e.intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/CreateVolume"}, func(context.Context, any) (any, error) { return nil, status.Error(codes.Code(1000), "unknown code") })
	families, err := e.Registry.Gather()
	if err != nil || len(families) != 20 {
		t.Fatalf("families=%d err=%v", len(families), err)
	}
	for _, family := range families {
		name := family.GetName()
		if !strings.HasPrefix(name, "shiftpv_") {
			t.Fatalf("unexpected family %s", name)
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				switch label.GetName() {
				case "pool", "node", "source", "phase", "state", "reason", "method", "code":
				default:
					t.Fatalf("unexpected label %s", label.GetName())
				}
			}
		}
	}
	contains(t, output(t, e), "shiftpv_csi_requests_total{code=\"Unknown\",method=\"CreateVolume\"} 1")
	problems, err := testutil.GatherAndLint(e.Registry)
	if err != nil || len(problems) != 0 {
		t.Fatalf("metric contract lint: %v %v", problems, err)
	}
}

func TestCleanupContractMetricsExposeVerifyingAndFailClosed(t *testing.T) {
	c, _ := fixture()
	c.Cleanups = cleanupInventory{items: []cleanupapi.Cleanup{
		{},
		{Status: cleanupapi.Status{Phase: cleanupapi.PhaseVerifying}},
		{Status: cleanupapi.Status{Phase: cleanupapi.PhaseConfirmingAbsence}},
		{Status: cleanupapi.Status{Phase: cleanupapi.PhaseNeedsReview}},
		{Status: cleanupapi.Status{Phase: "Invalid"}},
	}}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter),
		`shiftpv_cleanup_requests{state="Pending"} 1`,
		`shiftpv_cleanup_requests{state="Verifying"} 1`,
		`shiftpv_cleanup_requests{state="ConfirmingAbsence"} 1`,
		`shiftpv_cleanup_requests{state="NeedsReview"} 1`,
		`shiftpv_cleanup_requests{state="Unknown"} 1`,
	)
	c.Cleanups = cleanupInventory{err: errors.New("cleanup inventory unavailable")}
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("cleanup inventory failure was hidden")
	}
	contains(t, output(t, c.Exporter), `shiftpv_metrics_snapshot_success{source="metadata"} 0`)
}

func TestCopyObservationsClassifyAuthorityWithoutDeletingOrphans(t *testing.T) {
	c, inv := fixture()
	current := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "current", NodeName: "a", Role: volume.RoleServing}
	orphan := current
	orphan.CopyID = "orphan"
	inv.volumes = map[string]volumeapi.State{current.VolumeID: {UID: current.VolumeUID, CapacityBytes: 64, Phase: volumeapi.PhaseReady, OwnerNode: "a", CurrentCopy: &current}}
	inv.moves = nil
	inv.pools = []volumeapi.Pool{{
		Name: "pool-a", NodeName: "a", CapacityLimit: "1Gi",
		Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{
			{Marker: "current", Identity: &current, Present: true},
			{Marker: "orphan", Identity: &orphan, Present: true},
			{Marker: "unknown", Present: true, Problem: "UnrecordedPath"},
		}}},
	}}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	contains(t, output(t, c.Exporter), `shiftpv_copy_observations{pool="pool-a",state="Current"} 1`, `shiftpv_copy_observations{pool="pool-a",state="OrphanPreserved"} 1`, `shiftpv_copy_observations{pool="pool-a",state="NeedsReview"} 1`)
}

type persistentVolumeInventory struct {
	items []corev1.PersistentVolume
	err   error
}

func (i persistentVolumeInventory) List(context.Context) ([]corev1.PersistentVolume, error) {
	return i.items, i.err
}

func driverPersistentVolume(name, handle, phase, capacity string) corev1.PersistentVolume {
	return corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:               corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: volume.DriverName, VolumeHandle: handle}},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.PersistentVolumePhase(phase)},
	}
}

func TestPersistentVolumePhasesAndPoolAttribution(t *testing.T) {
	c, inv := fixture()
	bound := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "current", NodeName: "a", Role: volume.RoleServing}
	released := bound
	released.VolumeID = "shiftpv-fedcba9876543210fedcba9876543210"
	released.PoolName = "pool-b"
	inv.volumes = map[string]volumeapi.State{
		bound.VolumeID:    {UID: bound.VolumeUID, CapacityBytes: 64, Phase: volumeapi.PhaseReady, OwnerNode: "a", CurrentCopy: &bound},
		released.VolumeID: {UID: "released-uid", CapacityBytes: 64, Phase: volumeapi.PhaseReady, OwnerNode: "b", CurrentCopy: &released},
	}
	inv.moves = nil
	foreign := driverPersistentVolume("foreign", "other-handle", "Released", "5Gi")
	foreign.Spec.CSI.Driver = "csi.example.com"
	c.PersistentVolumes = persistentVolumeInventory{items: []corev1.PersistentVolume{
		driverPersistentVolume("bound", bound.VolumeID, "Bound", "1Gi"),
		driverPersistentVolume("released", released.VolumeID, "Released", "25Gi"),
		driverPersistentVolume("detached", "shiftpv-00000000000000000000000000000000", "Released", "10Gi"),
		foreign,
	}}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := output(t, c.Exporter)
	contains(t, body,
		`shiftpv_persistent_volumes{phase="Bound",pool="pool-a"} 1`,
		`shiftpv_persistent_volumes{phase="Released",pool="pool-a"} 0`,
		`shiftpv_persistent_volumes{phase="Released",pool="pool-b"} 1`,
		`shiftpv_persistent_volumes{phase="Released",pool="unknown"} 1`,
		`shiftpv_persistent_volumes_released_bytes{pool="pool-a"} 0`,
		`shiftpv_persistent_volumes_released_bytes{pool="pool-b"} 2.68435456e+10`,
		`shiftpv_persistent_volumes_released_bytes{pool="unknown"} 1.073741824e+10`)
	if strings.Contains(body, `driver="csi.example.com"`) || strings.Contains(body, "foreign") {
		t.Fatalf("foreign driver PersistentVolume counted:\n%s", body)
	}
	c.PersistentVolumes = persistentVolumeInventory{err: errors.New("persistent volume inventory unavailable")}
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("PersistentVolume inventory failure was hidden")
	}
	contains(t, output(t, c.Exporter), `shiftpv_metrics_snapshot_success{source="metadata"} 0`)
}

// An absent series and a Released count of zero read the same way in
// ShiftPVReleasedVolumes, so the family must be emitted with or without an
// inventory to observe.
func TestPersistentVolumeFamilyIsAlwaysEmitted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		inventory PersistentVolumeInventory
	}{
		{"empty", persistentVolumeInventory{}},
		{"unset", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := fixture()
			c.PersistentVolumes = tc.inventory
			if err := c.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			contains(t, output(t, c.Exporter),
				`shiftpv_persistent_volumes{phase="Bound",pool="unknown"} 0`,
				`shiftpv_persistent_volumes{phase="Released",pool="unknown"} 0`,
				`shiftpv_persistent_volumes_released_bytes{pool="unknown"} 0`)
		})
	}
}
