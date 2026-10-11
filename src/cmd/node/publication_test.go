package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	mountutils "k8s.io/mount-utils"

	nodecsi "github.com/project-jelly/ShiftPV/src/csi/node"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type publicationBinder struct{}

func (publicationBinder) Publish(string, string) error                    { return nil }
func (publicationBinder) Unpublish(string, string) error                  { return nil }
func (publicationBinder) HasPublishedTarget(string, string) (bool, error) { return false, nil }

type publicationAllocation []capacityunit.Extent

func (a publicationAllocation) Resolve(string) ([]capacityunit.Extent, error) { return a, nil }

// Exercise the CSI service, ownership locks and real client-go requests
// against one shared Pool. Only host mounts and the HTTP API are fixtures.
func runUnpublishBurst(t testing.TB, config func(*rest.Config) *rest.Config) (time.Duration, int64) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	poolRoot := filepath.Join(root, "pool")
	if err := os.Mkdir(poolRoot, 0700); err != nil {
		t.Fatal(err)
	}
	unit := &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 2048, Sectors: 4096}}}
	pool := map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool-a", "uid": "pool-uid", "generation": int64(1), "finalizers": []string{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "worker-a", "mountPath": "/pool", "capacityPolicy": volumeapi.PoolCapacityPolicyFixedBlock},
		"status":   volumeapi.PoolStatus{CapacityUnit: unit, Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionTrue, ObservedGeneration: 1}}},
	}
	objects := map[string]map[string]any{}
	copies := make([]volume.CopyIdentity, 21)
	for i := range copies {
		id := fmt.Sprintf("shiftpv-%032x", i+1)
		copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", NodeName: "worker-a", VolumeID: id, VolumeUID: id + "-uid", CopyID: id + "-copy", Role: volume.RoleServing}
		copies[i] = copy
		if err := ownership.PrepareServing(context.Background(), poolRoot, copy, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		objects[id] = map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
			"metadata": map[string]any{"name": id, "uid": copy.VolumeUID},
			"spec":     map[string]any{"volumeID": id},
			"status":   map[string]any{"phase": volumeapi.PhaseReady, "ownerNode": copy.NodeName, "currentCopy": copy, "publishedNodes": []string{copy.NodeName}},
		}
	}
	var mu sync.Mutex
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/shiftpvpools/") {
			if r.Method == http.MethodPut {
				if err := json.NewDecoder(r.Body).Decode(&pool); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				metadata := pool["metadata"].(map[string]any)
				metadata["generation"] = metadata["generation"].(float64) + 1
			}
			_ = json.NewEncoder(w).Encode(pool)
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id := parts[len(parts)-1]
		if id == "status" {
			id = parts[len(parts)-2]
		}
		object, found := objects[id]
		if !found {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPut {
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			objects[id] = object
		}
		_ = json.NewEncoder(w).Encode(object)
	}))
	defer server.Close()
	configs := newNodeAPIConfigs(&rest.Config{Host: server.URL})
	configs.foreground = config(configs.foreground)
	clients := newNodeAPIClients(configs, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	deps := readiness.HostProbeDependencies(root)
	deps.Allocations = publicationAllocation(unit.Extents)
	deps.ListMounts = func() ([]mountutils.MountInfo, error) {
		return []mountutils.MountInfo{{MountPoint: root, Major: 8, Minor: 1, Source: unit.Source, FsType: unit.Filesystem, Root: "/"}}, nil
	}
	probe, err := readiness.NewProbeWithDependencies(root, deps)
	if err != nil {
		t.Fatal(err)
	}
	s := &nodecsi.Service{NodeName: "worker-a", HostRoot: root, TargetRoot: "/var/lib/kubelet/pods", Pools: clients.foreground, Volumes: clients.foreground, Binder: publicationBinder{}, PoolVerifier: probe}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The real cleanup fence writer advances generation. Deliberately do not
	// run the readiness observer: unpublish must make progress without it.
	if _, err := clients.foreground.RequestPoolScan(ctx, copies[0]); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	start := time.Now()
	errors := make(chan error, 20)
	for _, copy := range copies[:20] {
		go func() {
			_, err := s.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: copy.VolumeID, TargetPath: "/var/lib/kubelet/pods/" + copy.VolumeID + "/volumes/csi/mount"})
			errors <- err
		}()
	}
	for range 20 {
		if err := <-errors; err != nil {
			t.Errorf("unpublish: %v", err)
		}
	}
	elapsed, calls := time.Since(start), requests.Load()-before
	mu.Lock()
	defer mu.Unlock()
	for i, copy := range copies {
		data, err := json.Marshal(objects[copy.VolumeID]["status"])
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			PublishedNodes []string `json:"publishedNodes"`
		}
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		want := 0
		if i == 20 {
			want = 1
		}
		if len(state.PublishedNodes) != want {
			t.Errorf("volume %d publications=%v", i, state.PublishedNodes)
		}
	}
	return elapsed, calls
}

func TestNodeUnpublishBurstPreservesUnrelatedPublication(t *testing.T) {
	elapsed, calls := runUnpublishBurst(t, func(c *rest.Config) *rest.Config { return c })
	if calls > 120 {
		t.Errorf("20 unpublishes used %d API calls, budgeted at most 120", calls)
	}
	t.Logf("20 unpublishes: %s, %d API calls", elapsed, calls)
}

func BenchmarkNodeUnpublishBurst(b *testing.B) {
	for _, name := range []string{"former-5-10", "current"} {
		b.Run(name, func(b *testing.B) {
			for range b.N {
				elapsed, calls := runUnpublishBurst(b, func(c *rest.Config) *rest.Config {
					if name == "former-5-10" {
						c.QPS, c.Burst = 5, 10
						c.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(5, 10)
					}
					return c
				})
				b.ReportMetric(elapsed.Seconds(), "burst-seconds")
				b.ReportMetric(float64(calls), "API-calls/burst")
			}
		})
	}
}
