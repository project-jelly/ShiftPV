package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	remotemeasurement "github.com/project-jelly/ShiftPV/src/pool/measurement/remote"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type capacityProbeDiscovery struct{}

func (capacityProbeDiscovery) Find(context.Context, string) (*volumeapi.NodeExecutor, error) {
	return &volumeapi.NodeExecutor{PodUID: "executor-uid", NodeName: "worker-a"}, nil
}

func (capacityProbeDiscovery) Endpoint(context.Context, volumeapi.NodeExecutor) (connection.Target, bool, error) {
	return connection.Target{}, true, nil
}

type capacityProbeRPC struct {
	after func()
	calls int
}

func (r *capacityProbeRPC) GetCapacity(_ context.Context, _ connection.Target, request *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	r.calls++
	r.after()
	return &protocol.CapacityResponse{ExecutorUid: request.ExecutorUid, Evidence: request.Evidence, Nonce: request.Nonce,
		TotalBytes: 100, AvailableBytes: 40, AvailableInodes: 10}, nil
}

type forbiddenCapacityFallback struct{ calls int }

func (f *forbiddenCapacityFallback) StatFSForPool(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error) {
	f.calls++
	return poolcapacity.Filesystem{}, errors.New("unexpected fallback")
}

func TestCapacityProbeBudgetRetainsPreAndPostRPCAPIValidation(t *testing.T) {
	for _, mode := range []string{"fresh", "registration changes", "post-read forbidden"} {
		t.Run(mode, func(t *testing.T) {
			var reads atomic.Int32
			var afterRPC atomic.Bool
			probeTime := time.Now().UTC().Format(time.RFC3339Nano)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if r.Method != http.MethodGet || r.UserAgent() != "shiftpv-capacity-probe" {
					http.Error(w, "unexpected probe request", http.StatusBadRequest)
					return
				}
				if afterRPC.Load() && mode == "post-read forbidden" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","code":403}`))
					return
				}
				generation := 1
				if afterRPC.Load() && mode == "registration changes" {
					generation = 2
				}
				object := map[string]any{
					"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
					"metadata": map[string]any{"name": "pool-a", "uid": "pool-uid", "generation": generation, "finalizers": []string{volumeapi.PoolProtectionFinalizer}},
					"spec":     map[string]any{"nodeName": "worker-a", "mountPath": "/mnt/a", "capacity": map[string]string{"limit": "1Gi"}},
					"status": map[string]any{"observedGeneration": generation, "lastProbeTime": probeTime,
						"inventory":  map[string]any{"valid": true, "observedAt": probeTime},
						"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "PoolReady", "observedGeneration": generation, "lastTransitionTime": probeTime}}},
				}
				if strings.HasSuffix(r.URL.Path, "/shiftpvpools") {
					object = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPoolList", "items": []any{object}}
				}
				_ = json.NewEncoder(w).Encode(object)
			}))
			defer server.Close()
			budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
			original := &rest.Config{Host: server.URL, QPS: 1, Burst: 1, RateLimiter: budget, Timeout: time.Minute, UserAgent: "observer"}
			config := capacityProbeRESTConfig(original)
			retry := capacityRetryRESTConfig(original)
			if config.QPS != 50 || config.Burst != 100 || config.RateLimiter == nil || config.RateLimiter == budget || config.RateLimiter == retry.RateLimiter || config.RateLimiter.QPS() != 50 || config.UserAgent != "shiftpv-capacity-probe" || config.Timeout != 0 {
				t.Fatal("probe uses an inherited, retry or unbounded API budget")
			}
			clients := wiring.ForConfig(config, "probe", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
			registry := &volumeapi.Registry{Client: clients.Dynamic}
			rpc := &capacityProbeRPC{after: func() { afterRPC.Store(true) }}
			fallback := &forbiddenCapacityFallback{}
			probe := &remotemeasurement.Client{Pools: registry, Discovery: capacityProbeDiscovery{}, RPC: rpc, Fallback: fallback}
			expected := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/a", Generation: 1}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stats, err := probe.StatFSForPool(ctx, expected)
			wantReads := int32(4) // GET + peer LIST before and after the Node RPC.
			if mode == "post-read forbidden" {
				wantReads = 3
			}
			if mode == "fresh" {
				if err != nil || stats.AvailableBytes != 40 {
					t.Fatalf("stats=%+v error=%v", stats, err)
				}
			} else if err == nil || stats != (poolcapacity.Filesystem{}) {
				t.Fatalf("post-RPC validation failure authorized capacity: %+v %v", stats, err)
			}
			if mode == "registration changes" && !strings.Contains(err.Error(), "Pool measurement evidence changed") {
				t.Fatalf("changed registration did not reach the evidence guard: %v", err)
			}
			if mode == "post-read forbidden" && !apierrors.IsForbidden(err) {
				t.Fatalf("API rejection did not reach the post-RPC guard: %v", err)
			}
			if reads.Load() != wantReads || rpc.calls != 1 || fallback.calls != 0 || budget.waits.Load() != 0 {
				t.Fatalf("reads=%d RPC=%d fallback=%d observer waits=%d", reads.Load(), rpc.calls, fallback.calls, budget.waits.Load())
			}
			if original.QPS != 1 || original.Burst != 1 || original.RateLimiter != budget || original.Timeout != time.Minute || original.UserAgent != "observer" || retry.QPS != 5 || retry.Burst != 10 {
				t.Fatal("probe configuration changed the observer or retry budget")
			}
		})
	}
}
