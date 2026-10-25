package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	mobilitycontroller "github.com/project-jelly/ShiftPV/src/mobility/controller"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/measurement"
	"github.com/project-jelly/ShiftPV/src/pool/measurement/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type mobilityProbeRPCFunc func(context.Context, *protocol.CapacityRequest) (*protocol.CapacityResponse, error)

func (f mobilityProbeRPCFunc) GetCapacity(ctx context.Context, _ connection.Target, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	return f(ctx, req)
}

type mobilityProbeDiscovery struct {
	capacityProbeDiscovery
	block bool
}

func (d mobilityProbeDiscovery) Find(ctx context.Context, node string) (*volumeapi.NodeExecutor, error) {
	if d.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return d.capacityProbeDiscovery.Find(ctx, node)
}

func TestMobilityProbeBudgetCoversEntireResidentCall(t *testing.T) {
	for _, stage := range []string{"initial API read", "discovery", "RPC", "final API read", "API answer"} {
		t.Run(stage, func(t *testing.T) {
			api, registry, pool := newMobilityProbeAPI(t)
			var afterRPC atomic.Bool
			api.stall = func(req *http.Request) bool {
				return req.Method == http.MethodGet && (stage == "initial API read" ||
					(stage == "final API read" && afterRPC.Load()))
			}
			rpc := mobilityProbeRPCFunc(func(ctx context.Context, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
				afterRPC.Store(true)
				if stage == "RPC" {
					<-ctx.Done()
					return nil, status.Error(codes.DeadlineExceeded, "RPC budget expired")
				}
				if stage == "API answer" {
					return nil, status.Error(codes.Unavailable, "RPC endpoint unavailable")
				}
				return probeAnswer(req), nil
			})
			probe := newMobilityCapacityProbe(registry, mobilityProbeDiscovery{block: stage == "discovery"}, rpc, nil)
			probe.timeout = 200 * time.Millisecond
			started := time.Now()
			stats, err := probe.StatFSForPool(context.Background(), pool)
			if time.Since(started) > 2*time.Second || stats != (capacity.Filesystem{}) ||
				!errors.Is(err, mobilitycontroller.ErrCapacityProbePending) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("whole-call budget failed: elapsed=%v stats=%+v err=%v", time.Since(started), stats, err)
			}
			if api.unexpected.Load() != 0 {
				t.Fatal("resident capacity probe attempted an unrelated API operation")
			}
			if stage == "API answer" && api.writes.Load() != 1 {
				t.Fatalf("RPC failure did not reach the API nonce request: writes=%d", api.writes.Load())
			}
			if stage == "RPC" && api.writes.Load() != 0 {
				t.Fatal("expired caller context started an API fallback")
			}
		})
	}
}

func TestMobilityProbeUsesFreshAPIFallbackWithoutHelper(t *testing.T) {
	for _, mode := range []string{"API only", "unavailable", "unimplemented", "RPC timeout", "RPC success"} {
		t.Run(mode, func(t *testing.T) {
			api, registry, pool := newMobilityProbeAPI(t)
			api.answer = true
			var rpc remote.RPC
			if mode != "API only" {
				rpc = mobilityProbeRPCFunc(func(_ context.Context, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
					switch mode {
					case "unavailable":
						return nil, status.Error(codes.Unavailable, "endpoint unavailable")
					case "unimplemented":
						return nil, status.Error(codes.Unimplemented, "method unavailable")
					case "RPC timeout":
						return nil, status.Error(codes.DeadlineExceeded, "RPC attempt timed out")
					}
					return probeAnswer(req), nil
				})
			}
			probe := newMobilityCapacityProbe(registry, capacityProbeDiscovery{}, rpc, nil)
			for range 2 {
				stats, err := probe.StatFSForPool(context.Background(), pool)
				if err != nil || stats.AvailableBytes != 40 {
					t.Fatalf("resident probe failed: %+v %v", stats, err)
				}
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			wantWrites := int32(2)
			if mode == "RPC success" {
				wantWrites = 0
			}
			if api.writes.Load() != wantWrites || api.unexpected.Load() != 0 ||
				(wantWrites == 2 && (len(api.nonces) != 2 || api.nonces[0] == api.nonces[1])) {
				t.Fatalf("probe reused a nonce or created a helper: writes=%d nonces=%v unexpected=%d", api.writes.Load(), api.nonces, api.unexpected.Load())
			}
			var apiClient *measurement.Client
			if rpcClient, ok := probe.probe.(*remote.Client); ok {
				apiClient = rpcClient.Fallback.(*measurement.Client)
			} else {
				apiClient = probe.probe.(*measurement.Client)
			}
			if apiClient.Fallback != nil {
				t.Fatal("mobility probe has a helper fallback")
			}
		})
	}
}

type mobilityProbeFunc func(context.Context, volumeapi.Pool) (capacity.Filesystem, error)

func (f mobilityProbeFunc) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
	return f(ctx, pool)
}

func TestMobilityProbeRejectsLateSuccess(t *testing.T) {
	probe := &mobilityCapacityProbe{timeout: 10 * time.Millisecond, probe: mobilityProbeFunc(func(ctx context.Context, _ volumeapi.Pool) (capacity.Filesystem, error) {
		<-ctx.Done()
		return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 40}, nil
	})}
	stats, err := probe.StatFSForPool(context.Background(), volumeapi.Pool{})
	if stats != (capacity.Filesystem{}) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, mobilitycontroller.ErrCapacityProbePending) {
		t.Fatalf("late success authorized capacity: %+v %v", stats, err)
	}
}

func TestMobilityProbePreservesTimeoutAndAuthorityErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		cause   error
		pending bool
	}{
		{"API timeout", apierrors.NewTimeoutError("response delayed", 1), true},
		{"server timeout", apierrors.NewServerTimeout(schema.GroupResource{Resource: "shiftpvpools"}, "get", 1), true},
		{"deadline", context.DeadlineExceeded, true},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Resource: "shiftpvpools"}, "pool", errors.New("denied")), false},
		{"identity", errors.New("Pool measurement evidence changed"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &mobilityCapacityProbe{timeout: time.Second, probe: mobilityProbeFunc(func(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
				return capacity.Filesystem{}, test.cause
			})}
			_, err := probe.StatFSForPool(context.Background(), volumeapi.Pool{})
			if !errors.Is(err, test.cause) || errors.Is(err, mobilitycontroller.ErrCapacityProbePending) != test.pending {
				t.Fatalf("error identity or pending classification changed: %v", err)
			}
		})
	}
}

func probeAnswer(req *protocol.CapacityRequest) *protocol.CapacityResponse {
	return &protocol.CapacityResponse{ExecutorUid: req.ExecutorUid, Nonce: req.Nonce, Evidence: req.Evidence,
		TotalBytes: 100, AvailableBytes: 40, AvailableInodes: 10}
}

// HTTP transport is intentional: a blocked fake-client reactor would not prove
// that the production Kubernetes client obeys the whole-call cancellation.
type mobilityProbeAPI struct {
	mu         sync.Mutex
	object     map[string]any
	answer     bool
	stall      func(*http.Request) bool
	nonces     []string
	writes     atomic.Int32
	unexpected atomic.Int32
}

func newMobilityProbeAPI(t *testing.T) (*mobilityProbeAPI, *volumeapi.Registry, volumeapi.Pool) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	api := &mobilityProbeAPI{object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool-a", "uid": "pool-uid", "generation": 1, "resourceVersion": "1", "finalizers": []string{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "worker-a", "mountPath": "/mnt/a", "capacity": map[string]string{"limit": "1Gi"}},
		"status": map[string]any{"observedGeneration": 1, "lastProbeTime": now, "capacityProbeSupported": true,
			"inventory":  map[string]any{"valid": true, "observedAt": now},
			"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "PoolReady", "observedGeneration": 1, "lastTransitionTime": now}}},
	}}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	clients := wiring.ForConfig(capacityProbeRESTConfig(&rest.Config{Host: server.URL}), "mobility probe test", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	registry := &volumeapi.Registry{Client: clients.Dynamic}
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/a", Generation: 1}
	return api, registry, pool
}

func (a *mobilityProbeAPI) serve(w http.ResponseWriter, req *http.Request) {
	if a.stall != nil && a.stall(req) {
		<-req.Context().Done()
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if !strings.Contains(req.URL.Path, "/shiftpvpools") || (req.Method != http.MethodGet && req.Method != http.MethodPut) {
		a.unexpected.Add(1)
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	if req.Method == http.MethodPut {
		var object map[string]any
		if json.NewDecoder(req.Body).Decode(&object) != nil {
			http.Error(w, "invalid object", http.StatusBadRequest)
			return
		}
		a.writes.Add(1)
		a.object = object
		annotations := object["metadata"].(map[string]any)["annotations"].(map[string]any)
		var nonce struct{ ID, Evidence string }
		if json.Unmarshal([]byte(annotations[volumeapi.PoolCapacityProbeRequestAnnotation].(string)), &nonce) != nil {
			http.Error(w, "invalid nonce", http.StatusBadRequest)
			return
		}
		a.nonces = append(a.nonces, nonce.ID)
		if a.answer {
			object["status"].(map[string]any)["capacityProbe"] = map[string]any{"requestID": nonce.ID, "evidence": nonce.Evidence, "totalBytes": 100, "availableBytes": 40, "availableInodes": 10}
		}
	}
	var response any = a.object
	if strings.HasSuffix(req.URL.Path, "/shiftpvpools") {
		response = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPoolList", "items": []any{a.object}}
	}
	_ = json.NewEncoder(w).Encode(response)
}
