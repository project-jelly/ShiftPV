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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestCSIDeletionRetriesLocalBudgetRejection(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "request should be rejected locally", 500)
	}))
	defer server.Close()
	budget := wiring.NewAPIBudget(0.01, 1)
	budget.TryAccept()
	clients := wiring.ForConfig(&rest.Config{Host: server.URL, RateLimiter: budget}, "test", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	effects := &nodeexecutor.Client{}
	service := &controllercsi.Service{
		Client: clients.Typed, Namespace: "shiftpv", Operator: effects, CleanupOperator: effects,
		Volumes: &volumeapi.Registry{Client: clients.Dynamic}, Cleanups: &cleanupapi.Store{Client: clients.Dynamic},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := service.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "shiftpv-0123456789abcdef0123456789abcdef"})
	if status.Code(err) != codes.Unavailable || ctx.Err() != nil || requests.Load() != 0 {
		t.Fatalf("local token wait did not become a retryable CSI response: err=%v context=%v requests=%d", err, ctx.Err(), requests.Load())
	}
}

func TestCapacityRetryUsesIndependentRateBudget(t *testing.T) {
	original := &rest.Config{QPS: 100, Burst: 100, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), Timeout: time.Minute, UserAgent: "controller"}
	retry := capacityRetryRESTConfig(original)
	if retry.RateLimiter == original.RateLimiter || retry.QPS != 5 || retry.Burst != 10 || retry.Timeout != 0 {
		t.Fatal("retry observation shares the CSI budget")
	}
	if original.QPS != 100 || original.Burst != 100 || original.Timeout != time.Minute || original.UserAgent != "controller" {
		t.Fatal("original REST config changed")
	}
	original.RateLimiter.TryAccept()
	if !retry.RateLimiter.TryAccept() {
		t.Fatal("CSI rate limiting leaked into notification")
	}
}

type exhaustedObserverBudget struct {
	flowcontrol.RateLimiter
	waits atomic.Int32
}

func (b *exhaustedObserverBudget) Wait(context.Context) error {
	b.waits.Add(1)
	return errors.New("observer budget exhausted")
}

func TestCSILifecycleDependenciesUseIndependentBoundedBudget(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		kind := ""
		if strings.HasSuffix(r.URL.Path, "/shiftpvvolumes") {
			kind = "ShiftPVVolumeList"
		}
		if strings.HasSuffix(r.URL.Path, "/shiftpvmoves") {
			kind = "ShiftPVMoveList"
		}
		if kind != "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": kind, "items": []any{}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
	}))
	defer server.Close()
	budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
	original := &rest.Config{Host: server.URL, RateLimiter: budget, UserAgent: "observer", Timeout: time.Minute}
	fail := func(step string, err error) { t.Fatalf("%s: %v", step, err) }
	observer := wiring.ForConfig(original, "observer", fail)
	ctx := context.Background()
	if _, err := observer.Typed.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{}); err == nil || !strings.Contains(err.Error(), "observer budget exhausted") {
		t.Fatalf("fixture did not exhaust observer budget: %v", err)
	}
	cfg := config{namespace: "shiftpv", poolReadinessStaleAfter: time.Minute,
		helperCPURequest: "10m", helperMemoryRequest: "16Mi", helperCPULimit: "100m", helperMemoryLimit: "64Mi"}
	service, fallback := newCSILifecycle(cfg, original, fail)
	limiter := service.Client.CoreV1().RESTClient().GetRateLimiter()
	if limiter == nil || limiter == budget || limiter.QPS() != 50 || fallback.Client != service.Client || any(fallback.Pools) != any(service.Volumes) || any(service.CapacityPools) != any(service.Volumes) {
		t.Fatal("CSI clients, ledger or helper fallback use a different or unbounded budget")
	}
	registry := service.Volumes.(*volumeapi.Registry)
	if registry.Client != service.Cleanups.Client {
		t.Fatal("CSI registry and cleanup fence do not share the lifecycle client")
	}
	const id = "shiftpv-0123456789abcdef0123456789abcdef"
	if _, err := service.Volumes.Get(ctx, id); !apierrors.IsNotFound(err) {
		t.Fatalf("CSI dynamic request did not reach the API: %v", err)
	}
	if _, err := service.Cleanups.ListForVolume(ctx, id); err != nil {
		t.Fatalf("CSI cleanup fence did not reach the API: %v", err)
	}
	placement, err := service.ConsumerPlacement.Inspect(ctx, consumer.Request{Namespace: "test", Name: "claim", UID: "uid", Node: "node"})
	if err != nil || placement.Placement != consumer.Unknown {
		t.Fatalf("CSI consumer read did not reach the API: %v %v", placement, err)
	}
	if claim, err := service.CreationClaims.Inspect(ctx, creation.ClaimRequest{Namespace: "test", Name: "claim", RequestName: "pvc-uid", NodeName: "node"}); claim != nil || !errors.Is(err, creation.ErrClaimIdentity) {
		t.Fatalf("CSI original claim read did not reach the API: %+v %v", claim, err)
	}
	if budget.waits.Load() != 1 || original.QPS != 0 || original.Burst != 0 || original.RateLimiter != budget || original.UserAgent != "observer" || original.Timeout != time.Minute {
		t.Fatal("CSI requests consumed or changed the observer budget")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 5 {
		t.Fatalf("requests=%v", requests)
	}
	other, _ := newCSILifecycle(cfg, original, fail)
	if other.Client.CoreV1().RESTClient().GetRateLimiter() == limiter {
		t.Fatal("two CSI constructions share one limiter")
	}
}
