package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/node/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type blockedMetadataBudget struct {
	flowcontrol.RateLimiter
	entered, resume chan struct{}
	once            sync.Once
}

func (b *blockedMetadataBudget) Wait(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestNodeBackgroundBudgetDoesNotBlockForegroundRequests(t *testing.T) {
	var mu sync.Mutex
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.UserAgent()]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/namespaces/kube-system" {
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPoolList", "items": []any{}})
	}))
	defer server.Close()
	base := &rest.Config{Host: server.URL, QPS: 1, Burst: 1, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), UserAgent: "base", Timeout: time.Minute}
	configs := newNodeAPIConfigs(base)
	seen := map[flowcontrol.RateLimiter]bool{base.RateLimiter: true}
	for _, config := range []*rest.Config{configs.foreground, configs.measurement, configs.metadata, configs.effects} {
		if config.RateLimiter == nil || seen[config.RateLimiter] {
			t.Fatal("Node roles share an inherited or unbounded API budget")
		}
		seen[config.RateLimiter] = true
	}
	budget := &blockedMetadataBudget{RateLimiter: configs.metadata.RateLimiter, entered: make(chan struct{}), resume: make(chan struct{})}
	configs.metadata.RateLimiter = budget
	clients := newNodeAPIClients(configs, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	if clients.effects.Typed.CoreV1().RESTClient().GetRateLimiter() != configs.effects.RateLimiter {
		t.Fatal("typed effects client escaped its role budget")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector := &metadata.Collector{NodeName: "worker-a", HostRoot: "/host", Repository: clients.metadata, Retention: metadata.DefaultRetention}
	done := make(chan error, 1)
	go func() { done <- collector.Reconcile(ctx) }()
	select {
	case <-budget.entered:
	case <-ctx.Done():
		t.Fatal("GC did not enter the injected budget")
	}
	// Use real REST clients and Registry reads while the collector is stalled.
	if _, err := clients.foreground.ListPoolRegistrations(ctx); err != nil {
		t.Fatalf("foreground request blocked by GC: %v", err)
	}
	if _, err := clients.measurement.ListPoolRegistrations(ctx); err != nil {
		t.Fatalf("measurement request blocked by GC: %v", err)
	}
	if _, err := clients.effects.Dynamic.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Get(ctx, "kube-system", metav1.GetOptions{}); err != nil {
		t.Fatalf("effects request blocked by GC: %v", err)
	}
	mu.Lock()
	if requests["shiftpv-node-foreground"] != 1 || requests["shiftpv-node-measurement"] != 1 || requests["shiftpv-node-effects"] != 1 || requests["shiftpv-node-metadata"] != 0 {
		t.Errorf("unexpected requests before GC resumes: %v", requests)
	}
	mu.Unlock()
	close(budget.resume)
	if err := <-done; err != nil {
		t.Fatalf("GC did not resume: %v", err)
	}
	if base.QPS != 1 || base.Burst != 1 || base.UserAgent != "base" || base.Timeout != time.Minute || configs.foreground.QPS != 5 || configs.foreground.Burst != 10 || configs.effects.QPS != 50 || configs.effects.Burst != 100 {
		t.Fatal("client construction changed the base or role limits")
	}
}
