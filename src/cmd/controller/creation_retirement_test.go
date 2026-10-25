package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestCreationRetirementOwnsBoundedBudgetForSourceAndSettlement(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/shiftpvvolumes"):
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolumeList", "items": []any{}})
		case strings.HasSuffix(r.URL.Path, "/persistentvolumes"):
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeList", "items": []any{}})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
		}
	}))
	defer server.Close()
	budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
	original := &rest.Config{Host: server.URL, RateLimiter: budget, Timeout: time.Minute, UserAgent: "other-observer"}
	fail := func(step string, err error) { t.Fatalf("%s: %v", step, err) }
	observer := wiring.ForConfig(original, "observer", fail)
	if _, err := observer.Typed.CoreV1().Namespaces().Get(context.Background(), "default", metav1.GetOptions{}); err == nil {
		t.Fatal("fixture budget was not exhausted")
	}
	r := newCreationRetirement(config{namespace: "system", nodeDaemonSet: "node"}, original, fail)
	if _, err := r.Volumes.ListCreationIntents(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := volumeapi.State{RequestName: "pvc-original", CreationClaim: &volumeapi.ClaimReference{Namespace: "ns", Name: "data", UID: "original"}}
	if retired, err := r.Claims.Retired(context.Background(), "shiftpv-0123456789abcdef0123456789abcdef", state); !retired || err != nil {
		t.Fatal(retired, err)
	}
	if complete, err := r.Cancel.Advance(context.Background(), "shiftpv-0123456789abcdef0123456789abcdef", "volume-uid"); !complete || err != nil {
		t.Fatal(complete, err)
	}
	client := r.Claims.(creation.RetirementInspector).Reader.(creation.KubernetesRetirementReader).Client
	if limiter := client.CoreV1().RESTClient().GetRateLimiter(); limiter == budget || limiter.QPS() != 5 {
		t.Fatal("retirement uses another role's or unbounded budget")
	}
	if budget.waits.Load() != 1 || requests.Load() != 4 || original.UserAgent != "other-observer" || original.Timeout != time.Minute || original.RateLimiter != budget {
		t.Fatalf("budget leaked: waits=%d requests=%d", budget.waits.Load(), requests.Load())
	}
}
