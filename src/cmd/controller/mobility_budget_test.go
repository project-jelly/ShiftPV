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
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/webhook/certificate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func mobilityBudgetConfig() config {
	return config{mobilityEnabled: true, namespace: "system", poolReadinessStaleAfter: time.Minute,
		helperCPURequest: "10m", helperMemoryRequest: "16Mi", helperCPULimit: "100m", helperMemoryLimit: "64Mi"}
}

// mobilityBudgetAPI deliberately uses real REST clients. It is a transport and
// dependency fixture, not an API server or a filesystem/capacity simulation.
func mobilityBudgetAPI(t testing.TB, filter func(http.ResponseWriter, *http.Request) bool) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path+" "+r.UserAgent())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if filter != nil && filter(w, r) {
			return
		}
		switch {
		case r.URL.Path == "/api/v1/nodes/source":
			_ = json.NewEncoder(w).Encode(&corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "source", UID: "source-uid"}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/shiftpvmoves"):
			var object map[string]any
			if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
				t.Error(err)
			}
			metadata := object["metadata"].(map[string]any)
			metadata["name"], metadata["uid"], metadata["resourceVersion"] = "move-a", "move-uid", "7"
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(object)
		case strings.HasSuffix(r.URL.Path, "/shiftpvmoves/move-a"):
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": "move-a", "uid": "move-uid", "resourceVersion": "7"}, "spec": map[string]any{"volumeID": webhookVolumeID, "sourceNode": "source", "protocolVersion": volumeapi.MoveProtocolPreparedV1}})
		case strings.HasSuffix(r.URL.Path, "/shiftpvmoves") || strings.HasSuffix(r.URL.Path, "/shiftpvvolumes"):
			kind := "ShiftPVMoveList"
			if strings.HasSuffix(r.URL.Path, "/shiftpvvolumes") {
				kind = "ShiftPVVolumeList"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": kind, "items": []any{}})
		case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/system/pods/"):
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
		default:
			t.Errorf("unexpected API request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusForbidden)
		}
	}))
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), requests...) }
}

func TestMobilityDependenciesProgressWhileInheritedBudgetIsBlocked(t *testing.T) {
	server, requests := mobilityBudgetAPI(t, nil)
	defer server.Close()
	budget := &stalledObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), entered: make(chan struct{}, 1)}
	original := &rest.Config{Host: server.URL, RateLimiter: budget, UserAgent: "background", Timeout: time.Minute}
	fail := func(step string, err error) { t.Fatalf("%s: %v", step, err) }
	background := wiring.ForConfig(original, "background", fail)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := background.Typed.CoreV1().Nodes().Get(ctx, "source", metav1.GetOptions{})
		done <- err
	}()
	select {
	case <-budget.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background request did not enter its limiter")
	}
	mobility := newMobilityComponents(mobilityBudgetConfig(), original, fail)
	requestCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if _, err := mobility.client.CoreV1().Nodes().Get(requestCtx, "source", metav1.GetOptions{}); err != nil {
		t.Fatalf("mobility typed read inherited the blocked budget: %v", err)
	}
	move, err := mobility.volumes.CreateMove(requestCtx, "move-", volumeapi.MoveSpec{VolumeID: webhookVolumeID, SourceNode: "source", ProtocolVersion: volumeapi.MoveProtocolPreparedV1})
	if err != nil || move.Name != "move-a" || move.UID != "move-uid" {
		t.Fatalf("mobility journal write did not reach the API: %+v %v", move, err)
	}
	if observed, err := mobility.volumes.GetMove(requestCtx, move.Name); err != nil || observed.UID != move.UID {
		t.Fatalf("mobility journal read did not preserve identity: %+v %v", observed, err)
	}
	if _, err := mobility.cleanups.ListForVolume(requestCtx, webhookVolumeID); err != nil {
		t.Fatalf("mobility cleanup observation inherited the blocked budget: %v", err)
	}
	if settled, err := mobility.operator.FinalizeSourceUsage(requestCtx, move); err != nil || !settled {
		t.Fatalf("mobility source-usage helper inherited the blocked budget: %v %v", settled, err)
	}
	if budget.waits.Load() != 1 || original.RateLimiter != budget || original.Timeout != time.Minute || original.UserAgent != "background" || original.QPS != 0 || original.Burst != 0 {
		t.Fatal("mobility consumed or changed another role's budget")
	}
	if got := requests(); len(got) != 6 {
		t.Fatalf("production collaborators did not complete their HTTP path: %v", got)
	} else {
		for _, request := range got {
			if !strings.HasSuffix(request, " shiftpv-mobility") {
				t.Fatalf("collaborator escaped the mobility role: %s", request)
			}
		}
	}
	select {
	case err := <-done:
		t.Fatalf("background request stopped before cancellation: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("background cancellation was lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background request did not join cancellation")
	}
}

func TestMobilityComponentsOwnOneBoundedBudgetAndKeepOtherRolesSeparate(t *testing.T) {
	budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
	original := &rest.Config{Host: "http://unused.invalid", RateLimiter: budget}
	fail := func(step string, err error) { t.Fatalf("%s: %v", step, err) }
	cfg := mobilityBudgetConfig()
	mobility := newMobilityComponents(cfg, original, fail)
	limiter := mobility.client.CoreV1().RESTClient().GetRateLimiter()
	if limiter == nil || limiter == budget || limiter.QPS() != 50 || mobility.operator.Client != mobility.client || mobility.operator.Pools != mobility.volumes || mobility.volumes.Client != mobility.dynamicClient || mobility.cleanups.Client != mobility.dynamicClient {
		t.Fatal("mobility collaborators do not share their private bounded clients")
	}
	csi, _ := newCSILifecycle(cfg, original, fail)
	webhook := newWebhookServer(cfg, original, &certificate.Manager{}, fail)
	handler, _ := webhook.Handler.(*http.ServeMux).Handler(httptest.NewRequest(http.MethodPost, "/mutate", nil))
	admissionLimiter := handler.(*admission.Handler).Client.CoreV1().RESTClient().GetRateLimiter()
	others := []flowcontrol.RateLimiter{budget, csi.Client.CoreV1().RESTClient().GetRateLimiter(), admissionLimiter, capacityRetryRESTConfig(original).RateLimiter, capacityProbeRESTConfig(original).RateLimiter, newMobilityComponents(cfg, original, fail).client.CoreV1().RESTClient().GetRateLimiter()}
	for _, other := range others {
		if limiter == other {
			t.Fatal("mobility shares a limiter with another role or construction")
		}
	}
}

func TestMobilityTypedAndDynamicShareBoundedBudget(t *testing.T) {
	for _, mode := range []string{"immediate HTTP", "slow HTTP"} {
		t.Run(mode, func(t *testing.T) {
			server, requests := mobilityBudgetAPI(t, func(http.ResponseWriter, *http.Request) bool {
				if mode == "slow HTTP" {
					time.Sleep(4 * time.Millisecond)
				}
				return false
			})
			defer server.Close()
			budget := &controlledMobilityBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(50, 100), entered: make(chan struct{}, 1), release: make(chan struct{})}
			mobility := mobilityComponentsForConfig(mobilityBudgetConfig(), &rest.Config{Host: server.URL, RateLimiter: budget}, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			operations := []func() error{
				func() error {
					_, err := mobility.client.CoreV1().Nodes().Get(ctx, "source", metav1.GetOptions{})
					return err
				},
				func() error { _, err := mobility.volumes.GetMove(ctx, "move-a"); return err },
			}
			for i, operation := range operations {
				done := make(chan error, 1)
				go func() { done <- operation() }()
				select {
				case <-budget.entered:
				case err := <-done:
					t.Fatalf("client %d bypassed the shared limiter: %v", i, err)
				case <-time.After(5 * time.Second):
					t.Fatalf("client %d did not enter the shared limiter", i)
				}
				if got := len(requests()); got != i || budget.waits.Load() != int32(i+1) {
					t.Fatalf("client %d reached HTTP before budget release: requests=%d waits=%d", i, got, budget.waits.Load())
				}
				budget.release <- struct{}{}
				select {
				case err := <-done:
					if err != nil || len(requests()) != i+1 {
						t.Fatalf("released client %d did not reach HTTP: err=%v requests=%v", i, err, requests())
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("released client %d did not complete", i)
				}
			}
			// Cancellation during the shared token wait must not issue an HTTP
			// request or turn unknown state into journal adoption.
			done := make(chan error, 1)
			go func() { _, err := mobility.volumes.GetMove(ctx, "move-a"); done <- err }()
			select {
			case <-budget.entered:
			case err := <-done:
				t.Fatalf("cancelled read bypassed the shared limiter: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("read did not enter cancellable token wait")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || len(requests()) != 2 || budget.waits.Load() != 3 {
					t.Fatalf("token wait lost cancellation: err=%v requests=%v waits=%d", err, requests(), budget.waits.Load())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("token wait did not join cancellation")
			}
		})
	}
}

type controlledMobilityBudget struct {
	flowcontrol.RateLimiter
	entered chan struct{}
	release chan struct{}
	waits   atomic.Int32
}

func (b *controlledMobilityBudget) Wait(ctx context.Context) error {
	b.waits.Add(1)
	select {
	case b.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestMobilityReadsPreserveAPIUncertaintyAndCancellation(t *testing.T) {
	for _, mode := range []string{"forbidden", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var writes atomic.Int32
			server, requests := mobilityBudgetAPI(t, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodGet {
					writes.Add(1)
				}
				if mode == "cancelled" {
					cancel()
					<-r.Context().Done()
					return true
				}
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403})
				return true
			})
			defer server.Close()
			inherited := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
			mobility := newMobilityComponents(mobilityBudgetConfig(), &rest.Config{Host: server.URL, RateLimiter: inherited}, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
			move, err := mobility.volumes.GetMove(ctx, "move-a")
			if err == nil || move.UID != "" || writes.Load() != 0 || len(requests()) != 1 || inherited.waits.Load() != 0 {
				t.Fatalf("unverified journal adopted or API failure hidden: %+v %v requests=%v", move, err, requests())
			}
			if mode == "forbidden" && !apierrors.IsForbidden(err) || mode == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("original error was lost: %v", err)
			}
		})
	}
}

func TestMobilityDeniedJournalWriteDoesNotAdoptAnIntent(t *testing.T) {
	server, requests := mobilityBudgetAPI(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403})
		return true
	})
	defer server.Close()
	mobility := newMobilityComponents(mobilityBudgetConfig(), &rest.Config{Host: server.URL}, func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	move, err := mobility.volumes.CreateMove(context.Background(), "move-", volumeapi.MoveSpec{VolumeID: webhookVolumeID, SourceNode: "source", ProtocolVersion: volumeapi.MoveProtocolPreparedV1})
	if !apierrors.IsForbidden(err) || move.UID != "" || len(requests()) != 1 || !strings.HasPrefix(requests()[0], "POST ") {
		t.Fatalf("denied intent was adopted or retried: move=%+v err=%v requests=%v", move, err, requests())
	}
}

func TestDisabledMobilityDoesNotConstructClients(t *testing.T) {
	components := newMobilityComponents(config{}, nil, func(step string, err error) { t.Fatalf("disabled mobility constructed clients: %s: %v", step, err) })
	if components.client != nil || components.dynamicClient != nil || components.volumes != nil || components.operator != nil || components.cleanups != nil {
		t.Fatal("disabled mobility constructed unused dependencies")
	}
}

// BenchmarkMobilityAPIRequestBudget holds the HTTP trace fixed. The baseline
// recreates the old default typed/dynamic 5/10 clients; this is a client-side
// budget experiment, not an end-to-end mobility or cluster latency measurement.
func BenchmarkMobilityAPIRequestBudget(b *testing.B) {
	for _, mode := range []string{"default-5-10", "mobility-50-100"} {
		b.Run(mode, func(b *testing.B) {
			for attempt := 0; attempt < b.N; attempt++ {
				server, requests := mobilityBudgetAPI(b, nil)
				base := &rest.Config{Host: server.URL}
				fail := func(step string, err error) { b.Fatalf("%s: %v", step, err) }
				mobility := newMobilityComponents(mobilityBudgetConfig(), base, fail)
				if mode == "default-5-10" {
					clients := wiring.ForConfig(base, "former mobility", fail)
					mobility.client, mobility.dynamicClient = clients.Typed, clients.Dynamic
					mobility.volumes.Client = clients.Dynamic
				}
				started := time.Now()
				for i := 0; i < 60; i++ {
					if _, err := mobility.client.CoreV1().Nodes().Get(context.Background(), "source", metav1.GetOptions{}); err != nil {
						b.Fatal(err)
					}
					if _, err := mobility.volumes.GetMove(context.Background(), "move-a"); err != nil {
						b.Fatal(err)
					}
				}
				elapsed := time.Since(started)
				if len(requests()) != 120 {
					b.Fatalf("request trace changed: %v", requests())
				}
				b.ReportMetric(120, "requests/op")
				b.ReportMetric(float64(elapsed.Microseconds())/1000, "trace-ms/op")
				b.Logf("mode=%s requests=120 typed=60 dynamic=60 trace=%s", mode, elapsed)
				server.Close()
			}
		})
	}
}
