package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	lifecycleadmission "github.com/project-jelly/ShiftPV/src/lifecycle/admission"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/volume"
	"github.com/project-jelly/ShiftPV/src/webhook/certificate"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

const webhookVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"

type stalledObserverBudget struct {
	flowcontrol.RateLimiter
	entered chan struct{}
	waits   atomic.Int32
}

func (b *stalledObserverBudget) Wait(ctx context.Context) error {
	b.waits.Add(1)
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func webhookFixture(t *testing.T, original *rest.Config) *http.Server {
	t.Helper()
	fail := func(step string, err error) { t.Fatalf("%s: %v", step, err) }
	cfg := config{namespace: "system", uninstallPermit: "permit", controllerServiceAccount: "controller"}
	return newWebhookServer(cfg, original, &certificate.Manager{}, fail)
}

func webhookReview(t *testing.T, handler http.Handler, ctx context.Context, path string) *admissionv1.AdmissionResponse {
	t.Helper()
	request := &admissionv1.AdmissionRequest{UID: "request-uid", Namespace: "ns", Operation: admissionv1.Create, Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer"}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	request.Object = runtime.RawExtension{Raw: raw}
	if path == "/validate-delete" {
		request.Operation = admissionv1.Delete
		request.Resource = metav1.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"}
		request.Name = "shiftpv"
	}
	body, err := json.Marshal(admissionv1.AdmissionReview{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)).WithContext(ctx))
	if recorder.Code != http.StatusOK {
		t.Fatalf("HTTP=%d body=%s", recorder.Code, recorder.Body)
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(recorder.Body.Bytes(), &review); err != nil || review.Response == nil {
		t.Fatal(review, err)
	}
	if review.Response.UID != request.UID {
		t.Fatal("request identity was not retained")
	}
	return review.Response
}

func mutationAPI(t *testing.T, filter func(http.ResponseWriter, *http.Request) bool) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var paths []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if filter != nil && filter(w, r) {
			return
		}
		var object any
		switch r.URL.Path {
		case "/api/v1/namespaces/ns/persistentvolumeclaims/data":
			object = map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"namespace": "ns", "name": "data"}, "spec": map[string]any{"volumeName": "pv"}}
		case "/api/v1/persistentvolumes/pv":
			object = map[string]any{"apiVersion": "v1", "kind": "PersistentVolume", "metadata": map[string]any{"name": "pv"}, "spec": map[string]any{"csi": map[string]any{"driver": volume.DriverName, "volumeHandle": webhookVolumeID}}}
		case "/apis/shiftpv.io/v1alpha1/shiftpvvolumes/" + webhookVolumeID:
			object = map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": webhookVolumeID, "uid": "volume-uid"}, "status": map[string]any{"phase": "Ready", "ownerNode": "owner"}}
		case "/api/v1/nodes/owner":
			object = &corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "owner"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
		default:
			t.Errorf("unexpected API path %s", r.URL.Path)
			http.Error(w, "unexpected API request", 500)
			return
		}
		_ = json.NewEncoder(w).Encode(object)
	}))
	return api, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), paths...) }
}

func TestWebhookMutationProceedsWhileBackgroundBudgetIsBlocked(t *testing.T) {
	api, requestPaths := mutationAPI(t, nil)
	defer api.Close()
	budget := &stalledObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), entered: make(chan struct{}, 1)}
	original := &rest.Config{Host: api.URL, RateLimiter: budget, UserAgent: "background", Timeout: time.Minute}
	background := wiring.ForConfig(original, "background", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := background.Typed.CoreV1().Namespaces().Get(ctx, "blocked", metav1.GetOptions{})
		done <- err
	}()
	select {
	case <-budget.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background request did not enter limiter")
	}
	server := webhookFixture(t, original)
	requestCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	response := webhookReview(t, server.Handler, requestCtx, "/mutate")
	if !response.Allowed || !strings.Contains(string(response.Patch), "owner") {
		t.Fatalf("blocked background poisoned webhook: %+v", response)
	}
	select {
	case err := <-done:
		t.Fatalf("fixture stopped waiting before cancellation: %v", err)
	default:
	}
	if budget.waits.Load() != 1 || original.UserAgent != "background" || original.Timeout != time.Minute || original.RateLimiter != budget {
		t.Fatal("webhook consumed or changed background budget")
	}
	paths := requestPaths()
	if len(paths) != 4 {
		t.Fatalf("typed/dynamic path incomplete: %v", paths)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("background request did not join cancellation")
	}
}

func TestWebhookMutationRejectsAPIUncertaintyAndPropagatesCancellation(t *testing.T) {
	for _, mode := range []string{"forbidden ledger", "cancelled ledger"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api, requestPaths := mutationAPI(t, func(w http.ResponseWriter, r *http.Request) bool {
				if !strings.HasPrefix(r.URL.Path, "/apis/shiftpv.io/") {
					return false
				}
				if mode == "cancelled ledger" {
					cancel()
					<-r.Context().Done()
					return true
				}
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403})
				return true
			})
			defer api.Close()
			budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
			server := webhookFixture(t, &rest.Config{Host: api.URL, RateLimiter: budget, Timeout: time.Minute})
			response := webhookReview(t, server.Handler, ctx, "/mutate")
			if response.Allowed || len(response.Patch) != 0 || response.Result == nil {
				t.Fatalf("uncertainty granted placement: %+v", response)
			}
			if budget.waits.Load() != 0 || len(requestPaths()) != 3 {
				t.Fatalf("unverified owner was read or inherited budget consumed: %v", requestPaths())
			}
			if mode == "cancelled ledger" && !strings.Contains(response.Result.Message, "context canceled") {
				t.Fatal(response.Result.Message)
			}
		})
	}
}

func TestWebhookDeleteValidationReadsItsOwnAPIRoleAndKeepsSafetyPolicy(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty dependencies", true: "retained data blocks removal"}[retained], func(t *testing.T) {
			var requests atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if r.Method != http.MethodGet {
					t.Errorf("admission attempted %s", r.Method)
					http.Error(w, "read only", 403)
					return
				}
				if r.URL.Path == "/api/v1/namespaces/system/configmaps/permit" {
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
					return
				}
				kind := map[string]string{"/api/v1/persistentvolumes": "PersistentVolumeList", "/api/v1/persistentvolumeclaims": "PersistentVolumeClaimList", "/apis/shiftpv.io/v1alpha1/shiftpvvolumes": "ShiftPVVolumeList", "/apis/shiftpv.io/v1alpha1/shiftpvmoves": "ShiftPVMoveList", "/apis/shiftpv.io/v1alpha1/shiftpvpools": "ShiftPVPoolList"}[r.URL.Path]
				if kind == "" {
					t.Errorf("unexpected validation API path %s", r.URL.Path)
					http.Error(w, "unexpected API request", 403)
					return
				}
				version := "v1"
				if strings.HasPrefix(r.URL.Path, "/apis/shiftpv.io/") {
					version = "shiftpv.io/v1alpha1"
				}
				items := []any{}
				if retained && kind == "PersistentVolumeList" {
					items = append(items, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "retained"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: volume.DriverName, VolumeHandle: webhookVolumeID}}}})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": version, "kind": kind, "items": items})
			}))
			defer api.Close()
			budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
			server := webhookFixture(t, &rest.Config{Host: api.URL, RateLimiter: budget})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response := webhookReview(t, server.Handler, ctx, "/validate-delete")
			if response.Allowed == retained || budget.waits.Load() != 0 || requests.Load() < 6 {
				t.Fatalf("validation changed or budget leaked: response=%+v inheritedWaits=%d requests=%d", response, budget.waits.Load(), requests.Load())
			}
			if retained && (response.Result == nil || !strings.Contains(response.Result.Message, "retained")) {
				t.Fatal("retained dependency was not diagnosed")
			}
		})
	}
}

func TestWebhookHandlersShareOnlyTheirOwnBoundedBudget(t *testing.T) {
	budget := &exhaustedObserverBudget{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1)}
	original := &rest.Config{Host: "http://unused.invalid", RateLimiter: budget}
	server := webhookFixture(t, original)
	mux := server.Handler.(*http.ServeMux)
	mutate, _ := mux.Handler(httptest.NewRequest(http.MethodPost, "/mutate", nil))
	validate, _ := mux.Handler(httptest.NewRequest(http.MethodPost, "/validate-delete", nil))
	m := mutate.(*admission.Handler)
	v := validate.(*lifecycleadmission.Handler)
	checker := v.Checker.(*uninstallcheck.Checker)
	limiter := m.Client.CoreV1().RESTClient().GetRateLimiter()
	if limiter == budget || limiter.QPS() != 50 || m.Client != checker.Client || m.Client != v.Permit.(*uninstallcheck.PermitStore).Client {
		t.Fatal("admission readers do not share a private bounded budget")
	}
	if m.Volumes.(*volumeapi.Registry).Client != checker.Volumes.(*volumeapi.Registry).Client || m.Volumes.(*volumeapi.Registry).Client != checker.Cleanups.(*cleanupapi.Store).Client {
		t.Fatal("dynamic admission readers use another client")
	}
	other := webhookFixture(t, original)
	h, _ := other.Handler.(*http.ServeMux).Handler(httptest.NewRequest(http.MethodPost, "/mutate", nil))
	if h.(*admission.Handler).Client.CoreV1().RESTClient().GetRateLimiter() == limiter {
		t.Fatal("separate roles share a limiter")
	}
}
