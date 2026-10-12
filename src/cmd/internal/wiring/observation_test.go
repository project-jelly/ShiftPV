package wiring

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type gatedAPIBudget struct {
	flowcontrol.RateLimiter
	entered chan struct{}
	release <-chan struct{}
}

func (b *gatedAPIBudget) Wait(ctx context.Context) error {
	b.entered <- struct{}{}
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type observationRoundTripper func(*http.Request) (*http.Response, error)

func (f observationRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestAPIObservationSeparatesSharedBudgetFromTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseBudget, releaseHeaders := make(chan struct{}), make(chan struct{})
	budget := &gatedAPIBudget{RateLimiter: flowcontrol.NewFakeAlwaysRateLimiter(), entered: make(chan struct{}, 2), release: releaseBudget}
	serverEntered := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		serverEntered <- struct{}{}
		select {
		case <-releaseHeaders:
		case <-request.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}`))
	}))
	defer server.Close()
	var wrappedRequests atomic.Int32
	base := &rest.Config{Host: server.URL, QPS: 50, Burst: 100, RateLimiter: budget, WrapTransport: func(next http.RoundTripper) http.RoundTripper {
		return observationRoundTripper(func(request *http.Request) (*http.Response, error) {
			wrappedRequests.Add(1)
			return next.RoundTrip(request)
		})
	}}
	originalWrapper := reflect.ValueOf(base.WrapTransport).Pointer()
	steps := make(chan string, 4)
	observed := WithAPIObservation(base, "node_api_effects", func(step string, _ time.Duration) { steps <- step })
	clients := ForConfig(observed, "test", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	done := make(chan error, 2)
	go func() {
		_, err := clients.Typed.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})
		done <- err
	}()
	go func() {
		_, err := clients.Dynamic.Resource(metav1.SchemeGroupVersion.WithResource("namespaces")).Get(ctx, "default", metav1.GetOptions{})
		done <- err
	}()
	for range 2 {
		awaitSignal(t, ctx, budget.entered)
	}
	if wrappedRequests.Load() != 0 || len(steps) != 0 {
		t.Fatal("budget wait reached transport or reported completion before release")
	}
	close(releaseBudget)
	for range 2 {
		awaitSignal(t, ctx, serverEntered)
	}
	for range 2 {
		if got := awaitStep(t, ctx, steps); got != "node_api_effects_wait" {
			t.Fatalf("before response headers: observed %q", got)
		}
	}
	if len(steps) != 0 {
		t.Fatal("transport observation completed before response headers")
	}
	close(releaseHeaders)
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("request did not complete")
		}
		if got := awaitStep(t, ctx, steps); got != "node_api_effects_roundtrip" {
			t.Fatalf("after response headers: observed %q", got)
		}
	}
	if wrappedRequests.Load() != 2 || base.RateLimiter != budget || base.QPS != 50 || base.Burst != 100 || observed == base || reflect.ValueOf(base.WrapTransport).Pointer() != originalWrapper {
		t.Fatal("observation changed the original budget or lost the existing transport wrapper")
	}
}

func TestAPIObservationPreservesBudgetErrorsWithoutHTTP(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{"timeout rejection", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Second)
		}, nil},
		{"cancelled", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, context.Canceled},
		{"deadline exceeded", func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}, context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := NewAPIBudget(0.01, 1)
			budget.TryAccept()
			var requests atomic.Int32
			steps := make(chan string, 2)
			base := &rest.Config{Host: "http://example.invalid", RateLimiter: budget, Transport: observationRoundTripper(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return nil, errors.New("unexpected HTTP request")
			})}
			clients := ForConfig(WithAPIObservation(base, "node_api_effects", func(step string, _ time.Duration) { steps <- step }), "test", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
			ctx, cancel := test.ctx()
			defer cancel()
			_, err := clients.Typed.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})
			if test.want != nil && !errors.Is(err, test.want) || test.want == nil && !apierrors.IsTimeout(err) {
				t.Fatalf("observation changed error classification: %v", err)
			}
			if requests.Load() != 0 || len(steps) != 1 || <-steps != "node_api_effects_wait" {
				t.Fatal("failed budget wait sent HTTP or lost its observation")
			}
		})
	}
}

func TestAPIObservationWithoutCallbackPreservesConfig(t *testing.T) {
	base := &rest.Config{Host: "http://example.invalid", RateLimiter: NewAPIBudget(50, 100), QPS: 50, Burst: 100, WrapTransport: func(next http.RoundTripper) http.RoundTripper { return next }}
	observed := WithAPIObservation(base, "node_api_effects", nil)
	if observed == base || observed.RateLimiter != base.RateLimiter || observed.QPS != base.QPS || observed.Burst != base.Burst || reflect.ValueOf(observed.WrapTransport).Pointer() != reflect.ValueOf(base.WrapTransport).Pointer() {
		t.Fatal("disabled observation changed the client configuration")
	}
}

func awaitSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal("expected signal did not arrive")
	}
}

func awaitStep(t *testing.T, ctx context.Context, steps <-chan string) string {
	t.Helper()
	select {
	case step := <-steps:
		return step
	case <-ctx.Done():
		t.Fatal("expected observation did not arrive")
		return ""
	}
}
