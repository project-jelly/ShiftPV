package wiring

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestAPIBudgetDeadlineRejectionRemainsRetryableThroughREST(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected HTTP request", 500)
	}))
	defer server.Close()
	budget := NewAPIBudget(0.01, 1)
	if !budget.TryAccept() {
		t.Fatal("initial token unavailable")
	}
	clients := ForConfig(&rest.Config{Host: server.URL, RateLimiter: budget}, "test", func(step string, err error) { t.Fatalf("%s: %v", step, err) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := clients.Typed.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})
	if !apierrors.IsTimeout(err) || ctx.Err() != nil || requests.Load() != 0 {
		t.Fatalf("budget rejection lost its classification or sent a request: err=%v context=%v requests=%d", err, ctx.Err(), requests.Load())
	}
	_, err = clients.Dynamic.Resource(metav1.SchemeGroupVersion.WithResource("namespaces")).Get(ctx, "default", metav1.GetOptions{})
	if !apierrors.IsTimeout(err) || requests.Load() != 0 {
		t.Fatalf("dynamic client bypassed shared budget: %v", err)
	}
}

func TestAPIBudgetPreservesContextErrors(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		budget := NewAPIBudget(0.01, 1)
		budget.TryAccept()
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		want := context.DeadlineExceeded
		if canceled {
			cancel()
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
			want = context.Canceled
		}
		err := budget.Wait(ctx)
		cancel()
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
}
