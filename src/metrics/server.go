package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

type Exporter struct {
	Cache    *Cache
	Registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	steps    *prometheus.HistogramVec
}

func New(sources ...string) *Exporter {
	e := &Exporter{Cache: newCache(), Registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "shiftpv_csi_requests_total", Help: "Completed CSI lifecycle RPC calls, including retries."}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "shiftpv_csi_request_duration_seconds", Help: "CSI lifecycle RPC duration, not workload downtime.", Buckets: []float64{.005, .025, .1, .5, 1, 5, 30, 120, 600}}, []string{"method"}),
	}
	e.steps = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "shiftpv_provisioning_step_duration_seconds", Help: "Provisioning stage duration; nested stages overlap.", Buckets: []float64{.005, .025, .1, .5, 1, 5, 30, 120}}, []string{"step"})
	e.Registry.MustRegister(e.Cache, e.requests, e.duration, e.steps)
	for _, source := range sources {
		e.Cache.update(source, nil, false)
	}
	return e
}

func (e *Exporter) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(e.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 8, Timeout: 5 * time.Second}))
	return mux
}

func (e *Exporter) Serve(ctx context.Context, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return e.serve(ctx, listener)
}

func (e *Exporter) serve(ctx context.Context, listener net.Listener) error {
	server := &http.Server{Handler: e.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				_ = server.Close()
			}
		case <-done:
		}
	}()
	err := server.Serve(listener)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Start keeps listener failures outside the CSI process error channel.
func (e *Exporter) Start(ctx context.Context, address string) {
	go func() {
		if err := e.Serve(ctx, address); err != nil {
			klog.Errorf("metrics endpoint stopped: %v", err)
		}
	}()
}

func (e *Exporter) ServerOptions() []grpc.ServerOption {
	if e == nil {
		return nil
	}
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(e.intercept)}
}
