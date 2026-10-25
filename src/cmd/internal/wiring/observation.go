package wiring

import (
	"context"
	"net/http"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// WithAPIObservation copies a client configuration and observes its existing
// API budget and transport. Reusing the result for typed and dynamic clients
// preserves their shared budget. Prefix must be a fixed role name.
func WithAPIObservation(config *rest.Config, prefix string, observe func(string, time.Duration)) *rest.Config {
	observed := rest.CopyConfig(config)
	if observe == nil {
		return observed
	}
	if observed.RateLimiter != nil {
		observed.RateLimiter = &observedAPIBudget{observed.RateLimiter, prefix + "_wait", observe}
	}
	observed.Wrap(func(transport http.RoundTripper) http.RoundTripper {
		return observedAPITransport{transport, prefix + "_roundtrip", observe}
	})
	return observed
}

type observedAPIBudget struct {
	flowcontrol.RateLimiter
	step    string
	observe func(string, time.Duration)
}

func (b observedAPIBudget) Wait(ctx context.Context) error {
	started := time.Now()
	defer func() { b.observe(b.step, time.Since(started)) }()
	return b.RateLimiter.Wait(ctx)
}

type observedAPITransport struct {
	http.RoundTripper
	step    string
	observe func(string, time.Duration)
}

// RoundTrip measures through response headers or a transport error. Response
// body reads and API decoding occur after this observation.
func (t observedAPITransport) RoundTrip(request *http.Request) (*http.Response, error) {
	started := time.Now()
	defer func() { t.observe(t.step, time.Since(started)) }()
	return t.RoundTripper.RoundTrip(request)
}
