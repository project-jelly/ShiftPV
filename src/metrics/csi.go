package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (e *Exporter) intercept(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var method string
	switch info.FullMethod {
	case "/csi.v1.Controller/CreateVolume":
		method = "CreateVolume"
	case "/csi.v1.Controller/DeleteVolume":
		method = "DeleteVolume"
	case "/csi.v1.Node/NodePublishVolume":
		method = "NodePublishVolume"
	case "/csi.v1.Node/NodeUnpublishVolume":
		method = "NodeUnpublishVolume"
	default:
		return handler(ctx, request)
	}
	started := time.Now()
	response, err := handler(ctx, request)
	code := status.Code(err)
	if code > codes.Unauthenticated {
		code = codes.Unknown
	}
	e.requests.WithLabelValues(method, code.String()).Inc()
	e.duration.WithLabelValues(method).Observe(time.Since(started).Seconds())
	return response, err
}

func (e *Exporter) ObserveProvisioningStep(step string, elapsed time.Duration) {
	if e != nil {
		e.steps.WithLabelValues(step).Observe(elapsed.Seconds())
	}
}
