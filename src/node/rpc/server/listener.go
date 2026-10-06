package server

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Serve bounds calls even when a client supplies no deadline. Cancellation
// leaves the API intent and local receipt available to the recovery worker.
func Serve(ctx context.Context, listener net.Listener, cert tls.Certificate, auth security.Authorizer, service *Service) error {
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}})), grpc.ChainUnaryInterceptor(boundDeadline, auth.Unary), grpc.MaxRecvMsgSize(64*1024), grpc.MaxSendMsgSize(64*1024), grpc.MaxConcurrentStreams(64))
	protocol.RegisterNodeServer(server, service)
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
		case <-done:
		}
	}()
	err := server.Serve(listener)
	close(done)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
func boundDeadline(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	limit := 2 * time.Minute
	if info.FullMethod == protocol.Node_GetCapacity_FullMethodName {
		limit = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	return next(ctx, req)
}
