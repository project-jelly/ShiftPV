package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	rpcserver "github.com/project-jelly/ShiftPV/src/node/rpc/server"
	"github.com/project-jelly/ShiftPV/src/pool/measurement"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"k8s.io/client-go/rest"
)

func startNodeEffects(ctx context.Context, nodeName, hostRoot, address, controllerAccount string, observe func(string, time.Duration), errCh chan<- error) {
	clients := wiring.InCluster(fatal)
	config := rest.CopyConfig(clients.Config)
	config.QPS, config.Burst, config.RateLimiter = 50, 100, nil
	effectClients := wiring.ForConfig(config, "resident Node effects", fatal)
	volumes := &volumeapi.Registry{Client: effectClients.Dynamic}
	effects := &nodeexecutor.Node{Identity: volumeapi.NodeExecutor{Namespace: os.Getenv("POD_NAMESPACE"), PodName: os.Getenv("POD_NAME"), PodUID: os.Getenv("POD_UID"), NodeName: nodeName}, Discovery: nodeexecutor.Discovery{Client: effectClients.Typed}, Volumes: volumes, Cleanups: &cleanupapi.Store{Client: effectClients.Dynamic}, HostRoot: hostRoot}
	effects.ObserveStep = observe
	if address != "" {
		certificate, public, err := security.NewCertificate(effects.Identity.PodUID)
		if err != nil {
			fatal("Node RPC certificate", err)
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			fatal("Node RPC listener", err)
		}
		effects.RPCCertificate, effects.RPCPort = public, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		service := &rpcserver.Service{Identity: effects.Identity, Executor: effects, Pools: volumes, Probe: &measurement.Probe{Pools: volumes, HostRoot: hostRoot}, VerifyIdentity: effects.VerifyRPC}
		authorizer := security.Authorizer{Client: effectClients.Typed, Namespace: effects.Identity.Namespace, ServiceAccount: controllerAccount}
		go func() { errCh <- rpcserver.Serve(ctx, listener, certificate, authorizer, service) }()
	}
	go func() { errCh <- effects.Run(ctx) }()
}
