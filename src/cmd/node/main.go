package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/csi/identity"
	nodecsi "github.com/project-jelly/ShiftPV/src/csi/node"
	csiserver "github.com/project-jelly/ShiftPV/src/csi/server"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/metrics"
	shiftmount "github.com/project-jelly/ShiftPV/src/node/mount"
	nodeobservation "github.com/project-jelly/ShiftPV/src/node/observation"
	poolreadiness "github.com/project-jelly/ShiftPV/src/pool/readiness"
)

var version = "dev"

func main() {
	var (
		endpoint              = flag.String("endpoint", "unix:///csi/csi.sock", "CSI Unix socket endpoint")
		nodeName              = flag.String("node-name", os.Getenv("NODE_NAME"), "Kubernetes node name")
		hostRoot              = flag.String("host-root", "/host", "host filesystem root mounted into the node plugin")
		targetRoot            = flag.String("target-root", "/var/lib/kubelet/pods", "allowed kubelet publish target root")
		poolReadinessInterval = flag.Duration("pool-readiness-interval", time.Minute, "interval between local Pool mount and write probes")
		metricsAddress        = flag.String("metrics-listen-address", "", "metrics HTTP address; empty disables observation")
	)
	klog.InitFlags(nil)
	flag.Parse()
	if *poolReadinessInterval <= 0 {
		klog.Fatalf("pool readiness interval must be positive")
	}

	registry := &volumeapi.Registry{Client: wiring.InClusterDynamic(fatal)}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	binder := shiftmount.NewBinder(*targetRoot)
	nodeService := &nodecsi.Service{
		NodeName:   *nodeName,
		HostRoot:   *hostRoot,
		TargetRoot: *targetRoot,
		Binder:     binder,
		Volumes:    registry,
		Pools:      registry,
	}
	identityService := &identity.Service{Version: version}
	readinessReconciler := &poolreadiness.Reconciler{
		Wake:     poolreadiness.WatchPoolChanges(ctx, registry.Client, *nodeName),
		NodeName: *nodeName, Pools: registry, Inspector: poolreadiness.NewProbe(*hostRoot), Interval: *poolReadinessInterval,
	}
	inventoryScanner := &nodeobservation.Scanner{
		HostRoot: *hostRoot, TargetRoot: *targetRoot, Installation: registry, Publications: binder, Limit: 256,
	}
	inventoryScanner.SnapshotPublications = func() (nodeobservation.Publications, error) {
		return binder.PublicationSnapshot()
	}
	readinessReconciler.Inventory = inventoryScanner.Scan
	readinessReconciler.Release = inventoryScanner.ReleasePool
	var exporter *metrics.Exporter
	if *metricsAddress != "" {
		exporter = metrics.New("filesystem")
		exporter.Start(ctx, *metricsAddress)
		readinessReconciler.Observe = exporter.ObservePool
	}

	klog.Infof("starting ShiftPV node plugin %s on %s", version, *nodeName)
	errCh := make(chan error, 2)
	go func() { errCh <- readinessReconciler.Run(ctx) }()
	go func() {
		errCh <- csiserver.ServeContext(ctx, *endpoint, func(server *grpc.Server) {
			csi.RegisterIdentityServer(server, identityService)
			csi.RegisterNodeServer(server, nodeService)
		}, exporter.ServerOptions()...)
	}()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			klog.Errorf("ShiftPV node component stopped: %v", err)
		}
		stop()
	}
}

func fatal(step string, err error) { klog.Fatalf("%s: %v", step, err) }
