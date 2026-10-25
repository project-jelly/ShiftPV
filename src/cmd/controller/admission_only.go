package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"k8s.io/klog/v2"
)

// Maintenance needs the same admission checks for causal Pool scan requests
// while normal writers are stopped. Reuse serving/certificate policy without
// constructing CSI, discovery, capacity or lifecycle reconciliation workers.
func runAdmissionOnly(cfg config) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	clients := wiring.InCluster(fatal)
	gate := &uninstallcheck.QuiesceGate{Store: &uninstallcheck.PermitStore{
		Client: clients.Typed, Namespace: cfg.namespace, Name: cfg.uninstallPermit, CSIDriver: admission.DriverName,
	}, Interval: 200 * time.Millisecond}
	if err := wiring.BootstrapWithRetry(ctx, "maintenance admission gate", gate.Bootstrap); err != nil {
		fatal("bootstrap maintenance admission gate", err)
	}
	certificates := newCertificateManager(cfg, clients.Typed, gate)
	if err := wiring.BootstrapWithRetry(ctx, "maintenance admission certificates", certificates.Bootstrap); err != nil {
		fatal("bootstrap maintenance admission certificates", err)
	}
	server := newWebhookServer(cfg, clients.Config, certificates, fatal)
	errors := make(chan error, 3)
	go func() { errors <- gate.Run(ctx) }()
	go func() { errors <- certificates.Run(ctx) }()
	go serveWebhook(server, cfg.webhookAddress, errors)
	klog.Info("serving ShiftPV maintenance admission only; normal writers are stopped")
	awaitShutdown(ctx, errors, stop)
	shutdownWebhook(server)
}
