package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/flagvalue"
	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/csi/identity"
	csiserver "github.com/project-jelly/ShiftPV/src/csi/server"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	lifecycleadmission "github.com/project-jelly/ShiftPV/src/lifecycle/admission"
	poolcontroller "github.com/project-jelly/ShiftPV/src/lifecycle/poolcontroller"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	"github.com/project-jelly/ShiftPV/src/metrics"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	mobilitycontroller "github.com/project-jelly/ShiftPV/src/mobility/controller"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	poolmeasurement "github.com/project-jelly/ShiftPV/src/pool/measurement"
	remotemeasurement "github.com/project-jelly/ShiftPV/src/pool/measurement/remote"
	"github.com/project-jelly/ShiftPV/src/provisioning"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	webhookcertificate "github.com/project-jelly/ShiftPV/src/webhook/certificate"
)

var version = "dev"

// config is the operator-supplied controller configuration. Every field is
// bound directly to the flag of the same purpose, so the flag names, defaults
// and help text stay exactly where they were declared.
type config struct {
	nodeDaemonSet                             string
	nodeRPCTokenFile                          string
	storageClassNames                         flagvalue.Names
	endpoint, namespace, helperImage          string
	helperWait                                time.Duration
	cleanupAbsenceWait                        time.Duration
	helperCPURequest, helperMemoryRequest     string
	helperCPULimit, helperMemoryLimit         string
	helperServiceAccount                      string
	poolReadinessStaleAfter                   time.Duration
	mobilityEnabled                           bool
	mobilityInterval, moveJournalRetention    time.Duration
	mobilityImage                             string
	webhookAddress, webhookService            string
	webhookSecret, webhookConfiguration       string
	validationConfiguration                   string
	controllerServiceAccount, uninstallPermit string
	metricsAddress                            string
	metricsInterval                           time.Duration
}

// parseFlags declares the controller command line, parses it, and rejects the
// durations no component can run with.
func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.endpoint, "endpoint", "unix:///run/csi/csi.sock", "CSI Unix socket endpoint")
	flag.StringVar(&cfg.namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace for helper Pods and controller state")
	flag.StringVar(&cfg.nodeDaemonSet, "node-daemonset-name", "", "trusted resident Node DaemonSet; empty uses helper Pods")
	flag.StringVar(&cfg.nodeRPCTokenFile, "node-rpc-token-file", "", "projected token for internal Node gRPC; empty keeps API transport")
	flag.StringVar(&cfg.helperImage, "helper-image", "busybox:1.37", "directory helper Pod image")
	flag.DurationVar(&cfg.helperWait, "helper-timeout", 2*time.Minute, "helper Pod completion timeout")
	flag.DurationVar(&cfg.cleanupAbsenceWait, "cleanup-absence-wait", 5*time.Second, "bounded wait for post-delete absence proof; zero returns immediately")
	flag.StringVar(&cfg.helperCPURequest, "helper-cpu-request", "10m", "helper Pod CPU request")
	flag.StringVar(&cfg.helperMemoryRequest, "helper-memory-request", "16Mi", "helper Pod memory request")
	flag.StringVar(&cfg.helperCPULimit, "helper-cpu-limit", "100m", "helper Pod CPU limit")
	flag.StringVar(&cfg.helperMemoryLimit, "helper-memory-limit", "64Mi", "helper Pod memory limit")
	flag.StringVar(&cfg.helperServiceAccount, "helper-service-account", "", "service account used by identity-aware helper Pods")
	flag.DurationVar(&cfg.poolReadinessStaleAfter, "pool-readiness-stale-after", 3*time.Minute, "maximum age of a successful node Pool readiness probe")
	flag.BoolVar(&cfg.mobilityEnabled, "mobility-enabled", true, "run the automatic cordon mobility reconciler and admission webhook")
	flag.DurationVar(&cfg.mobilityInterval, "mobility-interval", 30*time.Second, "mobility reconciliation safety interval")
	flag.DurationVar(&cfg.moveJournalRetention, "move-journal-retention", mobilitycontroller.DefaultMoveJournalRetention, "minimum retention for settled terminal ShiftPVMove journals")
	flag.StringVar(&cfg.mobilityImage, "mobility-helper-image", "shiftpv-rsync-helper:dev", "rsync mobility helper image")
	flag.StringVar(&cfg.webhookAddress, "webhook-listen-address", ":9443", "mobility admission HTTPS listen address")
	flag.StringVar(&cfg.webhookService, "webhook-service-name", "shiftpv-webhook", "mobility admission Service name")
	flag.StringVar(&cfg.webhookSecret, "webhook-tls-secret-name", "shiftpv-webhook-tls", "managed mobility admission TLS Secret name")
	flag.StringVar(&cfg.webhookConfiguration, "webhook-configuration-name", "shiftpv-mobility", "managed MutatingWebhookConfiguration name")
	flag.StringVar(&cfg.validationConfiguration, "validation-webhook-configuration-name", "shiftpv-lifecycle", "managed lifecycle ValidatingWebhookConfiguration name")
	flag.StringVar(&cfg.controllerServiceAccount, "controller-service-account", "shiftpv-controller", "trusted controller service account for runtime ShiftPV resource deletion")
	flag.StringVar(&cfg.uninstallPermit, "uninstall-permit-name", "shiftpv-uninstall-permit", "trusted uninstall permit ConfigMap name")
	flag.StringVar(&cfg.metricsAddress, "metrics-listen-address", "", "metrics HTTP address; empty disables observation")
	flag.DurationVar(&cfg.metricsInterval, "metrics-snapshot-interval", 30*time.Second, "read-only metrics metadata interval")
	flag.Var(&cfg.storageClassNames, "storage-class-name", "repeatable StorageClass name protected from unsafe driver deletion")
	klog.InitFlags(nil)
	flag.Parse()
	if cfg.poolReadinessStaleAfter <= 0 {
		klog.Fatalf("pool readiness stale duration must be positive")
	}
	if cfg.cleanupAbsenceWait < 0 || cfg.cleanupAbsenceWait > 30*time.Second {
		klog.Fatalf("cleanup absence wait must be between zero and 30 seconds")
	}
	if cfg.moveJournalRetention < time.Hour {
		klog.Fatalf("move journal retention must be at least one hour")
	}
	return cfg
}

func main() {
	cfg := parseFlags()
	clients := wiring.InCluster(fatal)
	config, client, dynamicClient := clients.Config, clients.Typed, clients.Dynamic
	admissionClients := wiring.ForConfig(admissionRESTConfig(config), "lifecycle admission", fatal)
	admissionClient, admissionDynamicClient := admissionClients.Typed, admissionClients.Dynamic
	volumeRegistry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}
	cleanupStore := &cleanupapi.Store{Client: dynamicClient}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	exporter := startMetrics(ctx, cfg, config)
	permitStore := &uninstallcheck.PermitStore{Client: admissionClient, Namespace: cfg.namespace, Name: cfg.uninstallPermit, CSIDriver: admission.DriverName}
	quiesceGate := &uninstallcheck.QuiesceGate{Store: permitStore, Interval: 200 * time.Millisecond}
	if err := wiring.BootstrapWithRetry(ctx, "uninstall quiesce state", quiesceGate.Bootstrap); err != nil {
		klog.Fatalf("bootstrap uninstall quiesce gate: %v", err)
	}
	operator := newHelperRunner(cfg, client, volumeRegistry)
	operator.ObserveStep = exporter.ObserveProvisioningStep
	retryClients := wiring.ForConfig(capacityRetryRESTConfig(config), "capacity retry", fatal)
	retryPools := &volumeapi.Registry{Client: retryClients.Dynamic, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}
	capacityRetries := provisioning.NewRetries(retryClients.Typed, retryPools)
	go capacityRetries.Run(ctx, retryClients.Dynamic)
	probeConfig := capacityRetryRESTConfig(config)
	probeConfig.UserAgent = "shiftpv-capacity-probe"
	probePools := &volumeapi.Registry{Client: wiring.ForConfig(probeConfig, "capacity probe", fatal).Dynamic, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}
	apiCapacityProbe := &poolmeasurement.Client{Pools: probePools, Fallback: operator, ObserveStep: exporter.ObserveProvisioningStep}
	var capacityProbe controllercsi.PoolCapacityProbe = apiCapacityProbe
	poolLocks := &poolcapacity.Locker{}
	lifecycleChecker := newLifecycleChecker(cfg, admissionClient, admissionDynamicClient)
	poolLifecycleReconciler := &poolcontroller.Reconciler{Pools: volumeRegistry, Safety: lifecycleChecker, Quiesce: permitStore, PoolLocks: poolLocks, Interval: 2 * time.Second}
	effectClients := wiring.ForConfig(admissionRESTConfig(config), "resident Node effects", fatal)
	effects := &nodeexecutor.Client{Discovery: nodeexecutor.Discovery{Client: effectClients.Typed, Namespace: cfg.namespace, DaemonSet: cfg.nodeDaemonSet}, Volumes: &volumeapi.Registry{Client: effectClients.Dynamic, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}, Fallback: operator, Timeout: cfg.helperWait}
	if cfg.nodeRPCTokenFile != "" {
		// The outer RPC probe observes the complete call, including API fallback.
		apiCapacityProbe.ObserveStep = nil
		rpc := &connection.Client{TokenFile: cfg.nodeRPCTokenFile}
		defer rpc.Close()
		effects.RPC = rpc
		capacityProbe = &remotemeasurement.Client{RPC: rpc, Discovery: effects.Discovery, Pools: probePools, Fallback: capacityProbe, ObserveStep: exporter.ObserveProvisioningStep}
	}
	controllerService := &controllercsi.Service{
		Client: client, Namespace: cfg.namespace, Operator: effects, Volumes: volumeRegistry,
		CapacityPools: volumeRegistry, CapacityProbe: capacityProbe, PoolLocks: poolLocks, ProvisioningGate: quiesceGate,
		Cleanups: cleanupStore, CleanupOperator: effects, CleanupAbsenceWait: cfg.cleanupAbsenceWait,
		PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter, ObserveStep: exporter.ObserveProvisioningStep, RetryRequests: capacityRetries,
	}
	identityService := &identity.Service{Version: version}

	errCh := make(chan error, 6)
	go func() { errCh <- quiesceGate.Run(ctx) }()
	go func() { errCh <- poolLifecycleReconciler.Run(ctx) }()
	go func() {
		errCh <- csiserver.ServeContext(ctx, cfg.endpoint, func(server *grpc.Server) {
			csi.RegisterIdentityServer(server, identityService)
			csi.RegisterControllerServer(server, controllerService)
		}, exporter.ServerOptions()...)
	}()

	certificateManager := newCertificateManager(cfg, client, quiesceGate)
	if err := wiring.BootstrapWithRetry(ctx, "mobility webhook certificate prerequisites", certificateManager.Bootstrap); err != nil {
		klog.Fatalf("bootstrap mobility webhook certificate: %v", err)
	}
	go func() { errCh <- certificateManager.Run(ctx) }()
	stopMobility := startMobility(ctx, cfg, mobilityComponents{
		client: client, dynamicClient: dynamicClient, volumes: volumeRegistry, operator: operator,
		poolLocks: poolLocks, cleanups: cleanupStore, exporter: exporter, errCh: errCh,
	})
	defer stopMobility()
	webhookServer := newWebhookServer(cfg, client, admissionClient, volumeRegistry, lifecycleChecker, certificateManager)
	go serveWebhook(webhookServer, cfg.webhookAddress, errCh)

	klog.Infof("starting ShiftPV controller %s", version)
	awaitShutdown(ctx, errCh, stop)
	shutdownWebhook(webhookServer)
}

// Retry observation has its own bounded API budget so status-event bursts
// cannot consume the CSI registry's client-side rate limiter.
func capacityRetryRESTConfig(config *rest.Config) *rest.Config {
	retryConfig := rest.CopyConfig(config)
	retryConfig.QPS, retryConfig.Burst = 5, 10
	retryConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(retryConfig.QPS, retryConfig.Burst)
	retryConfig.Timeout = 0 // Reconcile is bounded separately; watches may stay open.
	retryConfig.UserAgent = "shiftpv-capacity-retry"
	return retryConfig
}

// admissionRESTConfig is the higher-throughput configuration the lifecycle
// admission clients run with.
func admissionRESTConfig(restConfig *rest.Config) *rest.Config {
	admissionConfig := rest.CopyConfig(restConfig)
	admissionConfig.QPS = 50
	admissionConfig.Burst = 100
	return admissionConfig
}

// startMetrics starts the metrics endpoint and inventory observer when the
// operator configured an address and interval, and reports the exporter the
// rest of the controller observes through.
func startMetrics(ctx context.Context, cfg config, restConfig *rest.Config) *metrics.Exporter {
	if cfg.metricsAddress == "" || cfg.metricsInterval <= 0 {
		return nil
	}
	exporter := metrics.New("metadata")
	exporter.Start(ctx, cfg.metricsAddress)
	observer, metricsErr := exporter.NewController(restConfig, cfg.metricsInterval, cfg.poolReadinessStaleAfter)
	if metricsErr != nil {
		klog.Errorf("metrics inventory disabled: %v", metricsErr)
	} else {
		go observer.Run(ctx)
	}
	return exporter
}

// newHelperRunner builds the helper Pod operator with the operator-configured
// image, timeout and resource envelope.
func newHelperRunner(cfg config, client kubernetes.Interface, pools *volumeapi.Registry) *helperpod.Runner {
	return &helperpod.Runner{
		Client: client, Namespace: cfg.namespace, ServiceAccountName: cfg.helperServiceAccount, Pools: pools, Image: cfg.helperImage, Timeout: cfg.helperWait,
		PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: mustQuantity("helper CPU request", cfg.helperCPURequest), corev1.ResourceMemory: mustQuantity("helper memory request", cfg.helperMemoryRequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU: mustQuantity("helper CPU limit", cfg.helperCPULimit), corev1.ResourceMemory: mustQuantity("helper memory limit", cfg.helperMemoryLimit),
			},
		},
	}
}

// newLifecycleChecker builds the uninstall safety checker the Pool lifecycle
// reconciler and the delete-validating webhook share.
func newLifecycleChecker(cfg config, admissionClient kubernetes.Interface, admissionDynamicClient dynamic.Interface) *uninstallcheck.Checker {
	return &uninstallcheck.Checker{
		Client: admissionClient, Volumes: &volumeapi.Registry{Client: admissionDynamicClient, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}, Cleanups: &cleanupapi.Store{Client: admissionDynamicClient}, StorageClassNames: cfg.storageClassNames.Values("shiftpv"), Namespace: cfg.namespace,
		InventoryMaxAge: cfg.poolReadinessStaleAfter,
	}
}

// newCertificateManager builds the managed webhook serving certificate and
// configuration reconciler.
func newCertificateManager(cfg config, client kubernetes.Interface, validationGate *uninstallcheck.QuiesceGate) *webhookcertificate.Manager {
	return &webhookcertificate.Manager{Client: client, ValidationGate: validationGate, Config: webhookcertificate.Config{
		Namespace:                   cfg.namespace,
		SecretName:                  cfg.webhookSecret,
		ServiceName:                 cfg.webhookService,
		ConfigurationName:           cfg.webhookConfiguration,
		ValidationConfigurationName: cfg.validationConfiguration,
		OwnerCSIDriver:              admission.DriverName,
		AdmissionEnabled:            cfg.mobilityEnabled,
		Interval:                    time.Minute,
		ServingValidity:             90 * 24 * time.Hour,
		ServingRenewBefore:          30 * 24 * time.Hour,
		CAValidity:                  10 * 365 * 24 * time.Hour,
		CARenewBefore:               365 * 24 * time.Hour,
		Now:                         time.Now,
	}}
}

// mobilityComponents are the already-built collaborators the mobility
// reconciler runs on.
type mobilityComponents struct {
	client        kubernetes.Interface
	dynamicClient dynamic.Interface
	volumes       *volumeapi.Registry
	operator      *helperpod.Runner
	poolLocks     *poolcapacity.Locker
	cleanups      *cleanupapi.Store
	exporter      *metrics.Exporter
	errCh         chan error
}

// startMobility starts the automatic cordon mobility reconciler when it is
// enabled and reports the shutdown the caller must defer; when mobility is
// disabled nothing is started and the shutdown does nothing.
func startMobility(ctx context.Context, cfg config, components mobilityComponents) func() {
	if !cfg.mobilityEnabled {
		return func() {}
	}
	eventScheme := runtime.NewScheme()
	if err := corev1.AddToScheme(eventScheme); err != nil {
		klog.Fatalf("register Kubernetes Event scheme: %v", err)
	}
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: components.client.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(eventScheme, corev1.EventSource{Component: "shiftpv-mobility-controller"})
	wake := mobilitycontroller.WatchEvents(ctx, components.client, components.dynamicClient, cfg.namespace)
	reconciler := &mobilitycontroller.Reconciler{Client: components.client, Repository: components.volumes, CapacityProbe: components.operator, PoolLocks: components.poolLocks, Namespace: cfg.namespace, HelperImage: cfg.mobilityImage, ServiceAccountName: cfg.helperServiceAccount, Cleanups: components.cleanups, CleanupOperator: components.operator, Interval: cfg.mobilityInterval, MoveJournalRetention: cfg.moveJournalRetention, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter, Recorder: eventRecorder, Wake: wake}
	if components.exporter != nil {
		components.exporter.ObserveDiscovery(nil, errors.New("discovery not observed yet"))
		reconciler.ObserveDiscovery = components.exporter.ObserveDiscovery
	}
	go func() { components.errCh <- reconciler.Run(ctx) }()
	return eventBroadcaster.Shutdown
}

// newWebhookServer builds the admission HTTPS server serving the mobility
// mutation, the lifecycle delete validation and the health endpoint.
func newWebhookServer(cfg config, client kubernetes.Interface, admissionClient kubernetes.Interface, volumes *volumeapi.Registry,
	lifecycleChecker *uninstallcheck.Checker, certificateManager *webhookcertificate.Manager) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/mutate", &admission.Handler{Client: client, Volumes: volumes})
	mux.Handle("/validate-delete", &lifecycleadmission.Handler{Checker: lifecycleChecker,
		Permit:            &uninstallcheck.PermitStore{Client: admissionClient, Namespace: cfg.namespace, Name: cfg.uninstallPermit, CSIDriver: admission.DriverName},
		TrustedController: "system:serviceaccount:" + cfg.namespace + ":" + cfg.controllerServiceAccount})
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	return &http.Server{Addr: cfg.webhookAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, TLSConfig: certificateManager.TLSConfig()}
}

// serveWebhook serves the admission endpoint until it is shut down, reporting a
// clean shutdown as no failure.
func serveWebhook(webhookServer *http.Server, address string, errCh chan error) {
	listener, err := net.Listen("tcp", address)
	if err == nil {
		err = webhookServer.Serve(tls.NewListener(listener, webhookServer.TLSConfig))
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	errCh <- err
}

// awaitShutdown blocks until a signal arrives or the first component stops,
// stopping the remaining components on a component failure.
func awaitShutdown(ctx context.Context, errCh chan error, stop context.CancelFunc) {
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			klog.Errorf("ShiftPV controller component stopped: %v", err)
		}
		stop()
	}
}

// shutdownWebhook drains the admission server on the shared shutdown budget.
func shutdownWebhook(webhookServer *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := webhookServer.Shutdown(shutdownCtx); err != nil {
		klog.Errorf("shut down mobility webhook: %v", err)
	}
}

func fatal(step string, err error) { klog.Fatalf("%s: %v", step, err) }

func mustQuantity(name, value string) resource.Quantity {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		klog.Fatalf("parse %s %q: %v", name, value, err)
	}
	return quantity
}
