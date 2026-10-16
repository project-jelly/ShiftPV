package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/lifecycle/upgrade"
)

type upgradeCheckClients func() (kubernetes.Interface, dynamic.Interface, error)

// runUpgradeCheck inspects before the controller, webhooks, or uninstall permit
// are initialized. Its result is a snapshot, not permission to discard journals.
func runUpgradeCheck(ctx context.Context, args []string, output io.Writer, load upgradeCheckClients) error {
	flags := flag.NewFlagSet("upgrade-check", flag.ContinueOnError)
	flags.SetOutput(output)
	namespace := flags.String("namespace", os.Getenv("POD_NAMESPACE"), "ShiftPV system namespace")
	daemonSet := flags.String("node-daemonset-name", "shiftpv-node", "trusted resident Node DaemonSet")
	timeout := flags.Duration("timeout", 90*time.Second, "maximum read-only inspection time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *namespace == "" || *daemonSet == "" || *timeout <= 0 {
		return fmt.Errorf("namespace and Node DaemonSet are required; timeout must be positive; positional arguments are not accepted")
	}
	typed, dynamicClient, err := load()
	if err != nil {
		return fmt.Errorf("initialize read-only inspection clients: %w", err)
	}
	inspectCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	checker := &upgrade.Checker{
		Client: typed, Volumes: &volumeapi.Registry{Client: dynamicClient},
		Cleanups: &cleanupapi.Store{Client: dynamicClient}, Namespace: *namespace, NodeDaemonSet: *daemonSet,
	}
	report, err := checker.Check(inspectCtx)
	if err != nil {
		return fmt.Errorf("inspect transition dependencies: %w", err)
	}
	if err := json.NewEncoder(output).Encode(report); err != nil {
		return fmt.Errorf("write inspection report: %w", err)
	}
	if !report.Safe() {
		return fmt.Errorf("unsettled work or unsupported Node executors remain; keep the current controller running and settle the reported dependencies")
	}
	return nil
}

func loadUpgradeCheckClients() (kubernetes.Interface, dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, err
	}
	config.UserAgent = "shiftpv-upgrade-check"
	typed, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	return typed, dynamicClient, err
}
