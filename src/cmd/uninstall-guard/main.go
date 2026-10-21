package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/flagvalue"
	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
)

func main() {
	var storageClassNames, preserveRetiredPools flagvalue.Names
	flag.Var(&storageClassNames, "storage-class", "repeatable ShiftPV StorageClass name")
	flag.Var(&preserveRetiredPools, "preserve-retired-pool", "repeatable exact retired Pool UID to retain with protection and storage unchanged")
	permitNamespace := flag.String("permit-namespace", os.Getenv("POD_NAMESPACE"), "namespace for the uninstall permit ConfigMap")
	permitName := flag.String("permit-name", "shiftpv-uninstall-permit", "uninstall permit ConfigMap name")
	validationWebhook := flag.String("validation-webhook", "shiftpv-lifecycle", "lifecycle ValidatingWebhookConfiguration name")
	timeout := flag.Duration("timeout", 90*time.Second, "maximum Kubernetes inspection time")
	retry := flag.Bool("retry", false, "keep retrying quiesced inspection until teardown is safe")
	retryInterval := flag.Duration("retry-interval", 5*time.Second, "delay between retry attempts")
	poolReadinessStaleAfter := flag.Duration("pool-readiness-stale-after", volumeapi.DefaultPoolReadinessStaleAfter, "maximum age of a successful node Pool readiness probe")
	flag.Parse()
	if *poolReadinessStaleAfter <= 0 {
		deny("parse flags", fmt.Errorf("pool readiness stale duration must be positive"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	clients := wiring.InCluster(deny)

	checker := &uninstallcheck.Checker{
		Client:                  clients.Typed,
		Volumes:                 &volumeapi.Registry{Client: clients.Dynamic, PoolReadinessStaleAfter: *poolReadinessStaleAfter},
		Cleanups:                &cleanupapi.Store{Client: clients.Dynamic},
		StorageClassNames:       storageClassNames.Values("shiftpv"),
		Namespace:               *permitNamespace,
		InventoryMaxAge:         *poolReadinessStaleAfter,
		PreserveRetiredPoolUIDs: preserveRetiredPools.Values(),
	}
	permit := &uninstallcheck.PermitStore{Client: clients.Typed, Namespace: *permitNamespace, Name: *permitName, CSIDriver: uninstallcheck.DriverName}
	var runErr error
	if *retry {
		runErr = runWithRetry(ctx, checker, permit, *validationWebhook, *timeout, *retryInterval)
	} else {
		attemptCtx, cancel := context.WithTimeout(ctx, *timeout)
		runErr = run(attemptCtx, checker, permit, *validationWebhook)
		cancel()
	}
	if runErr != nil {
		deny("quiesce and inspect ShiftPV dependencies", runErr)
	}
	fmt.Println("ShiftPV uninstall allowed: provisioning is quiesced and storage dependencies are settled")
	if len(checker.PreserveRetiredPoolUIDs) > 0 {
		fmt.Printf("Retained retired Pool UIDs: %v; CRs, protection finalizers, paths, markers, and storage remain. Physical absence was not established for these Pools.\n", checker.PreserveRetiredPoolUIDs)
	}
}

func runWithRetry(ctx context.Context, checker *uninstallcheck.Checker, permit *uninstallcheck.PermitStore, validationWebhook string, attemptTimeout, retryInterval time.Duration) error {
	if attemptTimeout <= 0 {
		return fmt.Errorf("attempt timeout must be positive")
	}
	if retryInterval <= 0 {
		return fmt.Errorf("retry interval must be positive")
	}
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := run(attemptCtx, checker, permit, validationWebhook)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		fmt.Fprintf(os.Stderr, "ShiftPV uninstall is waiting for a safe state: %v\n", err)

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("retry interrupted: %w; last attempt: %v", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func run(ctx context.Context, checker *uninstallcheck.Checker, permit *uninstallcheck.PermitStore, validationWebhook string) (resultErr error) {
	attempt, err := permit.BeginQuiesce(ctx)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := permit.Cancel(rollbackCtx, attempt); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("cancel uninstall quiesce: %w", err))
		}
	}()

	if err := permit.WaitForQuiesced(ctx, attempt); err != nil {
		return fmt.Errorf("wait for controller quiesce: %w", err)
	}
	inventoryAfter := time.Now().UTC()
	report, err := checkAfterQuiesce(ctx, checker, inventoryAfter)
	if err != nil {
		return fmt.Errorf("inspect ShiftPV dependencies: %w", err)
	}
	if !report.Safe() {
		return fmt.Errorf("dependent storage resources still exist:\n%s", formatBlockers(report))
	}
	if err := checker.ReleasePoolProtectionAfter(ctx, inventoryAfter); err != nil {
		return fmt.Errorf("release Pool deletion protection: %w", err)
	}
	if err := permit.DisableValidation(ctx, validationWebhook); err != nil {
		return fmt.Errorf("disable lifecycle validation for teardown: %w", err)
	}
	if err := permit.Grant(ctx, attempt); err != nil {
		return fmt.Errorf("grant protected resource deletion: %w", err)
	}
	completed = true
	return nil
}

func checkAfterQuiesce(ctx context.Context, checker *uninstallcheck.Checker, inventoryAfter time.Time) (uninstallcheck.Report, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var lastObserved uninstallcheck.Report
	for {
		report, err := checker.CheckAfter(ctx, inventoryAfter)
		if err != nil {
			return report, withLastObservedBlockers(err, lastObserved)
		}
		if report.Safe() || !report.WaitingForInventory() {
			return report, nil
		}
		lastObserved = report
		select {
		case <-ctx.Done():
			return uninstallcheck.Report{}, withLastObservedBlockers(ctx.Err(), lastObserved)
		case <-ticker.C:
		}
	}
}

func withLastObservedBlockers(err error, report uninstallcheck.Report) error {
	if report.Safe() {
		return err
	}
	return fmt.Errorf("%w; last observed blockers:\n%s", err, formatBlockers(report))
}

func formatBlockers(report uninstallcheck.Report) string {
	message := ""
	for _, blocker := range report.Blockers {
		name := blocker.Name
		if blocker.Namespace != "" {
			name = blocker.Namespace + "/" + name
		}
		message += fmt.Sprintf("- %s %s: %s\n", blocker.Kind, name, blocker.Reason)
	}
	return message + "Migrate or remove every dependency before uninstalling. Emergency bypass requires deleting the lifecycle ValidatingWebhookConfiguration before helm uninstall --no-hooks."
}

func deny(operation string, err error) {
	fmt.Fprintf(os.Stderr, "ShiftPV uninstall denied: %s: %v\n", operation, err)
	os.Exit(1)
}
