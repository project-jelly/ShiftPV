package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/project-jelly/ShiftPV/src/kubernetes/helperauth"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: shiftpv-volume-helper serve-source|copy|promote|verify-owner|statfs|usage [flags]")
	}
	switch os.Args[1] {
	case "copy":
		return runMoveCopy(os.Args[2:])
	case "serve-source":
		return runServeSource(os.Args[2:])
	case "promote":
		return runMovePromote(os.Args[2:])
	case "verify-owner":
		return runVerifyOwner(os.Args[2:])
	case "statfs", "usage":
		return runMeasurement(os.Args[1], os.Args[2:])
	default:
		return fmt.Errorf("unknown volume helper action %q", os.Args[1])
	}
}

func runServeSource(arguments []string) error {
	options, err := parseMoveOptions("serve-source", arguments, false)
	if err != nil {
		return err
	}
	dynamicClient, client, err := inClusterClients()
	if err != nil {
		return err
	}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
	move, err := registry.GetMove(context.Background(), options.MoveName)
	if err != nil {
		return fmt.Errorf("read source service Move: %w", err)
	}
	if move.UID != options.MoveUID || move.Status.SourceCopy == nil || move.Status.CopyOperationID != options.OperationID {
		return fmt.Errorf("source service intent identity changed: %w", volumeapi.ErrStateConflict)
	}
	identity := *move.Status.SourceCopy
	authority := withMountAuthority(registry, options.root, identity, helperauth.SourceAuthority(client, registry, options.MoveOptions, identity))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := authority(ctx); err != nil {
		return err
	}
	if err := ownership.VerifyServingPath(options.root, identity); err != nil {
		return fmt.Errorf("verify source copy: %w", err)
	}
	if err := authority(ctx); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "rsync", "--daemon", "--no-detach", "--config=/config/rsyncd.conf")
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}

// Measurement uses the controller's readiness budget.
// Move effects accept this flag for CLI compatibility and check exact authority.
func helperPoolReadinessFlag(flags *flag.FlagSet, staleAfter *time.Duration) {
	flags.DurationVar(staleAfter, volumeapi.PoolReadinessStaleAfterFlag,
		volumeapi.DefaultPoolReadinessStaleAfter, "maximum age of a successful node Pool readiness probe")
}

type moveOptions struct {
	helperauth.MoveOptions
	sourceService, passwordFile, root string
	// poolReadinessStaleAfter is accepted for forward compatibility. No move
	// subcommand judges probe freshness today: each rechecks authority through
	// PoolForIdentity, which never consults the budget.
	poolReadinessStaleAfter time.Duration
}

func parseMoveOptions(action string, arguments []string, copyAction bool) (moveOptions, error) {
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	var options moveOptions
	flags.StringVar(&options.MoveName, "move-name", "", "ShiftPVMove name")
	flags.StringVar(&options.MoveUID, "move-uid", "", "ShiftPVMove UID")
	flags.StringVar(&options.OperationID, "operation-id", "", "move operation identity")
	flags.StringVar(&options.Namespace, "namespace", "", "helper Pod namespace")
	flags.StringVar(&options.root, "pool-root", "/pool", "mounted Pool root")
	helperPoolReadinessFlag(flags, &options.poolReadinessStaleAfter)
	if copyAction {
		flags.StringVar(&options.sourceService, "source-service", "", "rsync source Service")
		flags.StringVar(&options.passwordFile, "password-file", "", "rsync password file")
	}
	if err := flags.Parse(arguments); err != nil {
		return moveOptions{}, err
	}
	options.PodName = os.Getenv("POD_NAME")
	if !volume.ValidObjectName(options.MoveName) || !volume.ValidIdentityToken(options.MoveUID) || !volume.ValidIdentityToken(options.OperationID) || options.Namespace == "" || options.PodName == "" {
		return moveOptions{}, fmt.Errorf("move helper identity is incomplete")
	}
	if copyAction && (len(utilvalidation.IsDNS1123Label(options.sourceService)) != 0 || options.passwordFile == "") {
		return moveOptions{}, fmt.Errorf("copy source is invalid")
	}
	return options, nil
}

func runMoveCopy(arguments []string) error {
	options, err := parseMoveOptions("copy", arguments, true)
	if err != nil {
		return err
	}
	dynamicClient, client, err := inClusterClients()
	if err != nil {
		return err
	}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
	move, err := registry.GetMove(context.Background(), options.MoveName)
	if err != nil {
		return fmt.Errorf("read copy Move: %w", err)
	}
	if move.UID != options.MoveUID || move.Status.IncomingCopy == nil || move.Status.CopyOperationID != options.OperationID {
		return fmt.Errorf("copy intent identity changed: %w", volumeapi.ErrStateConflict)
	}
	identity := *move.Status.IncomingCopy
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	authority := withMountAuthority(registry, options.root, identity, helperauth.MoveAuthority(client, registry, options.MoveOptions, "copy", identity))
	if err := authority(ctx); err != nil {
		return err
	}
	return ownership.PopulateIncoming(ctx, options.root, identity, options.OperationID, authority, func(copyCtx context.Context, target string) error {
		password, err := os.ReadFile(options.passwordFile)
		if err != nil {
			return fmt.Errorf("read rsync password: %w", err)
		}
		environment := append(os.Environ(), "RSYNC_PASSWORD="+strings.TrimSpace(string(password)))
		source := "rsync://shiftpv@" + options.sourceService + "/data/"
		copyArguments, verifyArguments := moveCopyArguments(source, target)
		copyCommand := exec.CommandContext(copyCtx, "rsync", copyArguments...)
		copyCommand.Env = environment
		if output, commandErr := copyCommand.CombinedOutput(); commandErr != nil {
			return fmt.Errorf("rsync copy: %w: %s", commandErr, strings.TrimSpace(string(output)))
		}
		verifyCommand := exec.CommandContext(copyCtx, "rsync", verifyArguments...)
		verifyCommand.Env = environment
		output, commandErr := verifyCommand.CombinedOutput()
		if commandErr != nil {
			return fmt.Errorf("rsync checksum validation: %w: %s", commandErr, strings.TrimSpace(string(output)))
		}
		if difference := strings.TrimSpace(string(output)); difference != "" {
			return fmt.Errorf("rsync checksum validation reported differences: %s", difference)
		}
		return nil
	})
}

func moveCopyArguments(source, target string) ([]string, []string) {
	common := []string{
		"-aHAXS",
		"--numeric-ids",
		"--one-file-system",
		"--no-devices",
		"--delete",
	}
	copyArguments := append(append([]string{}, common...), "--fsync", "--compress", "--compress-choice=zstd", "--compress-level=1", source, target+"/")
	verifyArguments := append(append([]string{}, common...), "--checksum", "--dry-run", "--itemize-changes", source, target+"/")
	return copyArguments, verifyArguments
}

func runMovePromote(arguments []string) error {
	options, err := parseMoveOptions("promote", arguments, false)
	if err != nil {
		return err
	}
	dynamicClient, client, err := inClusterClients()
	if err != nil {
		return err
	}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
	move, err := registry.GetMove(context.Background(), options.MoveName)
	if err != nil {
		return fmt.Errorf("read promotion Move: %w", err)
	}
	if move.UID != options.MoveUID || move.Status.IncomingCopy == nil || move.Status.DestinationCopy == nil || move.Status.PromotionOperationID != options.OperationID {
		return fmt.Errorf("promotion intent identity changed: %w", volumeapi.ErrStateConflict)
	}
	incoming, destination := *move.Status.IncomingCopy, *move.Status.DestinationCopy
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	authority := withMountAuthority(registry, options.root, incoming, helperauth.MoveAuthority(client, registry, options.MoveOptions, "promote", incoming))
	if err := authority(ctx); err != nil {
		return err
	}
	return ownership.PromoteIncoming(ctx, options.root, incoming, destination, options.OperationID, authority)
}

func runVerifyOwner(arguments []string) error {
	options, err := parseMoveOptions("verify-owner", arguments, false)
	if err != nil {
		return err
	}
	dynamicClient, client, err := inClusterClients()
	if err != nil {
		return err
	}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
	move, err := registry.GetMove(context.Background(), options.MoveName)
	if err != nil {
		return fmt.Errorf("read recovery Move: %w", err)
	}
	if move.UID != options.MoveUID {
		return fmt.Errorf("recovery intent identity changed: %w", volumeapi.ErrStateConflict)
	}
	state, err := registry.Get(context.Background(), move.Spec.VolumeID)
	if err != nil {
		return fmt.Errorf("read recovery owner volume state: %w", err)
	}
	if state.CurrentCopy == nil {
		return fmt.Errorf("recovery owner identity is unavailable: %w", volumeapi.ErrStateConflict)
	}
	identity := *state.CurrentCopy
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	authority := withMountAuthority(registry, options.root, identity, helperauth.RecoveryAuthority(client, registry, options.MoveOptions, identity))
	if err := authority(ctx); err != nil {
		return err
	}
	return ownership.WithExistingLock(ctx, options.root, ownership.PoolIdentity{InstallationID: identity.InstallationID, PoolUID: identity.PoolUID}, identity.VolumeID, func(store *ownership.Store) error {
		if err := authority(ctx); err != nil {
			return err
		}
		if err := store.VerifyServing(identity); err != nil {
			return err
		}
		return authority(ctx)
	})
}

func inClusterClients() (dynamic.Interface, kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("load in-cluster configuration: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return dynamicClient, client, nil
}

// withMountAuthority checks the helper's bound Pool root against the node's
// recorded mount identity. It runs before storage is opened and on each live
// authority recheck made by the operation.
func withMountAuthority(registry *volumeapi.Registry, root string, identity volume.CopyIdentity, authority func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := authority(ctx); err != nil {
			return err
		}
		pool, err := registry.PoolForIdentity(ctx, identity.PoolName, identity.PoolUID, identity.NodeName)
		if err != nil {
			return fmt.Errorf("read Pool mount identity: %w", err)
		}
		if pool.Name != identity.PoolName || pool.UID != identity.PoolUID {
			return fmt.Errorf("Pool mount authority changed: %w", volumeapi.ErrStateConflict)
		}
		return readiness.VerifyMountedPath(root, pool)
	}
}
