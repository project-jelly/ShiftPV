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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
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
		return fmt.Errorf("usage: shiftpv-volume-helper create|serve-source|copy|promote|verify-owner|cleanup|statfs|usage [flags]")
	}
	switch os.Args[1] {
	case "create":
		return runCreate(os.Args[2:])
	case "cleanup":
		return runCleanup(os.Args[2:])
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

// helperPoolReadinessFlag registers the probe staleness budget a node-bound
// helper judges Pool readiness with. Cleanup publication proof and measurement
// helpers consume it. Move effect helpers recheck authority through
// PoolForIdentity and accept the flag for CLI compatibility. The name and
// default are shared with the controller so parent and child agree.
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
	copyArguments := append(append([]string{}, common...), "--fsync", source, target+"/")
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

func runCreate(arguments []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	var identity volume.CopyIdentity
	operationID := flags.String("operation-id", "", "volume creation operation identity")
	flags.StringVar(&identity.InstallationID, "installation-id", "", "Kubernetes cluster identity")
	flags.StringVar(&identity.PoolName, "pool-name", "", "ShiftPVPool name")
	flags.StringVar(&identity.PoolUID, "pool-uid", "", "ShiftPVPool UID")
	flags.StringVar(&identity.VolumeID, "volume-id", "", "ShiftPV volume ID")
	flags.StringVar(&identity.VolumeUID, "volume-uid", "", "ShiftPVVolume UID")
	flags.StringVar(&identity.CopyID, "copy-id", "", "copy identity")
	flags.StringVar(&identity.NodeName, "node-name", "", "Kubernetes node name")
	root := flags.String("pool-root", "/pool", "mounted Pool root")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	identity.Role = volume.RoleServing
	if err := identity.Validate(); err != nil {
		return err
	}
	if !volume.ValidIdentityToken(*operationID) {
		return fmt.Errorf("volume creation operation identity is invalid")
	}
	expectedOperationID, err := volumeapi.CreationOperationID(identity.VolumeUID)
	if err != nil || *operationID != expectedOperationID {
		return fmt.Errorf("volume creation operation identity does not match the volume UID")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster configuration: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	registry := &volumeapi.Registry{Client: client}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	authority := func(checkCtx context.Context) error {
		installationID, err := registry.InstallationID(checkCtx)
		if err != nil {
			return fmt.Errorf("read installation identity: %w", err)
		}
		if installationID != identity.InstallationID {
			return fmt.Errorf("installation authority changed: %w", volumeapi.ErrStateConflict)
		}
		pool, err := registry.PoolForIdentity(checkCtx, identity.PoolName, identity.PoolUID, identity.NodeName)
		if err != nil {
			return fmt.Errorf("read Pool: %w", err)
		}
		if pool.Name != identity.PoolName || pool.UID != identity.PoolUID {
			return fmt.Errorf("Pool authority changed: %w", volumeapi.ErrStateConflict)
		}
		state, err := registry.Get(checkCtx, identity.VolumeID)
		if err != nil {
			return fmt.Errorf("read volume state: %w", err)
		}
		if state.UID != identity.VolumeUID || state.CurrentCopy == nil || *state.CurrentCopy != identity ||
			state.CreationOperationID != *operationID ||
			(state.Phase != volumeapi.PhasePending && state.Phase != volumeapi.PhaseReady) {
			return fmt.Errorf("volume creation authority changed: %w", volumeapi.ErrStateConflict)
		}
		return nil
	}
	authority = withMountAuthority(registry, *root, identity, authority)
	return ownership.PrepareServing(ctx, *root, identity, authority)
}

type cleanupOptions struct {
	authority                             cleanupapi.Authority
	operationID, namespace, podName, root string
	// poolReadinessStaleAfter mirrors the parent controller's budget so the
	// move source publication proof in helperauth resolves Pool readiness with
	// the operator's value instead of the compiled default.
	poolReadinessStaleAfter time.Duration
}

func parseCleanupOptions(arguments []string) (cleanupOptions, error) {
	flags := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	authorityKind := flags.String("authority-kind", "", "cleanup parent kind")
	authorityName := flags.String("authority-name", "", "cleanup parent name")
	authorityUID := flags.String("authority-uid", "", "cleanup parent UID")
	var options cleanupOptions
	flags.StringVar(&options.operationID, "operation-id", "", "cleanup operation identity")
	flags.StringVar(&options.namespace, "namespace", "", "helper Pod namespace")
	flags.StringVar(&options.root, "pool-root", "/pool", "mounted Pool root")
	helperPoolReadinessFlag(flags, &options.poolReadinessStaleAfter)
	if err := flags.Parse(arguments); err != nil {
		return cleanupOptions{}, err
	}
	options.podName = os.Getenv("POD_NAME")
	options.authority = cleanupapi.Authority{Kind: *authorityKind, Name: *authorityName, UID: *authorityUID}
	if options.authority.Validate() != nil || !volume.ValidIdentityToken(options.operationID) || options.namespace == "" || options.podName == "" {
		return cleanupOptions{}, fmt.Errorf("cleanup helper identity is incomplete")
	}
	return options, nil
}

// cleanupExecutor is the authorized executor binding a cleanup helper proved
// before it is allowed to reclaim anything: the durable parent intent it read,
// the controlling Job, and the exact Pod the intent is bound to.
type cleanupExecutor struct {
	approved        cleanupapi.Cleanup
	jobName, jobUID string
	pod             *corev1.Pod
}

func runCleanup(arguments []string) error {
	options, err := parseCleanupOptions(arguments)
	if err != nil {
		return err
	}
	dynamicClient, client, err := inClusterClients()
	if err != nil {
		return err
	}
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.poolReadinessStaleAfter}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	executor, err := authorizeCleanupExecutor(ctx, client, cleanups, options)
	if err != nil {
		return err
	}
	approved := executor.approved
	baseAuthority := helperauth.CleanupAuthority(cleanups, registry, helperauth.CleanupOptions{
		Authority: options.authority, Approved: approved, JobName: executor.jobName, JobUID: executor.jobUID, Pod: executor.pod,
	})
	authority := func(checkCtx context.Context, effectStarted bool) error {
		return withMountAuthority(registry, options.root, approved.Spec.Target, func(ctx context.Context) error {
			return baseAuthority(ctx, effectStarted)
		})(checkCtx)
	}
	if err := authority(ctx, false); err != nil {
		return err
	}
	localReceipt, digest, err := ownership.ReclaimWithResume(ctx, options.root, approved.Spec.Target, approved.Spec.OperationID, authority)
	if err != nil {
		return err
	}
	return recordCleanupReceipt(ctx, cleanups, executor, localReceipt, digest)
}

// authorizeCleanupExecutor reads the durable cleanup intent and proves this
// process is the exact executor it approved, in the order the checks must run:
// intent identity, executor Pod, controlling Job ownership, and the Pod binding
// the intent carries.
func authorizeCleanupExecutor(ctx context.Context, client kubernetes.Interface, cleanups *cleanupapi.Store, options cleanupOptions) (cleanupExecutor, error) {
	authorityIdentity, podName := options.authority, options.podName
	approved, err := cleanups.Get(ctx, authorityIdentity)
	if err != nil {
		return cleanupExecutor{}, fmt.Errorf("read cleanup intent: %w", err)
	}
	if intentIdentityChanged(approved, authorityIdentity, options.operationID) {
		return cleanupExecutor{}, fmt.Errorf("cleanup intent identity changed: %w", cleanupapi.ErrConflict)
	}
	pod, err := client.CoreV1().Pods(options.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return cleanupExecutor{}, fmt.Errorf("read cleanup executor Pod: %w", err)
	}
	jobName, jobUID := controllingJobReference(pod.OwnerReferences)
	if executorUnauthorized(approved, pod, jobUID, jobName) {
		return cleanupExecutor{}, fmt.Errorf("cleanup executor is not authorized")
	}
	if err := verifyCleanupExecutorJob(ctx, client, options.namespace, jobName, jobUID, authorityIdentity); err != nil {
		return cleanupExecutor{}, err
	}
	if pod.UID == "" {
		return cleanupExecutor{}, fmt.Errorf("cleanup executor Pod has no UID")
	}
	approved, err = bindCleanupExecutorPod(ctx, cleanups, approved, pod, authorityIdentity)
	if err != nil {
		return cleanupExecutor{}, err
	}
	if !helperauth.MatchesCleanupExecutor(approved.Status.Executor, jobName, jobUID, pod) {
		return cleanupExecutor{}, fmt.Errorf("cleanup executor does not match the exact running Pod")
	}
	return cleanupExecutor{approved: approved, jobName: jobName, jobUID: jobUID, pod: pod}, nil
}

// intentIdentityChanged reports whether the durable intent this helper read is
// no longer the exact intent its arguments name.
func intentIdentityChanged(approved cleanupapi.Cleanup, authorityIdentity cleanupapi.Authority, operationID string) bool {
	return approved.UID != authorityIdentity.UID || approved.Spec.Authority != authorityIdentity || approved.Spec.OperationID != operationID
}

// executorUnauthorized reports whether the running Pod and its controlling Job
// fail to match the executor the approved intent is running.
func executorUnauthorized(approved cleanupapi.Cleanup, pod *corev1.Pod, jobUID, jobName string) bool {
	return jobUID == "" || approved.Status.Phase != cleanupapi.PhaseRunning || approved.Status.Executor == nil ||
		approved.Status.Executor.JobName != jobName || approved.Status.Executor.JobUID != jobUID ||
		approved.Status.Executor.NodeName != approved.Spec.Target.NodeName || pod.Spec.NodeName != approved.Spec.Target.NodeName
}

// jobNotOwned reports whether the live Job is not the exact undeleted Job the
// cleanup parent owns.
func jobNotOwned(job *metav1.ObjectMeta, jobUID string, authorityIdentity cleanupapi.Authority) bool {
	return string(job.UID) != jobUID || job.DeletionTimestamp != nil || !helperauth.OwnedByCleanupParent(job.OwnerReferences, authorityIdentity)
}

// rebindsPod reports whether the approved executor still names an earlier Pod
// than the one running this cleanup.
func rebindsPod(approved cleanupapi.Cleanup, pod *corev1.Pod) bool {
	executor := *approved.Status.Executor
	return executor.PodUID != string(pod.UID)
}

// controllingJobReference names the Job that controls the executor Pod, or
// empty strings when no controlling Job owns it.
func controllingJobReference(references []metav1.OwnerReference) (string, string) {
	for _, owner := range references {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "Job" {
			return owner.Name, string(owner.UID)
		}
	}
	return "", ""
}

// verifyCleanupExecutorJob rechecks that the live controlling Job is the exact
// undeleted Job the cleanup parent owns.
func verifyCleanupExecutorJob(ctx context.Context, client kubernetes.Interface, namespace, jobName, jobUID string, authorityIdentity cleanupapi.Authority) error {
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read cleanup executor Job: %w", err)
	}
	if jobNotOwned(&job.ObjectMeta, jobUID, authorityIdentity) {
		return fmt.Errorf("cleanup Job is not owned by the exact parent: %w", volumeapi.ErrStateConflict)
	}
	return nil
}

// bindCleanupExecutorPod rebinds the durable intent to this retry Pod when the
// approved executor still names an earlier one, and returns the intent the rest
// of the cleanup must act on.
func bindCleanupExecutorPod(ctx context.Context, cleanups *cleanupapi.Store, approved cleanupapi.Cleanup, pod *corev1.Pod, authorityIdentity cleanupapi.Authority) (cleanupapi.Cleanup, error) {
	if !rebindsPod(approved, pod) {
		return approved, nil
	}
	executor := *approved.Status.Executor
	executor.PodUID = string(pod.UID)
	if err := cleanups.UpdateStatus(ctx, approved, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &executor}); err != nil {
		return cleanupapi.Cleanup{}, fmt.Errorf("bind cleanup executor retry Pod: %w", err)
	}
	bound, err := cleanups.Get(ctx, authorityIdentity)
	if err != nil {
		return cleanupapi.Cleanup{}, fmt.Errorf("read Pod-bound cleanup intent: %w", err)
	}
	return bound, nil
}

// recordCleanupReceipt publishes the local reclaim receipt for the parent
// controller to verify.
func recordCleanupReceipt(ctx context.Context, cleanups *cleanupapi.Store, executor cleanupExecutor, localReceipt ownership.Receipt, digest string) error {
	approved := executor.approved
	now := time.Now().UTC().Format(time.RFC3339Nano)
	receipt := &cleanupapi.Receipt{
		OperationID: approved.Spec.OperationID, ExecutorUID: executor.jobUID, ObservedAt: now,
		Retired: localReceipt.Retired, Purged: localReceipt.Purged, LocalReceiptDigest: digest,
	}
	return cleanups.UpdateStatus(ctx, approved, cleanupapi.Status{
		Phase: cleanupapi.PhaseVerifying, Executor: approved.Status.Executor, Receipt: receipt,
	})
}
