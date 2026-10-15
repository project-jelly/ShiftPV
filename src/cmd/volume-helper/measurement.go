package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type measurementOptions struct {
	pool                     volumeapi.Pool
	root, evidence, volumeID string
	copy                     *volume.CopyIdentity
	staleAfter               time.Duration
	now                      func() time.Time
	wait                     func(context.Context) error
}

type measurementRegistry interface {
	PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	InventoryPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	Get(context.Context, string) (volumeapi.State, error)
	InstallationID(context.Context) (string, error)
}

func parseMeasurementOptions(action string, arguments []string) (measurementOptions, error) {
	flags := flag.NewFlagSet(action, flag.ContinueOnError)
	var options measurementOptions
	var encodedCopy string
	flags.StringVar(&options.pool.Name, "pool-name", "", "exact Pool name")
	flags.StringVar(&options.pool.UID, "pool-uid", "", "exact Pool UID")
	flags.StringVar(&options.pool.NodeName, "node-name", "", "Pool node")
	flags.StringVar(&options.root, "pool-root", "/pool", "bound Pool root")
	flags.StringVar(&options.evidence, "pool-evidence", "", "expected registration and filesystem evidence digest")
	flags.StringVar(&options.volumeID, "volume-id", "", "quiesced volume to measure")
	flags.StringVar(&encodedCopy, "copy-identity", "", "expected serving copy identity")
	helperPoolReadinessFlag(flags, &options.staleAfter)
	if err := flags.Parse(arguments); err != nil {
		return measurementOptions{}, err
	}
	if err := options.validate(action, encodedCopy); err != nil {
		return measurementOptions{}, err
	}
	return options, nil
}

func (o *measurementOptions) validate(action, encodedCopy string) error {
	digest, err := hex.DecodeString(o.evidence)
	if err != nil || len(digest) != 32 || o.evidence != strings.ToLower(o.evidence) ||
		!volume.ValidObjectName(o.pool.Name) || !volume.ValidIdentityToken(o.pool.UID) || !volume.ValidObjectName(o.pool.NodeName) ||
		!filepath.IsAbs(o.root) || filepath.Clean(o.root) == "/" {
		return fmt.Errorf("measurement Pool identity is incomplete")
	}
	if action == "statfs" {
		if o.volumeID != "" || encodedCopy != "" {
			return fmt.Errorf("statfs must not name a volume copy")
		}
		return nil
	}
	if action != "usage" {
		return fmt.Errorf("unknown measurement action")
	}
	if _, err := volume.Path(o.root, o.volumeID); err != nil {
		return err
	}
	return o.parseCopy(encodedCopy)
}

func (o *measurementOptions) parseCopy(encodedCopy string) error {
	if encodedCopy != "" {
		o.copy = &volume.CopyIdentity{}
		if err := json.Unmarshal([]byte(encodedCopy), o.copy); err != nil {
			return fmt.Errorf("decode measurement copy: %w", err)
		}
		if o.copy.Validate() != nil || o.copy.Role != volume.RoleServing || o.copy.VolumeID != o.volumeID ||
			o.copy.PoolName != o.pool.Name || o.copy.PoolUID != o.pool.UID || o.copy.NodeName != o.pool.NodeName {
			return fmt.Errorf("measurement copy does not match the requested Pool")
		}
	}
	return nil
}

func runMeasurement(action string, arguments []string) error {
	options, err := parseMeasurementOptions(action, arguments)
	if err != nil {
		return err
	}
	dynamicClient, _, err := inClusterClients()
	if err != nil {
		return err
	}
	registry := &volumeapi.Registry{Client: dynamicClient, PoolReadinessStaleAfter: options.staleAfter}
	output, err := measureWithAuthority(context.Background(), registry, options, readiness.VerifyMountedPath, ownership.VerifyServingPath, func(ctx context.Context) (string, error) {
		return readMeasurement(ctx, action, options)
	})
	if err != nil {
		return err
	}
	// Publish no measurement until the post-read authority and mount checks pass.
	return os.WriteFile("/dev/termination-log", []byte(output+"\n"), 0600)
}

func measureWithAuthority(ctx context.Context, registry measurementRegistry, options measurementOptions, verifyMount func(string, volumeapi.Pool) error, verifyCopy func(string, volume.CopyIdentity) error, read func(context.Context) (string, error)) (string, error) {
	pool, copy, err := measurementAuthority(ctx, registry, options, verifyMount, verifyCopy)
	if err != nil {
		return "", err
	}
	configuration, err := capacity.UsageConfigurationEvidence(pool)
	if err != nil {
		return "", err
	}
	// Legacy node-scoped callers still pin the first exact copy across the read.
	options.copy = copy
	output, err := read(ctx)
	if err != nil {
		return "", err
	}
	if options.volumeID != "" {
		if err := awaitUsageAuthority(ctx, registry, options, configuration, verifyMount, verifyCopy); err != nil {
			return "", err
		}
	} else if _, _, err := measurementAuthority(ctx, registry, options, verifyMount, verifyCopy); err != nil {
		return "", err
	}
	return output, nil
}

func measurementAuthority(ctx context.Context, registry measurementRegistry, options measurementOptions, verifyMount func(string, volumeapi.Pool) error, verifyCopy func(string, volume.CopyIdentity) error) (volumeapi.Pool, *volume.CopyIdentity, error) {
	resolve := registry.ReadyPoolForIdentity
	if options.volumeID != "" {
		// Usage measures an existing quiesced copy; statfs serves new allocation.
		resolve = registry.InventoryPoolForIdentity
	}
	pool, err := resolve(ctx, options.pool.Name, options.pool.UID, options.pool.NodeName)
	if err != nil {
		return volumeapi.Pool{}, nil, fmt.Errorf("read measurement Pool: %w", err)
	}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil || evidence != options.evidence {
		return volumeapi.Pool{}, nil, fmt.Errorf("measurement Pool evidence changed")
	}
	if err := verifyMount(options.root, pool); err != nil {
		return volumeapi.Pool{}, nil, fmt.Errorf("verify measurement mount: %w", err)
	}
	if options.volumeID == "" {
		return pool, nil, nil
	}
	copy, err := measurementCopy(ctx, registry, options)
	if err != nil {
		return volumeapi.Pool{}, nil, err
	}
	if err := verifyCopy(options.root, *copy); err != nil {
		return volumeapi.Pool{}, nil, fmt.Errorf("verify measurement serving copy: %w", err)
	}
	return pool, copy, nil
}

// du runs once. Only a coherent successful observation awaiting a newer scan
// or timestamp may wait here. The raw current generation must become ready;
// substituting the initial generation would manufacture freshness.
func awaitUsageAuthority(ctx context.Context, registry measurementRegistry, options measurementOptions, configuration string, verifyMount func(string, volumeapi.Pool) error, verifyCopy func(string, volume.CopyIdentity) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := usageAuthorityCurrent(ctx, registry, options, configuration, verifyMount, verifyCopy)
		if err != nil || ready {
			return err
		}
		wait := options.wait
		if wait == nil {
			wait = waitUsageObservation
		}
		if err := wait(ctx); err != nil {
			return err
		}
	}
}

func usageAuthorityCurrent(ctx context.Context, registry measurementRegistry, options measurementOptions, configuration string, verifyMount func(string, volumeapi.Pool) error, verifyCopy func(string, volume.CopyIdentity) error) (bool, error) {
	pool, err := registry.PoolForIdentity(ctx, options.pool.Name, options.pool.UID, options.pool.NodeName)
	if err != nil {
		return false, fmt.Errorf("read current usage Pool: %w", err)
	}
	current, err := capacity.UsageConfigurationEvidence(pool)
	if err != nil || current != configuration {
		return false, fmt.Errorf("usage Pool configuration changed during measurement")
	}
	copy, err := measurementCopy(ctx, registry, options)
	if err != nil {
		return false, err
	}
	if err := verifyCopy(options.root, *copy); err != nil {
		return false, fmt.Errorf("verify measurement serving copy: %w", err)
	}
	now := time.Now()
	if options.now != nil {
		now = options.now()
	}
	staleAfter := options.staleAfter
	if staleAfter <= 0 {
		staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
	}
	if ready, reason := pool.InventoryReadyAt(now, staleAfter); !ready {
		if usageObservationPending(pool, now) {
			return false, nil
		}
		return false, fmt.Errorf("usage Pool observation changed: %s", reason)
	}
	if err := verifyMount(options.root, pool); err != nil {
		return false, fmt.Errorf("verify measurement mount: %w", err)
	}
	return true, nil
}

func usageObservationPending(pool volumeapi.Pool, now time.Time) bool {
	if !pool.BackingConfigurationCheck().OK || !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) ||
		pool.Status.ObservedGeneration <= 0 || pool.Status.ObservedGeneration > pool.Generation ||
		pool.Status.LastProbeTime.IsZero() || now.Before(pool.Status.LastProbeTime.Time) {
		return false
	}
	inventory := pool.Status.Inventory
	if inventory == nil || !inventory.Valid || inventory.Truncated || inventory.Message != "" ||
		inventory.ObservedAt.IsZero() || now.Before(inventory.ObservedAt.Time) {
		return false
	}
	return usageConditionsPositive(pool)
}

func usageConditionsPositive(pool volumeapi.Pool) bool {
	kinds := []string{volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint {
		kinds = append(kinds, volumeapi.PoolConditionMounted)
	}
	if pool.CapacityPolicy != "" {
		backing := volumeapi.PoolConditionCapacityBackingVerified
		if meta.FindStatusCondition(pool.Status.Conditions, backing) == nil {
			backing = volumeapi.PoolConditionCapacityIndependent
		}
		kinds = append(kinds, backing)
	}
	for _, kind := range kinds {
		condition := meta.FindStatusCondition(pool.Status.Conditions, kind)
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration <= 0 || condition.ObservedGeneration > pool.Generation {
			return false
		}
	}
	return true
}

func waitUsageObservation(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func measurementCopy(ctx context.Context, registry measurementRegistry, options measurementOptions) (*volume.CopyIdentity, error) {
	state, err := registry.Get(ctx, options.volumeID)
	if err != nil {
		return nil, err
	}
	copy := state.CurrentCopy
	if copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing ||
		copy.VolumeID != options.volumeID || copy.VolumeUID != state.UID || copy.NodeName != state.OwnerNode ||
		copy.PoolName != options.pool.Name || copy.PoolUID != options.pool.UID || copy.NodeName != options.pool.NodeName ||
		(options.copy != nil && *options.copy != *copy) || len(state.PublishedNodes) != 0 ||
		(state.Phase != volumeapi.PhaseMoving && state.Phase != volumeapi.PhaseReady) {
		return nil, fmt.Errorf("measurement volume has no exact quiesced serving copy")
	}
	installationID, err := registry.InstallationID(ctx)
	if err != nil || installationID != copy.InstallationID {
		return nil, fmt.Errorf("measurement installation identity changed")
	}
	identity := *copy
	return &identity, nil
}

func readMeasurement(ctx context.Context, action string, options measurementOptions) (string, error) {
	if action == "statfs" {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(options.root, &stat); err != nil {
			return "", err
		}
		return fmt.Sprintf("%d %d %d %d", stat.Blocks, stat.Bavail, stat.Bsize, stat.Ffree), nil
	}
	path, err := volume.Path(options.root, options.volumeID)
	if err != nil {
		return "", err
	}
	data, err := exec.CommandContext(ctx, "du", "-sbx", "--", path).Output()
	if err != nil {
		return "", fmt.Errorf("measure volume usage: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return "", fmt.Errorf("volume usage result is incomplete")
	}
	bytes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || bytes < 0 {
		return "", fmt.Errorf("volume usage result is invalid")
	}
	return strconv.FormatInt(bytes, 10), nil
}
