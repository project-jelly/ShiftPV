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
	"strconv"
	"strings"
	"syscall"
	"time"

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
}

type measurementRegistry interface {
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
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
	copy, err := measurementAuthority(ctx, registry, options, verifyMount, verifyCopy)
	if err != nil {
		return "", err
	}
	// Legacy node-scoped callers still pin the first exact copy across the read.
	options.copy = copy
	output, err := read(ctx)
	if err != nil {
		return "", err
	}
	if _, err := measurementAuthority(ctx, registry, options, verifyMount, verifyCopy); err != nil {
		return "", err
	}
	return output, nil
}

func measurementAuthority(ctx context.Context, registry measurementRegistry, options measurementOptions, verifyMount func(string, volumeapi.Pool) error, verifyCopy func(string, volume.CopyIdentity) error) (*volume.CopyIdentity, error) {
	pool, err := registry.ReadyPoolForIdentity(ctx, options.pool.Name, options.pool.UID, options.pool.NodeName)
	if err != nil {
		return nil, fmt.Errorf("read measurement Pool: %w", err)
	}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil || evidence != options.evidence {
		return nil, fmt.Errorf("measurement Pool evidence changed")
	}
	if err := verifyMount(options.root, pool); err != nil {
		return nil, fmt.Errorf("verify measurement mount: %w", err)
	}
	if options.volumeID == "" {
		return nil, nil
	}
	copy, err := measurementCopy(ctx, registry, options)
	if err != nil {
		return nil, err
	}
	if err := verifyCopy(options.root, *copy); err != nil {
		return nil, fmt.Errorf("verify measurement serving copy: %w", err)
	}
	return copy, nil
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
