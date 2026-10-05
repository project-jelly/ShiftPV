//go:build linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

func mountedMeasurementFixture(t *testing.T) (*measurementRepository, measurementOptions) {
	t.Helper()
	if os.Getenv("SHIFTPV_LINUX_MOUNT_INTEGRATION") != "1" {
		t.Skip("requires isolated privileged mount integration")
	}
	repo, options := measurementFixture(t)
	parent := options.root
	options.root = filepath.Join(parent, "pool")
	if err := os.Mkdir(options.root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", options.root, "tmpfs", 0, "size=4m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(options.root, 0) })
	repo.pool.MountPath = "/pool"
	repo.pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
	result := readiness.NewProbe(parent).Inspect(repo.pool)
	if !result.Mounted.OK || result.MountIdentity == nil {
		t.Fatalf("mount probe=%+v", result)
	}
	repo.pool.Status.MountIdentity = result.MountIdentity
	options.pool = repo.pool
	var err error
	options.evidence, err = capacity.MeasurementEvidence(repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	return repo, options
}

func TestLinuxMeasurementIntegrationRejectsMountLossDuringRead(t *testing.T) {
	repo, options := mountedMeasurementFixture(t)
	options.volumeID, options.copy = "", nil
	read := func(ctx context.Context) (string, error) { return readMeasurement(ctx, "statfs", options) }
	output, err := measureWithAuthority(context.Background(), repo, options, readiness.VerifyMountedPath, ownership.VerifyServingPath, read)
	if err != nil || output == "" {
		t.Fatalf("initial output=%q err=%v", output, err)
	}
	output, err = measureWithAuthority(context.Background(), repo, options, readiness.VerifyMountedPath, ownership.VerifyServingPath, func(ctx context.Context) (string, error) {
		value, err := read(ctx)
		if err != nil {
			return "", err
		}
		if err := unix.Unmount(options.root, 0); err != nil {
			t.Fatal(err)
		}
		return value, nil
	})
	if err == nil || output != "" {
		t.Fatalf("fallback capacity accepted: output=%q err=%v", output, err)
	}
}

func TestLinuxMeasurementIntegrationMeasuresVerifiedCopyThroughReadOnlyBind(t *testing.T) {
	repo, options := mountedMeasurementFixture(t)
	if err := ownership.PrepareServing(context.Background(), options.root, *options.copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	bind := filepath.Join(filepath.Dir(options.root), "helper")
	if err := os.Mkdir(bind, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(options.root, bind, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(bind, 0) })
	if err := unix.Mount("", bind, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
		t.Fatal(err)
	}
	options.root = bind
	output, err := measureWithAuthority(context.Background(), repo, options, readiness.VerifyMountedPath, ownership.VerifyServingPath,
		func(ctx context.Context) (string, error) { return readMeasurement(ctx, "usage", options) })
	if err != nil || output == "" {
		t.Fatalf("read-only serving usage output=%q err=%v", output, err)
	}
}
