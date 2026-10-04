//go:build linux

package readiness

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestProbeMountedFilesystemRejectsUnmountFallback(t *testing.T) {
	hostRoot := t.TempDir()
	mountPath := filepath.Join(hostRoot, "pool")
	if err := os.Mkdir(mountPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", mountPath, "tmpfs", 0, "size=1m"); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("mount capability is unavailable")
		}
		t.Fatal(err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(mountPath, 0)
		}
	})
	pool := volumeapi.Pool{MountPath: "/pool", MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint}
	probe := NewProbe(hostRoot)
	first := probe.Inspect(pool)
	if !first.Mounted.OK || first.MountIdentity == nil {
		t.Fatalf("mounted Pool probe = %#v", first)
	}
	pool.Status.MountIdentity = first.MountIdentity
	if err := unix.Unmount(mountPath, 0); err != nil {
		t.Fatal(err)
	}
	mounted = false
	second := probe.Inspect(pool)
	if second.Mounted.Reason != "MountMissing" || second.Writable.Known || second.CapacityReadable.Known {
		t.Fatalf("unmounted Pool probe = %#v", second)
	}
	entries, err := os.ReadDir(mountPath)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unmounted parent directory was written: entries=%v err=%v", entries, err)
	}
}

func TestVerifyMountedPathChecksHelperBindAndFallback(t *testing.T) {
	root := t.TempDir()
	poolRoot := filepath.Join(root, "pool")
	helperRoot := filepath.Join(root, "helper")
	fallbackRoot := filepath.Join(root, "fallback")
	for _, path := range []string{poolRoot, helperRoot, fallbackRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount("tmpfs", poolRoot, "tmpfs", 0, "size=1m"); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("mount capability is unavailable")
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(poolRoot, 0) })
	pool := volumeapi.Pool{MountPath: "/pool", MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint}
	first := NewProbe(root).Inspect(pool)
	if !first.Mounted.OK || first.MountIdentity == nil {
		t.Fatalf("initial mount probe = %#v", first)
	}
	pool.Status.MountIdentity = first.MountIdentity
	if err := unix.Mount(poolRoot, helperRoot, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(helperRoot, 0) })
	if err := VerifyMountedPath(helperRoot, pool); err != nil {
		t.Fatalf("verify helper bind: %v", err)
	}
	if err := unix.Unmount(poolRoot, 0); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMountedPath(poolRoot, pool); err == nil {
		t.Fatal("unmounted host path was accepted")
	}
	if err := unix.Mount(poolRoot, fallbackRoot, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(fallbackRoot, 0) })
	if err := VerifyMountedPath(fallbackRoot, pool); err == nil {
		t.Fatal("helper bound to fallback directory was accepted")
	}
	if err := VerifyMountedPath(helperRoot, pool); err != nil {
		t.Fatalf("existing helper bind lost its original filesystem: %v", err)
	}
}
