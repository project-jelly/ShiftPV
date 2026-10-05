//go:build linux

package measurement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

func TestLinuxLiveCapacityRejectsMountLossDuringRead(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "mnt", "pool")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", root, "tmpfs", 0, "size=16m"); err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("mount capability is unavailable")
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(root, 0) })
	pool := testPool()
	pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
	observed := readiness.NewProbe(host).Inspect(pool)
	if !observed.Mounted.OK {
		t.Fatalf("mount observation failed: %+v", observed)
	}
	pool.Status.MountIdentity = observed.MountIdentity
	pools := &fakePools{pool: pool}
	probe := &Probe{Pools: pools, HostRoot: host}
	stats, err := probe.StatFSForPool(context.Background(), pool)
	if err != nil || stats.TotalBytes != 16<<20 {
		t.Fatalf("live tmpfs read=%+v err=%v", stats, err)
	}
	probe.Read = func(path string) (capacity.Filesystem, error) {
		stats, err := readFilesystem(path)
		if err != nil {
			return capacity.Filesystem{}, err
		}
		if err := unix.Unmount(root, 0); err != nil {
			return capacity.Filesystem{}, err
		}
		return stats, nil
	}
	if stats, err := probe.StatFSForPool(context.Background(), pool); err == nil || stats.TotalBytes != 0 {
		t.Fatalf("unmounted Pool read escaped: %+v err=%v", stats, err)
	}
}
