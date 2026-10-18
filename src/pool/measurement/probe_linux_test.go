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
		stats, err := ReadFilesystem(path)
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

func TestLinuxLiveCapacityRejectsReadOnlyBeforeReadinessRefresh(t *testing.T) {
	for _, bindOnly := range []bool{false, true} {
		name := "filesystem"
		if bindOnly {
			name = "bind"
		}
		t.Run(name, func(t *testing.T) {
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
			filesystemRoot := root
			t.Cleanup(func() { _ = unix.Unmount(filesystemRoot, 0) })
			pool := testPool()
			pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
			observed := readiness.NewProbe(host).Inspect(pool)
			if !observed.Mounted.OK || !observed.Writable.OK {
				t.Fatalf("initial observation=%+v", observed)
			}
			pool.Status.MountIdentity = observed.MountIdentity
			if bindOnly {
				bind := filepath.Join(host, "bind")
				if err := os.Mkdir(bind, 0700); err != nil {
					t.Fatal(err)
				}
				if err := unix.Mount(root, bind, "", unix.MS_BIND, ""); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = unix.Unmount(bind, 0) })
				root = bind
				pool.MountPath = "/bind"
			}
			probe := &Probe{Pools: &fakePools{pool: pool}, HostRoot: host}
			if _, err := probe.StatFSForPool(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			flags := uintptr(unix.MS_REMOUNT | unix.MS_RDONLY)
			if bindOnly {
				flags |= unix.MS_BIND
			}
			if err := unix.Mount("", root, "", flags, ""); err != nil {
				t.Fatal(err)
			}
			// Keep the original API readiness and mount identity. Only the
			// live allocation probe can notice this intervening remount.
			if stats, err := probe.StatFSForPool(context.Background(), pool); !errors.Is(err, syscall.EROFS) || stats != (capacity.Filesystem{}) {
				t.Fatalf("read-only allocation accepted before refresh: stats=%+v err=%v", stats, err)
			}
			if err := unix.Mount("", root, "", flags & ^uintptr(unix.MS_RDONLY), ""); err != nil {
				t.Fatal(err)
			}
			if stats, err := probe.StatFSForPool(context.Background(), pool); err != nil || stats.TotalBytes != 16<<20 {
				t.Fatalf("restored Pool=%+v err=%v", stats, err)
			}
		})
	}
}
