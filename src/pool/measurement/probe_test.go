package measurement

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestProbeRejectsAuthorityAndMountLossDuringRead(t *testing.T) {
	for _, mode := range []string{"stable", "mount-loss", "generation", "uid", "capacity", "read-error"} {
		t.Run(mode, func(t *testing.T) {
			pool := testPool()
			pools := &fakePools{pool: pool}
			checks := 0
			probe := &Probe{Pools: pools, HostRoot: "/host", VerifyMount: func(root string, current volumeapi.Pool) error {
				if root != "/host/mnt/pool" {
					t.Fatalf("unexpected host path %q", root)
				}
				checks++
				if mode == "mount-loss" && checks == 2 {
					return fmt.Errorf("mount disappeared")
				}
				return nil
			}}
			probe.Read = func(string) (capacity.Filesystem, error) {
				switch mode {
				case "generation":
					pools.pool.Generation++
				case "uid":
					pools.pool.UID = "replacement"
				case "capacity":
					pools.pool.Status.CapacityUnit = &volumeapi.PoolCapacityUnit{}
				case "read-error":
					return capacity.Filesystem{}, fmt.Errorf("read failed")
				}
				return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 50}, nil
			}
			stats, err := probe.StatFSForPool(context.Background(), pool)
			if mode == "stable" {
				if err != nil || stats.AvailableBytes != 50 || checks != 2 {
					t.Fatalf("stats=%+v checks=%d err=%v", stats, checks, err)
				}
			} else if err == nil || stats.TotalBytes != 0 {
				t.Fatalf("ambiguous read escaped: stats=%+v err=%v", stats, err)
			}
		})
	}
}

func TestReadFilesystemPinsDirectoryAndRejectsFile(t *testing.T) {
	root := t.TempDir()
	stats, err := readFilesystem(root)
	if err != nil || stats.TotalBytes <= 0 || stats.AvailableBytes < 0 || stats.AvailableBytes > stats.TotalBytes {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{file, filepath.Join(root, "missing")} {
		if _, err := readFilesystem(path); err == nil {
			t.Fatalf("non-directory %q accepted", path)
		}
	}
}
