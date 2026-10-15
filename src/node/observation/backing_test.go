//go:build linux || darwin

package observation

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

type scanAllocation func(string) ([]capacityunit.Extent, error)

func (f scanAllocation) Resolve(device string) ([]capacityunit.Extent, error) { return f(device) }

func TestInventoryWithPeerFailureStillVerifiesBackingBeforeAndAfterScan(t *testing.T) {
	for _, scenario := range []string{"peer unavailable", "own changed before scan", "own changed during scan"} {
		t.Run(scenario, func(t *testing.T) {
			scanner, pool, _, copy := interleavingFixture(t)
			host, err := filepath.EvalSymlinks(scanner.HostRoot)
			if err != nil {
				t.Fatal(err)
			}
			scanner.HostRoot = host
			pool.CapacityPolicy = volumeapi.PoolCapacityPolicyFixedBlock
			pool.Generation = 1
			pool.Status.CapacityUnit = &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 2048, Sectors: 4096}}}
			pool.Status.Conditions = []metav1.Condition{
				{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: "CapacityPeerUnproven"},
				{Type: volumeapi.PoolConditionCapacityBackingVerified, Status: metav1.ConditionTrue, ObservedGeneration: 1},
			}
			changed := scenario == "own changed before scan"
			deps := readiness.HostProbeDependencies(host)
			deps.ListMounts = func() ([]mountutils.MountInfo, error) {
				return []mountutils.MountInfo{{MountPoint: host, Major: 8, Minor: 1, Source: "/dev/test", FsType: "ext4", Root: "/"}}, nil
			}
			deps.Allocations = scanAllocation(func(string) ([]capacityunit.Extent, error) {
				if changed {
					return []capacityunit.Extent{{Device: "8:0", Start: 8192, Sectors: 4096}}, nil
				}
				return pool.Status.CapacityUnit.Extents, nil
			})
			probe, err := readiness.NewProbeWithDependencies(host, deps)
			if err != nil {
				t.Fatal(err)
			}
			checks, scans := 0, 0
			scanner.VerifyBacking = func(_ string, p volumeapi.Pool) error { checks++; return probe.VerifyBacking(p) }
			result := scanner.scan(context.Background(), pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
				scans++
				copies, truncated, err := scanPhysical(ctx, root, known, limit)
				changed = scenario == "own changed during scan"
				return copies, truncated, err
			}, physicalPathType)
			if scenario == "peer unavailable" {
				if !result.Valid || result.Truncated || checks != 2 || scans != 1 || len(result.Copies) != 1 || result.Copies[0].Identity == nil || *result.Copies[0].Identity != copy || !result.Copies[0].Published {
					t.Fatalf("existing inventory lost: checks=%d scans=%d result=%+v", checks, scans, result)
				}
			} else if result.Valid || !strings.HasPrefix(result.Message, "Mount") {
				t.Fatalf("changed backing became complete inventory: checks=%d result=%+v", checks, result)
			}
			if scenario == "own changed before scan" && scans != 0 {
				t.Fatal("scanned a changed backing")
			}
		})
	}
}
