package readiness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mountutils "k8s.io/mount-utils"
)

// This uses real path validation and Probe verification with injected mount and
// block-allocation reads. It does not qualify a physical disk or LVM device.
func TestApprovedBackingVerificationIgnoresScanGenerationButRejectsDrift(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "pool"), 0700); err != nil {
		t.Fatal(err)
	}
	extents := []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}
	mount := mountutils.MountInfo{MountPoint: root, Major: 8, Minor: 1, Source: "/dev/a", FsType: "ext4", Root: "/"}
	p := &Probe{HostRoot: root, inspect: inspectDirectory, allocations: allocationResolver{extents: extents},
		listMounts: func() ([]mountutils.MountInfo, error) { return []mountutils.MountInfo{mount}, nil },
	}
	pool := volumeapi.Pool{MountPath: "/pool", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}
	unit, check := p.InspectCapacityUnit(pool)
	if !check.OK {
		t.Fatal(check)
	}
	pool.Status.CapacityUnit = unit
	pool.Status.Conditions = []metav1.Condition{{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	if err := p.VerifyPool(pool); err != nil {
		t.Fatal(err)
	}
	pool.Generation++
	if err := p.VerifyPool(pool); err == nil {
		t.Fatal("new placement bypassed the observation gate")
	}
	if err := p.VerifyBacking(pool); err != nil {
		t.Fatalf("stable approved backing blocked by a scan: %v", err)
	}
	p.allocations = allocationResolver{extents: []capacityunit.Extent{{Device: "8:0", Start: 200, Sectors: 100}}}
	if err := p.VerifyBacking(pool); err == nil {
		t.Fatal("changed physical allocation passed")
	}
	p.allocations = allocationResolver{extents: extents}
	mount.Source = "/dev/replacement"
	if err := p.VerifyBacking(pool); err == nil {
		t.Fatal("changed mount identity passed")
	}
	mount.Source = "/dev/a"
	pool.Status.CapacityUnit = nil
	if err := p.VerifyBacking(pool); err == nil {
		t.Fatal("missing capacity anchor passed")
	}
}
