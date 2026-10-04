package readiness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

type fakeAllocationInspector struct {
	result   Result
	shared   bool
	inspects *int
}

func (f fakeAllocationInspector) InspectCapacityUnit(pool volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check) {
	start := uint64(0)
	device := "8:1"
	if pool.Name == "pool-b" && !f.shared {
		start = 1024
		device = "8:2"
	}
	return &volumeapi.PoolCapacityUnit{Device: device, Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: start, Sectors: 1024}}}, Check{OK: true, Known: true, Reason: "CapacityAllocationVerified", Message: "verified"}
}
func (f fakeAllocationInspector) Inspect(pool volumeapi.Pool) Result {
	if f.inspects != nil {
		(*f.inspects)++
	}
	result := f.result
	result.CapacityUnit, result.Independent = f.InspectCapacityUnit(pool)
	return result
}
func TestSharedAllocationIsRejectedBeforeWritableProbeAndInventory(t *testing.T) {
	ok := Check{OK: true, Known: true, Reason: "OK", Message: "ok"}
	repo := &fakeRepository{pools: []volumeapi.Pool{
		{Name: "pool-a", UID: "a-uid", NodeName: "node", MountPath: "/a", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1},
		{Name: "pool-b", UID: "b-uid", NodeName: "node", MountPath: "/b", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1},
	}}
	scans, inspects := 0, 0
	r := &Reconciler{NodeName: "node", Pools: repo, Interval: time.Minute, Inspector: fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}, shared: true, inspects: &inspects}, Inventory: func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		scans++
		return volumeapi.PoolInventory{Valid: true}
	}}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if scans != 0 || inspects != 0 {
		t.Fatal("shared allocation was scanned")
	}
	for _, status := range repo.statuses {
		ready := meta.FindStatusCondition(status.Conditions, volumeapi.PoolConditionReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "CapacityBackingOverlap" {
			t.Fatalf("shared allocation status=%+v", status)
		}
		if status.CapacityUnit != nil {
			t.Fatal("unproven capacity identity was anchored")
		}
	}
}

type allocationResolver struct {
	extents []capacityunit.Extent
	err     error
}

func (r allocationResolver) Resolve(string) ([]capacityunit.Extent, error) { return r.extents, r.err }
func TestFixedBlockProbeAnchorsDirectoryFilesystemAndRejectsChangingBacking(t *testing.T) {
	writes := 0
	hostRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(hostRoot, "var/lib/shiftpv"), 0755); err != nil {
		t.Fatal(err)
	}
	p := &Probe{HostRoot: hostRoot, inspect: func(string) error { return nil }, write: func(string) error { writes++; return nil }, statFS: func(string) (poolcapacity.Filesystem, error) { return poolcapacity.Filesystem{}, nil },
		allocations: allocationResolver{extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}},
		listMounts: func() ([]mountutils.MountInfo, error) {
			return []mountutils.MountInfo{{MountPoint: hostRoot, Major: 8, Minor: 1, Source: "/dev/a", FsType: "ext4", Root: "/"}}, nil
		},
	}
	pool := volumeapi.Pool{MountPath: "/var/lib/shiftpv", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}
	result := p.Inspect(pool)
	if !result.Independent.OK || result.CapacityUnit == nil || writes != 1 {
		t.Fatalf("directory allocation=%+v writes=%d", result, writes)
	}
	pool.Status.CapacityUnit = result.CapacityUnit
	p.allocations = allocationResolver{extents: []capacityunit.Extent{{Device: "8:0", Start: 200, Sectors: 100}}}
	result = p.Inspect(pool)
	if result.Independent.Reason != "CapacityIdentityChanged" || writes != 1 {
		t.Fatalf("changed backing was probed: %+v writes=%d", result, writes)
	}
	p.allocations = allocationResolver{err: fmt.Errorf("thin allocation unsupported")}
	result = p.Inspect(pool)
	if result.Independent.OK || writes != 1 {
		t.Fatal("unknown/thin allocation was approved")
	}
}

func TestCapacityMountUsesMostSpecificUnambiguousMount(t *testing.T) {
	mounts := []mountutils.MountInfo{{MountPoint: "/host"}, {MountPoint: "/host"}, {MountPoint: "/host/mnt/a", Major: 8, Minor: 1}}
	p := &Probe{listMounts: func() ([]mountutils.MountInfo, error) { return mounts, nil }}
	entry, err := p.capacityMount("/host/mnt/a/data")
	if err != nil || entry.Minor != 1 {
		t.Fatalf("specific mount=%v err=%v", entry, err)
	}
	mounts = append(mounts, mounts[2])
	if _, err := p.capacityMount("/host/mnt/a/data"); err == nil {
		t.Fatal("ambiguous selected mount accepted")
	}
}

func TestNestedPoolRootsAreRejectedBeforeWrites(t *testing.T) {
	a := volumeapi.Pool{Name: "pool-a", UID: "a-uid", NodeName: "node", MountPath: "/mnt/a", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}
	b := volumeapi.Pool{Name: "pool-b", UID: "b-uid", NodeName: "node", MountPath: "/mnt/a/nested", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}
	r := &Reconciler{NodeName: "node", Inspector: fakeAllocationInspector{}}
	observations := r.capacityIsolation([]volumeapi.Pool{a, b})
	for _, p := range []volumeapi.Pool{a, b} {
		if observed := observations[p.UID]; observed.check.OK || observed.check.Reason != "CapacityPathsOverlap" {
			t.Fatalf("nested root approved: %+v", observed)
		}
	}
}

func TestDeletingLegacyDuplicateStillCanBeInspected(t *testing.T) {
	deleting := metav1.NewTime(time.Now())
	pools := []volumeapi.Pool{
		{Name: "a", UID: "a-uid", NodeName: "node", MountPath: "/a", DeletionTimestamp: &deleting},
		{Name: "b", UID: "b-uid", NodeName: "node", MountPath: "/b"},
	}
	r := &Reconciler{NodeName: "node", Inspector: fakeInspector{}}
	observations := r.capacityIsolation(pools)
	if _, exists := observations["a-uid"]; exists {
		t.Fatal("terminating legacy Pool was blocked from cleanup inspection")
	}
	if observations["b-uid"].check.OK || observations["b-uid"].check.Reason != "CapacityIsolationRequired" {
		t.Fatal("mixed duplicate became usable")
	}
}

func TestFixedBlockRejectsSymlinkBeforeWrite(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "real"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	p := NewProbe(root)
	writes := 0
	p.write = func(string) error { writes++; return nil }
	result := p.Inspect(volumeapi.Pool{MountPath: "/alias", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock})
	if result.Independent.Reason != "CapacityPathInvalid" || writes != 0 {
		t.Fatalf("symlink written: %+v writes=%d", result, writes)
	}
}

func TestMissingLivePeerEvidenceDoesNotReuseOldAnchor(t *testing.T) {
	inspector := fakeAllocationInspector{}
	left := volumeapi.Pool{Name: "pool-a", UID: "a", MountPath: "/a"}
	right := volumeapi.Pool{Name: "pool-b", UID: "b", MountPath: "/b"}
	unit, check := inspector.InspectCapacityUnit(left)
	right.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(right)
	results := map[string]allocationObservation{
		"a": {unit: unit, check: check},
		"b": {check: capacityFailure("CapacityAllocationUnproven", fmt.Errorf("unsupported changed backing"))},
	}
	compareAllocations(left, right, results)
	if results["a"].check.Reason != "CapacityPeerUnproven" || results["b"].check.Reason != "CapacityAllocationUnproven" {
		t.Fatalf("ambiguous peer results=%+v", results)
	}
}
