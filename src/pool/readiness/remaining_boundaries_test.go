package readiness

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

type unresponsiveInspector struct {
	fakeAllocationInspector
	boundary string
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (i *unresponsiveInspector) wait(pool volumeapi.Pool, boundary string) {
	if pool.Name == "pool-a" && i.boundary == boundary {
		i.once.Do(func() { close(i.entered) })
		<-i.release
	}
}

func (i *unresponsiveInspector) Inspect(pool volumeapi.Pool) Result {
	i.wait(pool, "filesystem")
	return i.fakeAllocationInspector.Inspect(pool)
}

func (i *unresponsiveInspector) InspectCapacityUnit(pool volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check) {
	i.wait(pool, "allocation")
	return i.fakeAllocationInspector.InspectCapacityUnit(pool)
}

// This is a desired liveness regression, expected to fail on e035075. The
// injected host operation does not cooperate with cancellation, like a blocked
// synchronous filesystem call. No physical disk failure is claimed.
func TestReportUnresponsiveProbeDoesNotStarvePeer(t *testing.T) {
	for _, boundary := range []string{"allocation", "filesystem"} {
		t.Run(boundary, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK"}
			base := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok, Filesystem: testFilesystem}}
			pools := []volumeapi.Pool{
				testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}),
				testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}),
			}
			for n := range pools {
				pools[n].Status.RegistrationApproved = true
				pools[n].Status.CapacityUnit, _ = base.InspectCapacityUnit(pools[n])
			}
			i := &unresponsiveInspector{fakeAllocationInspector: base, boundary: boundary, entered: make(chan struct{}), release: make(chan struct{})}
			repo := &fakeRepository{pools: pools}
			r, err := NewReconciler("node", repo, i, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			r.PoolTimeout = 20 * time.Millisecond
			peer := make(chan struct{}, 1)
			r.Observe = func(pool volumeapi.Pool, _ Result, _ error) {
				if pool.Name == "pool-b" {
					peer <- struct{}{}
				}
			}
			done := make(chan error, 1)
			go func() { done <- r.Reconcile(context.Background()) }()
			defer func() {
				close(i.release)
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("probe did not finish after test dependency resumed")
				}
			}()
			select {
			case <-i.entered:
			case <-time.After(time.Second):
				t.Fatal("host dependency was not reached")
			}
			select {
			case <-peer:
			case <-time.After(200 * time.Millisecond):
				t.Errorf("POOL-F2: independent peer was not observed after 10 PoolTimeout windows while %s was blocked", boundary)
			}
		})
	}
}

func TestReportRenumberedDeviceRejectsUnchangedBacking(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "pool"), 0755); err != nil {
		t.Fatal(err)
	}
	minor := 1
	extents := []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}
	p := &Probe{HostRoot: root, allocations: allocationResolver{extents: extents}, listMounts: func() ([]mountutils.MountInfo, error) {
		return []mountutils.MountInfo{{MountPoint: filepath.Join(root, "pool"), Major: 253, Minor: minor, Source: "/dev/mapper/retained-lv", FsType: "ext4"}}, nil
	}}
	pool := volumeapi.Pool{MountPath: "/pool", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}
	original, check := p.InspectCapacityUnit(pool)
	if !check.OK {
		t.Fatalf("initial allocation rejected: %+v", check)
	}
	pool.Status.CapacityUnit = original
	minor = 7 // Same source, filesystem and physical ranges; kernel number changed.
	current, check := p.InspectCapacityUnit(pool)
	if check.OK || check.Reason != "CapacityIdentityChanged" || !reflect.DeepEqual(original.Extents, current.Extents) || original.Source != current.Source {
		t.Fatalf("unexpected renumbering result: before=%+v after=%+v check=%+v", original, current, check)
	}
	t.Logf("POOL-F3 observed conservatively rejected transition %s -> %s with unchanged source/extents; real reboot not claimed", original.Device, current.Device)
}
