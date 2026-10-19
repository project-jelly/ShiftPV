package readiness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	calls    atomic.Int32
	blockAt  int32
}

func (i *unresponsiveInspector) wait(pool volumeapi.Pool, boundary string) {
	if pool.Name == "pool-a" && i.boundary == boundary {
		call := i.calls.Add(1)
		if i.blockAt != 0 && call != i.blockAt {
			return
		}
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

// The injected host operation ignores cancellation, like a blocked synchronous
// filesystem call. This exercises the execution boundary without a physical fault.
func TestReportUnresponsiveProbeDoesNotStarvePeer(t *testing.T) {
	for _, boundary := range []string{"allocation", "filesystem", "inventory", "release", "status"} {
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
			if boundary == "allocation" {
				pools = append(pools, testRegistration(volumeapi.Pool{Name: "pool-c", UID: "c", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock}))
			}
			if boundary == "release" {
				stamp := metav1.NewTime(testTime)
				pools[0].DeletionTimestamp = &stamp
				pools[0].IdentityReleaseApproval = pools[0].UID
			}
			i := &unresponsiveInspector{fakeAllocationInspector: base, boundary: boundary, entered: make(chan struct{}), release: make(chan struct{})}
			repo := &fakeRepository{pools: pools}
			r, err := NewReconciler("node", &unresponsiveStatus{fakeRepository: repo, inspector: i}, i, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			r.PoolTimeout = 20 * time.Millisecond
			r.Inventory = func(_ context.Context, pool volumeapi.Pool, _ time.Time) volumeapi.PoolInventory {
				i.wait(pool, "inventory")
				return volumeapi.PoolInventory{Valid: true}
			}
			r.Release = func(_ context.Context, pool volumeapi.Pool) error {
				i.wait(pool, "release")
				return nil
			}
			peer := make(chan struct{}, 4)
			r.Observe = func(pool volumeapi.Pool, _ Result, _ error) {
				if pool.Name == "pool-b" {
					peer <- struct{}{}
				}
			}
			done := make(chan error, 1)
			go func() { done <- r.Reconcile(context.Background()) }()
			released := false
			defer func() {
				if !released {
					close(i.release)
				}
				waitPoolWorkers(t, r)
			}()
			select {
			case <-i.entered:
			case <-time.After(time.Second):
				t.Fatal("host dependency was not reached")
			}
			select {
			case <-peer:
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("POOL-F2: independent peer was not observed after 10 PoolTimeout windows while %s was blocked", boundary)
			}
			if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("missing blocked Pool error: %v", err)
			}
			if boundary == "allocation" {
				candidate := repo.statuses["pool-c"]
				if candidate.RegistrationApproved || candidate.CapacityUnit != nil {
					t.Fatal("new registration approved without live evidence from an approved peer")
				}
				peerStatus := repo.statuses["pool-b"]
				ready := meta.FindStatusCondition(peerStatus.Conditions, volumeapi.PoolConditionReady)
				if !completeInventory(peerStatus.Inventory) || ready == nil || ready.Reason != "CapacityPeerUnproven" {
					t.Fatal("peer must keep fresh inventory while new allocation remains blocked")
				}
			}
			for n := 0; n < 2; n++ {
				if err := r.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("retry lost blocked Pool error: %v", err)
				}
			}
			if i.calls.Load() != 1 {
				t.Fatalf("retries started %d blocked calls for one Pool", i.calls.Load())
			}
			close(i.release)
			released = true
			waitPoolWorkers(t, r)
			if _, ok := repo.statuses["pool-a"]; ok {
				t.Fatal("late observation wrote status after its deadline")
			}
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatalf("Pool did not recover on a fresh observation: %v", err)
			}
			if _, ok := repo.statuses["pool-a"]; !ok {
				t.Fatal("fresh observation did not publish recovered Pool status")
			}
		})
	}
}

type unresponsiveStatus struct {
	*fakeRepository
	inspector *unresponsiveInspector
}

func (r *unresponsiveStatus) SetPoolStatus(ctx context.Context, name, uid, node string, status volumeapi.PoolStatus) error {
	r.inspector.wait(volumeapi.Pool{Name: name}, "status")
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.fakeRepository.SetPoolStatus(ctx, name, uid, node, status)
}

func waitPoolWorkers(t *testing.T, r *Reconciler) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		r.workMu.Lock()
		var done <-chan struct{}
		for _, work := range r.inFlight {
			done = work.done
			break
		}
		r.workMu.Unlock()
		if done == nil {
			return
		}
		select {
		case <-done:
		case <-deadline:
			t.Fatal("Pool worker did not finish after its dependency resumed")
		}
	}
}

func TestRegistrationRecheckTimeoutCannotApproveLateEvidence(t *testing.T) {
	ok := Check{Known: true, OK: true, Reason: "OK"}
	base := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok, Filesystem: testFilesystem}}
	a := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock})
	b := testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock})
	b.Status.RegistrationApproved = true
	b.Status.CapacityUnit, _ = base.InspectCapacityUnit(b)
	i := &unresponsiveInspector{fakeAllocationInspector: base, boundary: "allocation", blockAt: 2, entered: make(chan struct{}), release: make(chan struct{})}
	repo := &fakeRepository{pools: []volumeapi.Pool{a, b}}
	r, err := NewReconciler("node", repo, i, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.PoolTimeout = 20 * time.Millisecond
	r.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		return volumeapi.PoolInventory{Valid: true}
	}
	defer waitPoolWorkers(t, r)
	defer close(i.release)
	if err := r.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registration recheck did not time out: %v", err)
	}
	select {
	case <-i.entered:
	default:
		t.Fatal("fresh approval recheck was not reached")
	}
	if !completeInventory(repo.statuses[b.Name].Inventory) {
		t.Fatal("candidate approval recheck blocked approved peer inventory")
	}
	// Let the old worker return, then check that its proof was discarded.
	i.release <- struct{}{}
	waitPoolWorkers(t, r)
	if _, ok := repo.statuses[a.Name]; ok {
		t.Fatal("expired registration recheck approved late backing evidence")
	}
	if err := r.Reconcile(context.Background()); err != nil || !repo.statuses[a.Name].RegistrationApproved {
		t.Fatalf("fresh registration did not recover after host resumed: %v", err)
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
