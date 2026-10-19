package readiness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type batchBlockedInspector struct {
	boundary string
	release  <-chan struct{}
}

func (i batchBlockedInspector) wait(pool volumeapi.Pool, boundary string) {
	if strings.HasPrefix(pool.Name, "blocked-") && i.boundary == boundary {
		<-i.release
	}
}

func (i batchBlockedInspector) InspectCapacityUnit(pool volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check) {
	i.wait(pool, "allocation")
	n := 100
	if strings.HasPrefix(pool.Name, "blocked-") {
		_, _ = fmt.Sscanf(pool.Name, "blocked-%d", &n)
	}
	return &volumeapi.PoolCapacityUnit{Device: fmt.Sprintf("253:%d", n), Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: uint64(n+1) * 1024, Sectors: 1024}}}, Check{Known: true, OK: true, Reason: "CapacityAllocationVerified"}
}

func (i batchBlockedInspector) Inspect(pool volumeapi.Pool) Result {
	i.wait(pool, "filesystem")
	ok := Check{Known: true, OK: true, Reason: "OK"}
	unit, backing := i.InspectCapacityUnit(pool)
	return Result{Backing: backing, CapacityUnit: unit, Accessible: ok, Writable: ok, CapacityReadable: ok, Filesystem: testFilesystem}
}

type batchBlockedRepository struct {
	*fakeRepository
	inspector batchBlockedInspector
}

func (r batchBlockedRepository) SetPoolStatus(ctx context.Context, name, uid, node string, status volumeapi.PoolStatus) error {
	r.inspector.wait(volumeapi.Pool{Name: name}, "status")
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.fakeRepository.SetPoolStatus(ctx, name, uid, node, status)
}

// Keep the production timeout/freshness ratio, scaled down for DI. The same
// invariant applies to several failed disks, many LVs on one failed disk, and
// pending registrations: their waits must not add up before a healthy peer runs.
func TestManyBlockedPoolsPreserveHealthyPeerProgress(t *testing.T) {
	for _, count := range []int{2, 40} {
		for _, approved := range []bool{true, false} {
			for _, boundary := range []string{"allocation", "filesystem", "inventory", "release", "status"} {
				if !approved && boundary == "release" {
					continue // Identity release is a deletion, not a new registration.
				}
				t.Run(fmt.Sprintf("count=%d/approved=%t/%s", count, approved, boundary), func(t *testing.T) {
					testBlockedPoolBatch(t, count, approved, boundary)
				})
			}
		}
	}
}

func testBlockedPoolBatch(t *testing.T, count int, approved bool, boundary string) {
	t.Helper()
	release := make(chan struct{})
	inspector := batchBlockedInspector{boundary: boundary, release: release}
	base := inspector
	base.boundary = "" // Construct the approved anchors before injecting failure.
	pools := make([]volumeapi.Pool, 0, count+1)
	started := time.Now()
	for n := 0; n <= count; n++ {
		name := fmt.Sprintf("blocked-%d", n)
		if n == count {
			name = "healthy"
		}
		pool := testRegistration(volumeapi.Pool{Name: name, UID: name, NodeName: "node", Generation: 1,
			CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Finalizers: []string{volumeapi.PoolProtectionFinalizer}})
		pool.Status.RegistrationApproved = approved || name == "healthy"
		if pool.Status.RegistrationApproved {
			pool.Status.CapacityUnit, _ = base.InspectCapacityUnit(pool)
		}
		pool.Status.ObservedGeneration = 1
		pool.Status.LastProbeTime = metav1.NewTime(started)
		pool.Status.Inventory = &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(started), Valid: true}
		for _, kind := range []string{volumeapi.PoolConditionReady, volumeapi.PoolConditionCapacityIndependent, volumeapi.PoolConditionCapacityBackingVerified, volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
			pool.Status.Conditions = append(pool.Status.Conditions, metav1.Condition{Type: kind, Status: metav1.ConditionTrue, ObservedGeneration: 1})
		}
		if boundary == "release" && name != "healthy" {
			stamp := metav1.NewTime(started)
			pool.DeletionTimestamp, pool.IdentityReleaseApproval = &stamp, pool.UID
		}
		pools = append(pools, pool)
	}
	repo := &fakeRepository{pools: pools}
	r, err := NewReconciler("node", batchBlockedRepository{repo, inspector}, inspector, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r.PoolTimeout = 20 * time.Millisecond
	freshness := volumeapi.DefaultPoolReadinessStaleAfter * r.PoolTimeout / defaultPoolTimeout
	healthy := pools[len(pools)-1]
	if ready, reason := healthy.InventoryReadyAt(started, freshness); !ready {
		t.Fatalf("invalid initially Ready fixture: %s", reason)
	}
	r.Inventory = func(_ context.Context, pool volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
		inspector.wait(pool, "inventory")
		return volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}
	}
	r.Release = func(_ context.Context, pool volumeapi.Pool) error {
		inspector.wait(pool, "release")
		return nil
	}
	observed := make(chan struct{}, 1)
	r.Observe = func(pool volumeapi.Pool, _ Result, _ error) {
		if pool.UID == healthy.UID {
			observed <- struct{}{}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Reconcile(ctx) }()
	defer func() {
		cancel()
		close(release)
		waitPoolWorkers(t, r)
	}()
	select {
	case <-observed:
	case <-time.After(freshness):
		t.Fatalf("%d blocked peers consumed the healthy Pool's freshness budget", count)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("missing blocked observation error: %v", err)
		}
	case <-time.After(8 * r.PoolTimeout):
		t.Fatal("pending registrations accumulated per-Pool waits instead of one pass budget")
	}
	healthy.Status = repo.statuses[healthy.Name]
	if ready, reason := healthy.InventoryReadyAt(time.Now(), freshness); !ready {
		t.Fatalf("healthy own inventory stopped: %s", reason)
	}
}
