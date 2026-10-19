package readiness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	mountutils "k8s.io/mount-utils"
)

type backingResolver func(string) ([]capacityunit.Extent, error)

func (f backingResolver) Resolve(device string) ([]capacityunit.Extent, error) { return f(device) }

// Registry encoding, registration, path validation and write probes are real;
// mount/sysfs and inventory observations are injected at their existing seams.
type backingFixture struct {
	countsMu   sync.Mutex
	stateMu    sync.RWMutex
	registry   *volumeapi.Registry
	client     *dynamicfake.FakeDynamicClient
	reconciler *Reconciler
	probe      *Probe
	mounts     []mountutils.MountInfo
	extents    map[string][]capacityunit.Extent
	writes     map[string]int
	scans      map[string]int
}

func newBackingFixture(t *testing.T) *backingFixture {
	t.Helper()
	host, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &backingFixture{writes: map[string]int{}, scans: map[string]int{}, extents: map[string][]capacityunit.Extent{}}
	f.mounts = []mountutils.MountInfo{{MountPoint: host, Major: 8, Minor: 0, Root: "/", Source: "/dev/root", FsType: "ext4"}}
	objects := []runtime.Object{}
	for i, name := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(host, name), 0700); err != nil {
			t.Fatal(err)
		}
		device := fmt.Sprintf("8:%d", i+1)
		f.extents[device] = []capacityunit.Extent{{Device: "8:0", Start: uint64((i + 1) * 100), Sectors: 100}}
		f.mounts = append(f.mounts, mountutils.MountInfo{MountPoint: filepath.Join(host, name), Major: 8, Minor: i + 1, Root: "/", Source: "/dev/" + name, FsType: "ext4"})
		objects = append(objects, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
			"metadata": map[string]any{"name": name, "uid": name + "-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
			"spec": map[string]any{"nodeName": "node", "poolGroup": "default", "mountPath": "/" + name,
				"mountPolicy": volumeapi.PoolMountPolicyRequireMountPoint, "capacityPolicy": volumeapi.PoolCapacityPolicyFixedBlock,
				"capacity": map[string]any{"limit": "1Gi"}},
		}})
	}
	f.client = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, objects...)
	f.registry = &volumeapi.Registry{Client: f.client, Now: func() time.Time { return testTime }}
	f.probe, err = NewProbeWithDependencies(host, ProbeDependencies{
		Allocations: backingResolver(func(device string) ([]capacityunit.Extent, error) {
			f.stateMu.RLock()
			defer f.stateMu.RUnlock()
			if extents, ok := f.extents[device]; ok {
				return append([]capacityunit.Extent(nil), extents...), nil
			}
			return nil, fmt.Errorf("device %s unavailable", device)
		}),
		InspectDirectory: inspectDirectory,
		Write: func(path string) error {
			f.countsMu.Lock()
			f.writes[filepath.Base(path)]++
			f.countsMu.Unlock()
			return writeProbe(path)
		},
		StatFS: func(string) (capacity.Filesystem, error) {
			return capacity.Filesystem{TotalBytes: 2 << 30, AvailableBytes: 1 << 30, AvailableInodes: 64}, nil
		},
		ListMounts: func() ([]mountutils.MountInfo, error) {
			f.stateMu.RLock()
			defer f.stateMu.RUnlock()
			return append([]mountutils.MountInfo(nil), f.mounts...), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.reconciler, err = NewReconciler("node", f.registry, f.probe, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f.reconciler.Now = func() time.Time { return testTime }
	f.reconciler.Inventory = func(_ context.Context, pool volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
		f.countsMu.Lock()
		f.scans[pool.Name]++
		f.countsMu.Unlock()
		if err := f.probe.VerifyBacking(pool); err != nil {
			return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Message: err.Error()}
		}
		return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}
	}
	return f
}

func (f *backingFixture) pool(t *testing.T, name string) volumeapi.Pool {
	t.Helper()
	pool, err := f.registry.PoolForIdentity(context.Background(), name, name+"-uid", "node")
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func (f *backingFixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestApprovedPeerLossPreservesOwnObservation(t *testing.T) {
	f := newBackingFixture(t)
	f.reconcile(t)
	for _, name := range []string{"a", "b"} {
		if pool := f.pool(t, name); !pool.RegistrationApproved() || pool.Status.CapacityUnit == nil || pool.Status.MountIdentity == nil {
			t.Fatalf("initial registration did not converge: %+v", pool)
		}
	}
	originalB := f.pool(t, "b")
	mounts := append([]mountutils.MountInfo(nil), f.mounts...)
	f.mounts = f.mounts[:2] // Approved B loses its filesystem; A is unchanged.
	f.reconcile(t)
	a, b := f.pool(t, "a"), f.pool(t, "b")
	accessible := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionAccessible)
	if accessible == nil || accessible.Status != metav1.ConditionTrue || f.writes["a"] != 2 || f.scans["a"] != 2 || !completeInventory(a.Status.Inventory) {
		t.Fatalf("healthy own probe/inventory stopped: accessible=%+v writes=%v scans=%v inventory=%+v", accessible, f.writes, f.scans, a.Status.Inventory)
	}
	if _, err := f.registry.ReadyPoolForIdentity(context.Background(), "a", "a-uid", "node"); err == nil {
		t.Fatal("unknown peer backing admitted a new allocation")
	}
	if ready, reason := a.CleanupReadyAt(testTime, time.Minute); !ready {
		t.Fatalf("healthy existing work lost own backing: %s", reason)
	}
	if !b.RegistrationApproved() || !volumeapi.SameCapacityUnit(b.Status.CapacityUnit, originalB.Status.CapacityUnit) || completeInventory(b.Status.Inventory) {
		t.Fatalf("missing peer lost its anchor or fabricated inventory: %+v", b.Status)
	}
	f.mounts = mounts
	f.reconcile(t)
	for _, name := range []string{"a", "b"} {
		if _, err := f.registry.ReadyPoolForIdentity(context.Background(), name, name+"-uid", "node"); err != nil {
			t.Fatalf("restored peer did not recover admission: %v", err)
		}
	}
}

func TestApprovedOwnBackingChangeNeverUsesOldAnchorAsEvidence(t *testing.T) {
	f := newBackingFixture(t)
	f.reconcile(t)
	original := f.pool(t, "a")
	f.extents["8:1"] = []capacityunit.Extent{{Device: "8:0", Start: 300, Sectors: 100}}
	f.reconcile(t)
	a, b := f.pool(t, "a"), f.pool(t, "b")
	backing := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionCapacityBackingVerified)
	if backing == nil || backing.Status != metav1.ConditionFalse || backing.Reason != "CapacityIdentityChanged" ||
		!volumeapi.SameCapacityUnit(a.Status.CapacityUnit, original.Status.CapacityUnit) || !a.RegistrationApproved() ||
		completeInventory(a.Status.Inventory) || f.writes["a"] != 1 || f.scans["a"] != 1 {
		t.Fatalf("changed own allocation reused or replaced its anchor: status=%+v writes=%v scans=%v", a.Status, f.writes, f.scans)
	}
	if ready, _ := a.ExecutionReadyAt(testTime, time.Minute); ready {
		t.Fatal("changed backing authorized an existing effect")
	}
	if ready, reason := b.CleanupReadyAt(testTime, time.Minute); !ready || f.scans["b"] != 2 {
		t.Fatalf("healthy peer stopped: ready=%v reason=%s scans=%v", ready, reason, f.scans)
	}
}

func TestApprovedFullPoolPreservesExactExistingWorkAndRecovers(t *testing.T) {
	for _, exhausted := range []string{"bytes", "inodes"} {
		t.Run(exhausted, func(t *testing.T) {
			f := newBackingFixture(t)
			f.reconcile(t)
			original := f.pool(t, "a")
			statFS := f.probe.statFS
			f.probe.statFS = func(path string) (capacity.Filesystem, error) {
				stats, err := statFS(path)
				if filepath.Base(path) == "a" {
					if exhausted == "bytes" {
						stats.AvailableBytes = 0
					} else {
						stats.AvailableInodes = 0
					}
				}
				return stats, err
			}
			f.reconcile(t)
			pool := f.pool(t, "a")
			if ready, reason := pool.ReadyAt(testTime, time.Minute); ready || reason != "NoSpace" {
				t.Fatalf("full Pool admitted new placement: ready=%v reason=%s", ready, reason)
			}
			if !pool.RegistrationApproved() || !volumeapi.SameCapacityUnit(pool.Status.CapacityUnit, original.Status.CapacityUnit) ||
				pool.Status.MountIdentity == nil || *pool.Status.MountIdentity != *original.Status.MountIdentity ||
				!completeInventory(pool.Status.Inventory) || f.writes["a"] != 2 || f.scans["a"] != 2 {
				t.Fatalf("allocation headroom changed existing backing or inventory: status=%+v writes=%v scans=%v", pool.Status, f.writes, f.scans)
			}
			for name, check := range map[string]func(time.Time, time.Duration) (bool, string){
				"cleanup": pool.CleanupReadyAt, "execution": pool.ExecutionReadyAt,
			} {
				if allowed, reason := check(testTime, time.Minute); !allowed {
					t.Fatalf("%s lost exact successful backing/write evidence: %s", name, reason)
				}
			}
			if err := f.probe.VerifyBacking(pool); err != nil {
				t.Fatalf("live own backing changed with headroom: %v", err)
			}
			if _, err := f.registry.ReadyPoolForIdentity(context.Background(), "b", "b-uid", "node"); err != nil {
				t.Fatalf("full Pool blocked an independent healthy peer: %v", err)
			}
			f.probe.statFS = statFS
			f.reconcile(t)
			if _, err := f.registry.ReadyPoolForIdentity(context.Background(), "a", "a-uid", "node"); err != nil {
				t.Fatalf("restored headroom did not recover allocation: %v", err)
			}
		})
	}
}

func TestEarlyProbeReturnReplacesSameGenerationBackingSuccess(t *testing.T) {
	for _, scenario := range []string{"directory unavailable", "mount lost after allocation read"} {
		t.Run(scenario, func(t *testing.T) {
			f := newBackingFixture(t)
			f.reconcile(t)
			later := testTime.Add(time.Second)
			f.reconciler.Now = func() time.Time { return later }
			f.probe.inspect = func(path string) error {
				if filepath.Base(path) == "a" {
					if scenario == "directory unavailable" {
						return os.ErrPermission
					}
					f.stateMu.Lock()
					f.mounts = []mountutils.MountInfo{f.mounts[0], f.mounts[len(f.mounts)-1]}
					f.stateMu.Unlock()
				}
				return inspectDirectory(path)
			}
			f.reconcile(t)
			a := f.pool(t, "a")
			backing := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionCapacityBackingVerified)
			if backing == nil || backing.Status != metav1.ConditionUnknown || backing.ObservedGeneration != a.Generation ||
				!a.Status.LastProbeTime.Time.Equal(later) || completeInventory(a.Status.Inventory) || f.scans["a"] != 1 {
				t.Fatalf("early return refreshed old own evidence: status=%+v scans=%v", a.Status, f.scans)
			}
			if ready, _ := a.ExecutionReadyAt(later, time.Minute); ready {
				t.Fatal("partial probe authorized an existing effect")
			}
		})
	}
}

func TestRegistrationRecheckNeverApprovesOwnHealthyOverlappingCandidate(t *testing.T) {
	f := newBackingFixture(t)
	inventory := f.reconciler.Inventory
	f.reconciler.Inventory = func(ctx context.Context, pool volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
		result := inventory(ctx, pool, now)
		if pool.Name == "a" {
			// The first complete scan has succeeded. Only the final admission
			// recheck can observe this newly conflicting peer allocation.
			f.stateMu.Lock()
			f.extents["8:2"] = append([]capacityunit.Extent(nil), f.extents["8:1"]...)
			f.stateMu.Unlock()
		}
		return result
	}
	f.reconcile(t)
	for _, name := range []string{"a", "b"} {
		pool := f.pool(t, name)
		if pool.RegistrationApproved() || pool.Status.CapacityUnit != nil || pool.Status.MountIdentity != nil {
			t.Fatalf("own evidence bypassed the final independence check: %+v", pool.Status)
		}
	}
	a := f.pool(t, "a")
	backing := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionCapacityBackingVerified)
	independent := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionCapacityIndependent)
	if backing == nil || backing.Status != metav1.ConditionTrue || independent == nil || independent.Status != metav1.ConditionFalse || f.scans["a"] != 1 {
		t.Fatalf("did not exercise own success with admission rejection: %+v scans=%v", a.Status, f.scans)
	}
}

func TestHelperMountCheckUsesCurrentOwnConditionWithoutTimeBudget(t *testing.T) {
	f := newBackingFixture(t)
	f.reconcile(t)
	f.mounts = f.mounts[:2]
	f.reconcile(t)
	a := f.pool(t, "a")
	a.Status.LastProbeTime = metav1.NewTime(testTime.Add(-24 * time.Hour))
	root := filepath.Join(f.probe.HostRoot, "a")
	if err := f.probe.verifyObservedMountedPath(root, a); err != nil {
		t.Fatalf("existing helper gained a new freshness or peer gate: %v", err)
	}
	if err := f.probe.VerifyPool(a); err == nil {
		t.Fatal("allocation verification lost its independent observation gate")
	}
	oldGeneration := a.Generation
	a.Generation++
	if err := f.probe.verifyObservedMountedPath(root, a); err == nil {
		t.Fatal("helper bypassed the current-generation own condition")
	}
	a.Generation = oldGeneration
	f.mounts[1].Source = "/dev/replacement"
	if err := f.probe.verifyObservedMountedPath(root, a); err == nil {
		t.Fatal("helper trusted condition without checking its live bound mount")
	}
}

func TestRegistrationRecheckRejectsOwnBackingChangeAfterInventory(t *testing.T) {
	f := newBackingFixture(t)
	inventory := f.reconciler.Inventory
	f.reconciler.Inventory = func(ctx context.Context, pool volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
		result := inventory(ctx, pool, now)
		if pool.Name == "a" {
			f.stateMu.Lock()
			f.extents["8:1"] = []capacityunit.Extent{{Device: "8:0", Start: 300, Sectors: 100}}
			f.stateMu.Unlock()
		}
		return result
	}
	f.reconcile(t)
	a := f.pool(t, "a")
	backing := meta.FindStatusCondition(a.Status.Conditions, volumeapi.PoolConditionCapacityBackingVerified)
	if a.RegistrationApproved() || a.Status.CapacityUnit != nil || a.Status.MountIdentity != nil ||
		backing == nil || backing.Status != metav1.ConditionFalse || backing.Reason != "CapacityIdentityChanged" || f.scans["a"] != 1 {
		t.Fatalf("final own-backing failure was not retained: status=%+v scans=%v", a.Status, f.scans)
	}
}
