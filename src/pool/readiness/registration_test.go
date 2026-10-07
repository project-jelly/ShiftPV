package readiness

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRejectedCandidateDoesNotStopApprovedPool(t *testing.T) {
	for _, scenario := range []string{"legacy", "shared", "unproven", "nested", "invalid capacity", "fractional capacity", "overflow capacity"} {
		t.Run(scenario, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK", Message: "ok"}
			inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}}
			a := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1})
			a.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(a)
			a.Status.RegistrationApproved = true
			b := testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1})
			var checks Inspector = inspector
			switch scenario {
			case "legacy":
				b.CapacityPolicy = ""
			case "shared":
				inspector.shared = true
				checks = inspector
			case "unproven":
				checks = changedPeerInspector{inspector}
			case "nested":
				b.MountPath = a.MountPath + "/nested"
			case "invalid capacity":
				b.CapacityLimit = "-1Gi"
			case "fractional capacity":
				b.CapacityLimit = "1500m"
			case "overflow capacity":
				b.CapacityLimit = "9223372036854775808"
			}
			repo := &fakeRepository{pools: []volumeapi.Pool{b, a}}
			scanned := map[string]int{}
			r, err := NewReconciler("node", repo, checks, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			r.Inventory = func(_ context.Context, p volumeapi.Pool, _ time.Time) volumeapi.PoolInventory {
				scanned[p.Name]++
				return volumeapi.PoolInventory{Valid: true}
			}
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			good, bad := repo.statuses[a.Name], repo.statuses[b.Name]
			ready := meta.FindStatusCondition(good.Conditions, volumeapi.PoolConditionReady)
			rejected := meta.FindStatusCondition(bad.Conditions, volumeapi.PoolConditionReady)
			if ready == nil || ready.Status != metav1.ConditionTrue || !good.RegistrationApproved || rejected == nil || rejected.Status != metav1.ConditionFalse || bad.RegistrationApproved || bad.CapacityUnit != nil || scanned[b.Name] != 0 {
				t.Fatalf("good=%+v bad=%+v scans=%v", good, bad, scanned)
			}
		})
	}
}

func TestApprovedPoolLimitErrorPreservesLivePeerEvidence(t *testing.T) {
	for _, limit := range []string{"not-a-quantity", "1500m", "9223372036854775808"} {
		t.Run(limit, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK"}
			inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}}
			a := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1})
			b := testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 2, CapacityLimit: limit})
			for _, p := range []*volumeapi.Pool{&a, &b} {
				p.Status.RegistrationApproved = true
				p.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(*p)
			}
			repo := &fakeRepository{pools: []volumeapi.Pool{b, a}}
			r, _ := NewReconciler("node", repo, inspector, time.Minute)
			r.Now = func() time.Time { return testTime }
			scanned := map[string]int{}
			r.Inventory = func(_ context.Context, p volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
				scanned[p.Name]++
				return volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}
			}
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			a.Status, b.Status = repo.statuses[a.Name], repo.statuses[b.Name]
			if ready, reason := a.ReadyAt(testTime, time.Minute); !ready {
				t.Fatalf("healthy Pool blocked: %s", reason)
			}
			if ready, reason := b.ReadyAt(testTime, time.Minute); ready || reason != "PoolCapacityLimitInvalid" {
				t.Fatalf("bad limit admitted: ready=%v reason=%s", ready, reason)
			}
			if independent, reason := volumeapi.PoolCapacityIndependent(a, []volumeapi.Pool{a, b}); !independent {
				t.Fatalf("controller lost peer proof: %s", reason)
			}
			if scanned[a.Name] != 1 || scanned[b.Name] != 1 || !b.Status.RegistrationApproved || !completeInventory(b.Status.Inventory) {
				t.Fatalf("scans=%v status=%+v", scanned, b.Status)
			}
		})
	}
}

func TestInvalidLimitDoesNotBlockApprovedCleanup(t *testing.T) {
	for _, policy := range []string{"", volumeapi.PoolCapacityPolicyFixedBlock} {
		for _, deleting := range []bool{false, true} {
			name := "legacy"
			if policy != "" {
				name = "fixed"
			}
			if deleting {
				name += "/deleting"
			}
			t.Run(name, func(t *testing.T) {
				ok := Check{Known: true, OK: true, Reason: "OK"}
				inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}}
				pool := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", Generation: 2, CapacityPolicy: policy, CapacityLimit: "not-a-quantity"})
				pool.Status.RegistrationApproved = true
				if policy != "" {
					pool.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(pool)
				}
				if deleting {
					timestamp := metav1.NewTime(testTime)
					pool.DeletionTimestamp = &timestamp
					pool.IdentityReleaseApproval = pool.UID
				}
				repo := &fakeRepository{pool: pool}
				r, _ := NewReconciler("node", repo, inspector, time.Minute)
				r.Now = func() time.Time { return testTime }
				scans, releases := 0, 0
				r.Inventory = func(_ context.Context, _ volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
					scans++
					return volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(now)}
				}
				r.Release = func(_ context.Context, p volumeapi.Pool) error {
					if p.UID != pool.UID || p.IdentityReleaseApproval != p.UID {
						t.Fatal("release identity mismatch")
					}
					releases++
					return nil
				}
				if err := r.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
				pool.Status = repo.status
				if ready, reason := pool.CleanupReadyAt(testTime, time.Minute); !ready {
					t.Fatalf("cleanup blocked by placement limit: %s", reason)
				}
				wantReleases := 0
				if deleting {
					wantReleases = 1
				}
				if scans != 1 || releases != wantReleases || !completeInventory(pool.Status.Inventory) {
					t.Fatalf("scans=%d releases=%d status=%+v", scans, releases, pool.Status)
				}
			})
		}
	}
}

func TestInvalidLimitDoesNotBypassCleanupGuards(t *testing.T) {
	for _, scenario := range []string{"backing unproven", "mount missing", "inventory truncated", "release not approved"} {
		t.Run(scenario, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK"}
			inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}}
			pool := testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", Generation: 2, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, CapacityLimit: "not-a-quantity"})
			pool.Status.RegistrationApproved = true
			pool.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(pool)
			timestamp := metav1.NewTime(testTime)
			pool.DeletionTimestamp = &timestamp
			pool.IdentityReleaseApproval = pool.UID
			var checks Inspector = inspector
			switch scenario {
			case "backing unproven":
				checks = changedPeerInspector{inspector}
			case "mount missing":
				pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
				pool.Status.MountIdentity = &volumeapi.PoolMountIdentity{Device: "8:2", Root: "/", Source: "/dev/test", Filesystem: "ext4"}
				inspector.result.Mounted = Check{Known: true, Reason: "MountMissing"}
				checks = inspector
			case "release not approved":
				pool.IdentityReleaseApproval = "different-uid"
			}
			repo := &fakeRepository{pool: pool}
			r, _ := NewReconciler("node", repo, checks, time.Minute)
			r.Now = func() time.Time { return testTime }
			scans, releases := 0, 0
			r.Inventory = func(_ context.Context, _ volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
				scans++
				return volumeapi.PoolInventory{Valid: true, Truncated: scenario == "inventory truncated", ObservedAt: metav1.NewTime(now)}
			}
			r.Release = func(context.Context, volumeapi.Pool) error { releases++; return nil }
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			pool.Status = repo.status
			if releases != 0 {
				t.Fatal("incomplete evidence or missing exact approval released identity")
			}
			if scenario == "backing unproven" || scenario == "mount missing" {
				if ready, _ := pool.CleanupReadyAt(testTime, time.Minute); ready || scans != 0 {
					t.Fatalf("unsafe backing admitted: status=%+v scans=%d", pool.Status, scans)
				}
			}
		})
	}
}

type registrationRepository struct {
	*fakeRepository
	reads  int
	change func(int, *fakeRepository)
}

func (r *registrationRepository) ListPoolRegistrations(ctx context.Context) ([]volumeapi.Pool, error) {
	r.reads++
	if r.change != nil {
		r.change(r.reads, r.fakeRepository)
	}
	return r.fakeRepository.ListPoolRegistrations(ctx)
}

func TestRegistrationRechecksAfterInventory(t *testing.T) {
	for _, scenario := range []string{"peer appeared", "generation changed", "API failed"} {
		t.Run(scenario, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK"}
			inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}, shared: true}
			a := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1})
			repo := &registrationRepository{fakeRepository: &fakeRepository{pools: []volumeapi.Pool{a}}}
			repo.change = func(n int, f *fakeRepository) {
				if n != 2 {
					return
				}
				switch scenario {
				case "peer appeared":
					b := a
					b.Name = "pool-b"
					b.UID = "b"
					b.MountPath = "/b"
					f.pools = append(f.pools, b)
				case "generation changed":
					f.pools[0].Generation++
				case "API failed":
					f.err = errors.New("API unavailable")
				}
			}
			r, _ := NewReconciler("node", repo, inspector, time.Minute)
			r.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
				return volumeapi.PoolInventory{Valid: true}
			}
			err := r.Reconcile(context.Background())
			if scenario != "peer appeared" && err == nil {
				t.Fatal("changed or unavailable snapshot approved")
			}
			for _, s := range repo.statuses {
				if s.RegistrationApproved || s.CapacityUnit != nil {
					t.Fatalf("stale approval=%+v", s)
				}
			}
			if scenario == "peer appeared" {
				c := meta.FindStatusCondition(repo.status.Conditions, volumeapi.PoolConditionReady)
				if c == nil || c.Status != metav1.ConditionFalse {
					t.Fatalf("conflict=%+v", c)
				}
			}
		})
	}
}

func TestRegistrationHistorySurvivesFailedProbe(t *testing.T) {
	pool := testRegistration(volumeapi.Pool{Name: "pool", UID: "uid", NodeName: "node", Generation: 1})
	pool.Status.Conditions = []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue}}
	pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true}
	r := &Reconciler{}
	status, _ := r.observedStatus(context.Background(), pool, Result{Accessible: Check{Known: true, Reason: "PathMissing"}}, testTime)
	if !status.RegistrationApproved {
		t.Fatal("legacy approval forgotten after failure")
	}
	pool.Status = status
	status, _ = r.observedStatus(context.Background(), pool, Result{}, testTime)
	if !status.RegistrationApproved {
		t.Fatal("approval forgotten on subsequent failure")
	}
}

func TestNewReconcilerRejectsMissingDependencies(t *testing.T) {
	for _, tc := range []struct {
		node      string
		repo      Repository
		inspector Inspector
		interval  time.Duration
	}{
		{"", &fakeRepository{}, fakeInspector{}, time.Minute},
		{"node", nil, fakeInspector{}, time.Minute},
		{"node", &fakeRepository{}, nil, time.Minute},
		{"node", &fakeRepository{}, fakeInspector{}, 0},
	} {
		if r, err := NewReconciler(tc.node, tc.repo, tc.inspector, tc.interval); err == nil || r != nil {
			t.Fatalf("incomplete configuration accepted: %v %v", r, err)
		}
	}
}

func TestConcurrentConflictingCandidatesNeverApprove(t *testing.T) {
	ok := Check{Known: true, OK: true, Reason: "OK"}
	writes := 0
	inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}, shared: true, inspects: &writes}
	repo := &fakeRepository{pools: []volumeapi.Pool{
		testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1}),
		testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, Generation: 1}),
	}}
	r, _ := NewReconciler("node", repo, inspector, time.Minute)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- r.Reconcile(context.Background()) }()
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if writes != 0 || repo.statusSets != 4 {
		t.Fatalf("writes=%d statuses=%d", writes, repo.statusSets)
	}
	for _, s := range repo.statuses {
		if s.RegistrationApproved || s.CapacityUnit != nil {
			t.Fatalf("conflicting registration approved: %+v", s)
		}
	}
}
