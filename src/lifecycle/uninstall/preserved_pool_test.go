package uninstall

import (
	"context"
	"errors"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestExplicitRetiredPoolUninstallPreservesMissingInventory(t *testing.T) {
	f := newRetirementFixture()
	f.repo.pools[0].RetirementApproval = f.pool.UID
	f.repo.pools[0].Finalizers = []string{PoolProtectionFinalizer}
	f.checker.StorageClassNames = []string{"shiftpv"}
	f.checker.PreserveRetiredPoolUIDs = []string{f.pool.UID}
	report, err := f.checker.Check(context.Background())
	if err != nil || !report.Safe() {
		t.Fatalf("explicit preservation cannot uninstall with original Pool retained: blockers=%+v err=%v", report.Blockers, err)
	}
	if err := f.checker.ReleasePoolProtection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.repo.removed) != 0 {
		t.Fatalf("preserved Pool finalizer removed: %v", f.repo.removed)
	}
	if len(f.repo.pools) != 1 || f.repo.pools[0].UID != f.pool.UID || f.repo.pools[0].Status.Inventory != nil || f.repo.pools[0].Finalizers[0] != volumeapi.PoolProtectionFinalizer {
		t.Fatal("original Pool identity or finalizer changed")
	}
}

func newPreservationFixture() retirementFixture {
	f := newRetirementFixture()
	f.repo.pools[0].RetirementApproval = f.pool.UID
	f.repo.pools[0].Finalizers = []string{PoolProtectionFinalizer}
	f.checker.StorageClassNames = []string{"shiftpv"}
	f.checker.PreserveRetiredPoolUIDs = []string{f.pool.UID}
	return f
}

func TestPreservedRetirementDoesNotAuthorizePoolDeletion(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprint("terminating=", terminating), func(t *testing.T) {
			f := newPreservationFixture()
			if terminating {
				at := metav1.Now()
				f.repo.pools[0].DeletionTimestamp = &at
			}
			report, err := f.checker.Check(context.Background())
			if err != nil || !report.Safe() {
				t.Fatalf("preservation denied: %+v, %v", report, err)
			}
			report, err = f.checker.CheckPoolDeleteAfter(context.Background(), f.pool.Name, types.UID(f.pool.UID), time.Time{})
			if err != nil || report.Safe() {
				t.Fatalf("preservation granted deletion: %+v, %v", report, err)
			}
			for range 2 {
				if err := f.checker.ReleasePoolProtection(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if len(f.repo.removed) != 0 {
				t.Fatalf("preserved finalizer released: %v", f.repo.removed)
			}
		})
	}
}

func TestPreservedRetirementRequiresExactApprovedProtectedIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*retirementFixture)
	}{
		{"default remains closed", func(f *retirementFixture) { f.checker.PreserveRetiredPoolUIDs = nil }},
		{"unknown UID", func(f *retirementFixture) { f.checker.PreserveRetiredPoolUIDs = []string{"unknown"} }},
		{"empty UID", func(f *retirementFixture) { f.checker.PreserveRetiredPoolUIDs = []string{""} }},
		{"name is not UID", func(f *retirementFixture) { f.checker.PreserveRetiredPoolUIDs = []string{f.pool.Name} }},
		{"one unknown among selected UIDs", func(f *retirementFixture) {
			f.checker.PreserveRetiredPoolUIDs = append(f.checker.PreserveRetiredPoolUIDs, "unknown")
		}},
		{"replacement UID at same name", func(f *retirementFixture) { f.repo.pools[0].UID = "replacement-uid" }},
		{"active registration", func(f *retirementFixture) { f.repo.pools[0].RetirementApproval = "" }},
		{"retirement request alone", func(f *retirementFixture) { f.repo.pools[0].RetirementApproval = "" }},
		{"old UID approval", func(f *retirementFixture) { f.repo.pools[0].RetirementApproval = "old-uid" }},
		{"unapproved registration", func(f *retirementFixture) { f.repo.pools[0].Status.RegistrationApproved = false }},
		{"unprotected registration", func(f *retirementFixture) { f.repo.pools[0].Finalizers = nil }},
		{"invalid path", func(f *retirementFixture) { f.repo.pools[0].MountPath = "/" }},
		{"ambiguous UID", func(f *retirementFixture) { f.repo.pools = append(f.repo.pools, f.repo.pools[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPreservationFixture()
			test.change(&f)
			report, err := f.checker.Check(context.Background())
			if err == nil && report.Safe() {
				t.Fatal("unsafe preservation allowed")
			}
			// Default finalizer release retains its existing prechecked-call contract.
			if len(f.checker.PreserveRetiredPoolUIDs) > 0 {
				if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
					t.Fatal("unsafe release allowed")
				}
				if len(f.repo.removed) != 0 {
					t.Fatalf("mutated before denial: %v", f.repo.removed)
				}
			}
		})
	}
}

func TestPreservationKeepsEveryDriverObligationClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*retirementFixture)
	}{
		{"old claim", func(f *retirementFixture) {
			class := "shiftpv"
			_ = f.kube.Tracker().Add(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "application", UID: "old-claim-uid"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &class}})
		}},
		{"peer PV", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(shiftPVPersistentVolumeOnNode("peer-pv", "other-node"))
		}},
		{"peer Volume", func(f *retirementFixture) {
			copy := retirementCopy("peer-uid")
			copy.PoolName, copy.NodeName = "peer", "other-node"
			f.owner(copy)
		}},
		{"initial reservation", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", InitialPool: &volumeapi.PoolReference{Name: f.pool.Name, UID: f.pool.UID}}
		}},
		{"legacy unbound creation", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", RequestName: "pvc", InitialNode: "node", CapacityBytes: 1, Finalizers: []string{volumeapi.VolumeProtectionFinalizer}}
		}},
		{"peer Move", func(f *retirementFixture) {
			f.repo.moves = []volumeapi.Move{{Name: "pending", Spec: volumeapi.MoveSpec{SourceNode: "other-node"}, Status: volumeapi.MoveStatus{Phase: "Pending"}}}
		}},
		{"preparation hold", func(f *retirementFixture) {
			f.repo.moves = []volumeapi.Move{{Name: "prepared", Spec: volumeapi.MoveSpec{SourceNode: "other-node"}, Status: volumeapi.MoveStatus{Phase: "Preparing", Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: f.pool.Name, UID: f.pool.UID}}}}}
		}},
		{"peer cleanup", func(f *retirementFixture) {
			copy := retirementCopy("peer-uid")
			copy.PoolName, copy.NodeName = "peer", "other-node"
			f.cleanup.items = []cleanupapi.Cleanup{{Name: "cleanup", Spec: cleanupapi.Spec{Target: *copy, Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move"}}, Status: cleanupapi.Status{Phase: cleanupapi.PhaseRunning}}}
		}},
		{"active helper Pod", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "helper", Namespace: "system"}, Spec: retirementPodSpec(f.pool.MountPath, f.pool.NodeName), Status: corev1.PodStatus{Phase: corev1.PodRunning}})
		}},
		{"terminating helper Pod", func(f *retirementFixture) {
			at := metav1.Now()
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "helper", Namespace: "system", DeletionTimestamp: &at}, Spec: retirementPodSpec(f.pool.MountPath, f.pool.NodeName), Status: corev1.PodStatus{Phase: corev1.PodRunning}})
		}},
		{"nonterminal Job without Pod", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "helper", Namespace: "system"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: retirementPodSpec(f.pool.MountPath, f.pool.NodeName)}}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPreservationFixture()
			test.setup(&f)
			report, err := f.checker.Check(context.Background())
			if err != nil || report.Safe() {
				t.Fatalf("uninstall lost obligation: report=%+v err=%v", report, err)
			}
			if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
				t.Fatal("release lost obligation")
			}
			if len(f.repo.removed) != 0 {
				t.Fatalf("mutated protection on denied uninstall: %v", f.repo.removed)
			}
		})
	}
}

func TestPreservationRechecksBeforeRelease(t *testing.T) {
	for _, change := range []string{"late hold", "same-name replacement"} {
		t.Run(change, func(t *testing.T) {
			f := newPreservationFixture()
			report, err := f.checker.Check(context.Background())
			if err != nil || !report.Safe() {
				t.Fatalf("initial check: %+v %v", report, err)
			}
			if change == "late hold" {
				f.repo.moves = []volumeapi.Move{{Name: "late", Spec: volumeapi.MoveSpec{SourceNode: "other-node"}, Status: volumeapi.MoveStatus{Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: f.pool.Name, UID: f.pool.UID}}}}}
			} else {
				f.repo.pools[0].UID = "replacement-uid"
			}
			if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
				t.Fatal("check/release race was accepted")
			}
			if len(f.repo.removed) != 0 {
				t.Fatalf("mutated before denial: %v", f.repo.removed)
			}
		})
	}
}

func TestPreservationObservationFailureStaysClosed(t *testing.T) {
	for _, resource := range []string{"pools", "volumes", "moves", "cleanups", "persistentvolumes", "persistentvolumeclaims", "pods", "jobs"} {
		t.Run(resource, func(t *testing.T) {
			f := newPreservationFixture()
			failure := errors.New("unavailable")
			switch resource {
			case "pools":
				f.repo.poolsErr = failure
			case "volumes":
				f.repo.volumesErr = failure
			case "moves":
				f.repo.movesErr = failure
			case "cleanups":
				f.cleanup.err = failure
			default:
				f.kube.PrependReactor("list", resource, func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, failure })
			}
			if _, err := f.checker.Check(context.Background()); err == nil {
				t.Fatal("unknown observation allowed uninstall")
			}
			if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
				t.Fatal("unknown observation allowed release")
			}
			if len(f.repo.removed) != 0 {
				t.Fatalf("mutated before denial: %v", f.repo.removed)
			}
		})
	}
}

func TestPreservationStillReleasesOnlyOrdinaryProvenEmptyPool(t *testing.T) {
	f := newPreservationFixture()
	now := time.Now().UTC()
	barrier := now.Add(-time.Second)
	f.repo.pools = append(f.repo.pools, volumeapi.Pool{Name: "empty", UID: "empty-uid", NodeName: "other-node", Finalizers: []string{PoolProtectionFinalizer}, Status: volumeapi.PoolStatus{Inventory: &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}}})
	f.checker.Now = func() time.Time { return now }
	report, err := f.checker.CheckAfter(context.Background(), barrier)
	if err != nil || !report.Safe() {
		t.Fatalf("mixed pools: %+v %v", report, err)
	}
	if err := f.checker.ReleasePoolProtectionAfter(context.Background(), barrier); err != nil {
		t.Fatal(err)
	}
	if len(f.repo.removed) != 1 || f.repo.removed[0] != "empty/empty-uid" {
		t.Fatalf("released wrong identities: %v", f.repo.removed)
	}
	f.repo.removed = nil
	f.repo.pools[1].Status.Inventory.ObservedAt = metav1.NewTime(barrier)
	if err := f.checker.ReleasePoolProtectionAfter(context.Background(), barrier); err == nil {
		t.Fatal("release discarded quiesce freshness barrier")
	}
	if len(f.repo.removed) != 0 {
		t.Fatal("released ordinary Pool with old inventory")
	}
}

func TestPreservedPoolStillReservesItsPathAfterReinstall(t *testing.T) {
	f := newPreservationFixture()
	if err := f.checker.ReleasePoolProtection(context.Background()); err != nil {
		t.Fatal(err)
	}
	retained := f.repo.pools[0]
	for _, path := range []string{retained.MountPath, retained.MountPath + "/nested"} {
		candidate := volumeapi.Pool{Name: "replacement", UID: "replacement-uid", NodeName: retained.NodeName, MountPath: path}
		if ok, reason := volumeapi.PoolCapacityIndependent(candidate, []volumeapi.Pool{retained, candidate}); ok || reason != "CapacityPathsOverlap" {
			t.Fatalf("reinstall bypassed retained path: path=%s ok=%v reason=%s", path, ok, reason)
		}
	}
}

type preservationRaceRepository struct {
	*memoryRepository
	registrations   int
	movesReads      int
	onRegistrations func(int)
	onMoves         func(int)
}

func (r *preservationRaceRepository) ListPoolRegistrations(ctx context.Context) ([]volumeapi.Pool, error) {
	r.registrations++
	if r.onRegistrations != nil {
		r.onRegistrations(r.registrations)
	}
	return r.memoryRepository.ListPoolRegistrations(ctx)
}
func (r *preservationRaceRepository) ListMoves(ctx context.Context) ([]volumeapi.Move, error) {
	r.movesReads++
	if r.onMoves != nil {
		r.onMoves(r.movesReads)
	}
	return r.memoryRepository.ListMoves(ctx)
}

func TestPreservationDoesNotReleaseRecreatedUIDAfterSafeRecheck(t *testing.T) {
	f := newPreservationFixture()
	r := &preservationRaceRepository{memoryRepository: f.repo}
	f.checker.Volumes = r
	r.onRegistrations = func(n int) {
		if n == 3 {
			r.pools[0].UID = "recreated-uid"
		}
	}
	report, err := f.checker.Check(context.Background())
	if err != nil || !report.Safe() {
		t.Fatalf("initial check: %+v %v", report, err)
	}
	if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
		t.Fatal("UID replacement between safe check and release was accepted")
	}
	if len(r.removed) != 0 {
		t.Fatalf("removed replacement identity: %v", r.removed)
	}
}

func TestPreservationRechecksLateHoldAfterGlobalScan(t *testing.T) {
	f := newPreservationFixture()
	r := &preservationRaceRepository{memoryRepository: f.repo}
	f.checker.Volumes = r
	r.onMoves = func(n int) {
		if n == 4 {
			r.moves = []volumeapi.Move{{Name: "late", Spec: volumeapi.MoveSpec{SourceNode: "other-node"}, Status: volumeapi.MoveStatus{Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: f.pool.Name, UID: f.pool.UID}}}}}
		}
	}
	report, err := f.checker.Check(context.Background())
	if err != nil || !report.Safe() {
		t.Fatalf("initial check: %+v %v", report, err)
	}
	if err := f.checker.ReleasePoolProtection(context.Background()); err == nil {
		t.Fatal("retirement approval hid a hold arriving after global scan")
	}
	if len(r.removed) != 0 {
		t.Fatalf("released with late hold: %v", r.removed)
	}
}
