package readiness

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type slowPoolStatus struct {
	*fakeRepository
	blocked bool
}

func (r *slowPoolStatus) SetPoolStatus(ctx context.Context, name, uid, node string, status volumeapi.PoolStatus) error {
	if name == "pool-a" && r.blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.fakeRepository.SetPoolStatus(ctx, name, uid, node, status)
}

func TestPoolDeadlineAllowsIndependentObservationAndRecovery(t *testing.T) {
	for _, boundary := range []string{"status", "inventory", "release"} {
		t.Run(boundary, func(t *testing.T) {
			ok := Check{Known: true, OK: true, Reason: "OK"}
			inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok, Filesystem: testFilesystem}}
			a := testRegistration(volumeapi.Pool{Name: "pool-a", UID: "a", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock})
			b := testRegistration(volumeapi.Pool{Name: "pool-b", UID: "b", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock})
			for _, pool := range []*volumeapi.Pool{&a, &b} {
				pool.Status.RegistrationApproved = true
				pool.Status.CapacityUnit, _ = inspector.InspectCapacityUnit(*pool)
			}
			if boundary == "release" {
				stamp := metav1.NewTime(testTime)
				a.DeletionTimestamp = &stamp
				a.IdentityReleaseApproval = a.UID
			}
			repository := &slowPoolStatus{fakeRepository: &fakeRepository{pools: []volumeapi.Pool{a, b}}, blocked: boundary == "status"}
			r, err := NewReconciler("node", repository, inspector, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			r.PoolTimeout = 10 * time.Millisecond
			blocked := true
			r.Inventory = func(ctx context.Context, pool volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
				if blocked && boundary == "inventory" && pool.Name == a.Name {
					<-ctx.Done()
					return volumeapi.PoolInventory{Message: ctx.Err().Error()}
				}
				return volumeapi.PoolInventory{Valid: true}
			}
			r.Release = func(ctx context.Context, pool volumeapi.Pool) error {
				if blocked && boundary == "release" {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := r.Reconcile(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("missing Pool timeout: %v", err)
			}
			if _, ok := repository.statuses[b.Name]; !ok || ctx.Err() != nil {
				t.Fatal("one Pool deadline prevented its peer observation")
			}
			if _, ok := repository.statuses[a.Name]; ok {
				t.Fatal("expired Pool observation published status")
			}
			waitPoolWorkers(t, r)
			blocked, repository.blocked = false, false
			if err := r.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			if _, ok := repository.statuses[a.Name]; !ok {
				t.Fatal("Pool did not recover after dependency resumed")
			}
		})
	}
}

func TestCanceledPassDoesNotStartPoolWork(t *testing.T) {
	repository := &fakeRepository{pool: volumeapi.Pool{Name: "pool", UID: "uid", NodeName: "node"}}
	r, err := NewReconciler("node", repository, fakeInspector{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Reconcile(ctx); !errors.Is(err, context.Canceled) || repository.statusSets != 0 {
		t.Fatalf("canceled pass wrote status: error=%v writes=%d", err, repository.statusSets)
	}
}
