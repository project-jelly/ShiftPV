package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

// Wait outside the fake client's mutex: this is an actual caller deadline,
// rather than an API timeout returned while the reconcile context stays live.
type deadlineLegacyStatusRepository struct {
	*volumeapi.Registry
	accepted bool
	attempts int
}

func (r *deadlineLegacyStatusRepository) SetMoveStatus(ctx context.Context, name, uid string, status volumeapi.MoveStatus) error {
	r.attempts++
	if r.accepted {
		if err := r.Registry.SetMoveStatus(ctx, name, uid, status); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestVolumeExpiryLegacyRecoveryRequiresFreshObservation(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "accepted"}[accepted], func(t *testing.T) {
			f, move := legacyRecoveryStatusFixture(t)
			protected := f.state(t, 0)
			repository := &deadlineLegacyStatusRepository{Registry: f.registry, accepted: accepted}
			f.reconciler.Repository = repository
			ctx, cancel := context.WithTimeout(f.ctx, 250*time.Millisecond)
			defer cancel()
			err := f.reconciler.reconcileRecovery(ctx, move)
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errMoveStatusPersistence) {
				t.Fatalf("caller expiry did not require re-observation: ctx=%v err=%v", ctx.Err(), err)
			}
			stored := f.move(t, 0)
			wantPhase := recoveryQuiescing
			if accepted {
				wantPhase = recoveryVerifying
			}
			if repository.attempts != 1 || stored.Status.RecoveryPhase != wantPhase || stored.Status.RecoveryReason != "" {
				t.Fatalf("expired write triggered stale diagnostics: attempts=%d status=%+v", repository.attempts, stored.Status)
			}
			if !accepted && !reflect.DeepEqual(stored, move) {
				t.Fatal("rejected write changed the Move")
			}
			if got := f.state(t, 0); !reflect.DeepEqual(got, protected) {
				t.Fatalf("expiry changed source authority: %+v", got)
			}
			assertNoExpiryJobs(t, f)

			restarted := newTestReconciler(f.client, f.registry)
			for range 2 {
				if err := restarted.reconcileRecovery(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
			}
			jobs, err := f.client.BatchV1().Jobs(restarted.Namespace).List(f.ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 1 || jobs.Items[0].Name != recoveryNames(move).Base+"-verify" ||
				!moveOwned(jobs.Items[0].OwnerReferences, move.UID) || f.move(t, 0).Status.RecoveryPhase != recoveryVerifying {
				t.Fatalf("fresh attempt did not resume exact verification: jobs=%+v err=%v", jobs, err)
			}
		})
	}
}

func TestVolumeExpiryPreparedApprovalRetainsSingleReservation(t *testing.T) {
	f, move := preparationAtCapacity(t)
	protected := f.state(t, 0)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	approvals := 0
	f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
		approved, _, _ := unstructured.NestedBool(object.Object, "status", "capacityApproved")
		if resource == volumeapi.MoveResource && approved {
			approvals++
			cancel()
			return ctx.Err()
		}
		return nil
	}
	err := f.reconciler.reconcileMove(ctx, move)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errCapacityApprovalPersistence) || ctx.Err() == nil {
		t.Fatalf("accepted approval did not stop on caller expiry: ctx=%v err=%v", ctx.Err(), err)
	}
	stored := f.move(t, 0)
	if approvals != 1 || stored.Status.Preparation != nil || !stored.Status.CapacityApproved || stored.Status.Phase != move.Status.Phase || stored.Status.Reason != "" {
		t.Fatalf("expired approval changed plan or replayed: writes=%d status=%+v", approvals, stored.Status)
	}
	if got := f.state(t, 0); !reflect.DeepEqual(got, protected) {
		t.Fatalf("approval expiry changed source: %+v", got)
	}
	assertNoExpiryJobs(t, f)
	assertPreparationReserved(t, f, preparationRequestedBytes)
	f.afterWrite = nil
	// Model the already-quiesced workload's controller creating its held
	// replacement, so fresh reconciliation can enter the real copy stage.
	workload := f.workloads[0]
	if err := f.client.CoreV1().Pods(workload.Namespace.Name).Delete(f.ctx, workload.Consumer.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := workload.Consumer.DeepCopy()
	replacement.Name, replacement.UID, replacement.ResourceVersion = "replacement", "", ""
	replacement.Spec.NodeName = ""
	replacement.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	replacement.Status.Phase = corev1.PodPending
	if _, err := f.client.CoreV1().Pods(replacement.Namespace).Create(f.ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	restarted := *f.reconciler
	if err := restarted.reconcileMove(f.ctx, stored); err != nil {
		t.Fatal(err)
	}
	fresh := f.move(t, 0)
	if !fresh.Status.CapacityApproved || fresh.Status.Preparation != nil ||
		!reflect.DeepEqual(fresh.Status.DestinationCopy, stored.Status.DestinationCopy) ||
		!reflect.DeepEqual(fresh.Status.IncomingCopy, stored.Status.IncomingCopy) {
		t.Fatalf("fresh attempt replaced durable approval: %+v", fresh.Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestVolumeExpirySourceUsageRetainsResultForFreshAttempt(t *testing.T) {
	for _, protocol := range []string{"", volumeapi.MoveProtocolPreparedV1} {
		t.Run("protocol="+protocol, func(t *testing.T) {
			f, move := preparationAtCapacity(t)
			if protocol == "" {
				object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, move.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
				unstructured.RemoveNestedField(object.Object, "status", "preparation")
				if _, err := f.commitDynamicUpdate(volumeapi.MoveResource, object); err != nil {
					t.Fatal(err)
				}
				move = f.move(t, 0)
			}
			installSourceUsageRunner(f)
			if err := f.reconciler.reconcileMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			pod := completeSourceUsageForTest(t, f, move)
			protected := f.state(t, 0)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			writes := 0
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				bytes, _, _ := unstructured.NestedInt64(object.Object, "status", "sourceBytes")
				if resource == volumeapi.MoveResource && bytes > 0 {
					writes++
					cancel()
					return ctx.Err()
				}
				return nil
			}
			f.client.ClearActions()
			err := f.reconciler.reconcileMove(ctx, move)
			if !errors.Is(err, context.Canceled) || !errors.Is(err, errMoveStatusPersistence) || ctx.Err() == nil {
				t.Fatalf("accepted usage did not stop on caller expiry: ctx=%v err=%v", ctx.Err(), err)
			}
			stored := f.move(t, 0)
			if writes != 1 || stored.Status.SourceBytes != 2097152 || stored.Status.CapacityApproved || stored.Status.Phase != move.Status.Phase || stored.Status.Reason != "" {
				t.Fatalf("expired usage write replayed or approved: writes=%d status=%+v", writes, stored.Status)
			}
			for _, action := range f.client.Actions() {
				if action.Matches("delete", "pods") || action.Matches("create", "jobs") {
					t.Fatal("expired usage write advanced helper retirement or disk work")
				}
			}
			if current := sourceUsagePodForTest(t, f, move); current.UID != pod.UID {
				t.Fatal("result Pod changed before fresh observation")
			}
			if got := f.state(t, 0); !reflect.DeepEqual(got, protected) {
				t.Fatalf("usage expiry changed source authority: %+v", got)
			}
			f.afterWrite = nil
			restarted := *f.reconciler
			restarted.SourceUsage = &helperpod.Runner{Client: f.client, Namespace: "system", Image: "new-image"}
			f.client.ClearActions()
			if err := restarted.reconcileMove(f.ctx, stored); err != nil {
				t.Fatal(err)
			}
			if got := f.move(t, 0); !got.Status.CapacityApproved || got.Status.SourceBytes != stored.Status.SourceBytes {
				t.Fatalf("fresh attempt failed to reuse stored bytes: %+v", got.Status)
			}
			for _, action := range f.client.Actions() {
				if action.Matches("create", "pods") {
					t.Fatal("fresh attempt repeated a durable measurement")
				}
			}
			pods, err := f.client.CoreV1().Pods("system").List(f.ctx, metav1.ListOptions{LabelSelector: helperpod.SourceUsagePurposeLabel + "=true"})
			if err != nil || len(pods.Items) != 0 {
				t.Fatalf("fresh attempt did not retire exact result Pod: pods=%d err=%v", len(pods.Items), err)
			}
			assertPreparationReserved(t, f, preparationRequestedBytes)
		})
	}
}

func TestVolumeExpiryCommittedOwnerIsNeverReversed(t *testing.T) {
	f := newLedgerHandoffFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writes := 0
	f.client.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		owner, _, _ := unstructured.NestedString(object.Object, "status", "ownerNode")
		if object.GetName() != f.move.Spec.VolumeID || owner != f.destinationPool.NodeName {
			return false, nil, nil
		}
		writes++
		if err := f.client.Tracker().Update(volumeapi.VolumeResource, object.DeepCopy(), ""); err != nil {
			return true, nil, err
		}
		cancel()
		return true, nil, ctx.Err()
	})
	if err := f.reconciler.commitOwner(ctx, &f.move, f.observed); !errors.Is(err, context.Canceled) || ctx.Err() == nil {
		t.Fatalf("accepted owner CAS did not return caller expiry: ctx=%v err=%v", ctx.Err(), err)
	}
	f.assertReserved(t, f.sourcePool.UID, handoffCapacity)
	f.assertReserved(t, f.destinationPool.UID, handoffCapacity)
	unlock, acquired := f.reconciler.PoolLocks.TryLock(f.destinationPool.UID)
	if !acquired {
		t.Fatal("owner CAS expiry retained Pool lock")
	}
	unlock()
	freshCtx := context.Background()
	restarted := *f.reconciler
	// The real Registry must recognize even a stale precommit retry as already
	// committed; a canceled response never grants authority to restore source.
	if err := restarted.commitOwner(freshCtx, &f.move, f.observed); err != nil {
		t.Fatal(err)
	}
	state, err := f.registry.Get(freshCtx, f.move.Spec.VolumeID)
	if err != nil || state.OwnerNode != f.destinationPool.NodeName || state.ActiveMove != f.move.Name ||
		!reflect.DeepEqual(state.CurrentCopy, f.move.Status.DestinationCopy) || writes != 1 {
		t.Fatalf("fresh retry rewrote or reversed owner: writes=%d state=%+v err=%v", writes, state, err)
	}
	observed := observation{Volume: state}
	observed.FSM.OwnerCommitted = true
	if err := restarted.commitOwner(freshCtx, &f.move, observed); err != nil {
		t.Fatal(err)
	}
	f.assertReserved(t, f.sourcePool.UID, handoffCapacity)
	f.assertReserved(t, f.destinationPool.UID, handoffCapacity)
}

func assertNoExpiryJobs(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	jobs, err := f.client.BatchV1().Jobs(f.reconciler.Namespace).List(f.ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("expired attempt started disk work: jobs=%+v err=%v", jobs, err)
	}
}

func TestVolumeExpiryBeforeDisruptionPreservesServingSource(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := preparationStartingDisruption(t, f)
	state := f.state(t, 0)
	placement, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.reconciler.ReconcileTimeout = 250 * time.Millisecond
	calls := 0
	f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(ctx context.Context, _ volumeapi.Pool) (poolcapacity.Filesystem, error) {
		calls++
		<-ctx.Done()
		return poolcapacity.Filesystem{}, errors.Join(ErrCapacityProbePending, ctx.Err())
	})
	// The longer parent bounds test failure; only the Volume child may expire.
	parent, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	work := volumeWork{volumeID: move.Spec.VolumeID, volumeUID: state.UID, moves: []moveReference{{name: move.Name, uid: move.UID}}}
	_, err = f.reconciler.advanceVolume(parent, work)
	if calls != 1 || parent.Err() != nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrCapacityProbePending) {
		t.Fatalf("Volume child did not expire at the probe: calls=%d parent=%v err=%v", calls, parent.Err(), err)
	}
	if got := f.move(t, 0); !reflect.DeepEqual(got, move) {
		t.Fatalf("expired preflight changed hold, phase or diagnostics: before=%+v after=%+v", move, got)
	}
	if got := f.state(t, 0); !reflect.DeepEqual(got, state) {
		t.Fatalf("expired preflight changed serving source: %+v", got)
	}
	current, err := f.client.CoreV1().Pods("system").Get(f.ctx, placement.Name, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(current, placement) {
		t.Fatalf("expired preflight changed exact placement: current=%+v err=%v", current, err)
	}
	assertPreparationOriginals(t, f)
	assertPreparationReserved(t, f, preparationRequestedBytes)
	assertNoExpiryJobs(t, f)

	f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
		calls++
		if ctx.Err() != nil {
			t.Fatalf("fresh attempt reused expired context: %v", ctx.Err())
		}
		if release, acquired := f.reconciler.PoolLocks.TryLock(pool.UID); acquired {
			release()
			t.Fatal("fresh probe lost its shared Pool lock")
		}
		return poolcapacity.Filesystem{AvailableBytes: 1}, nil // Reachability only.
	})
	if _, err := f.reconciler.advanceVolume(f.ctx, work); err != nil {
		t.Fatal(err)
	}
	recovered := f.move(t, 0)
	if calls != 2 || recovered.UID != move.UID || recovered.Status.Phase != "WaitingForUnpublish" ||
		recovered.Status.CapacityApproved || !reflect.DeepEqual(recovered.Status.Preparation, move.Status.Preparation) || f.state(t, 0).Phase != volumeapi.PhaseMoving {
		t.Fatalf("fresh attempt did not resume the exact hold: calls=%d status=%+v", calls, recovered.Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}
