package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type preparationCapacityProbeFunc func(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error)

func (f preparationCapacityProbeFunc) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
	return f(ctx, pool)
}

func setPreparationProbeSupported(t *testing.T, f *preparationRegistryFixture, name string, supported bool) {
	t.Helper()
	pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = unstructured.SetNestedField(pool.Object, supported, "status", "capacityProbeSupported"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.dynamic.Resource(volumeapi.PoolResource).UpdateStatus(f.ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// Same-node registrations need real independent fixed-allocation evidence.
func preparationFixedPool(pool *unstructured.Unstructured, device string, start int64) {
	_ = unstructured.SetNestedField(pool.Object, "FixedBlock", "spec", "capacityPolicy")
	_ = unstructured.SetNestedMap(pool.Object, map[string]any{
		"device": device, "source": "/dev/test-" + device, "filesystem": "ext4",
		"extents": []any{map[string]any{"device": "8:0", "start": start, "sectors": int64(4096)}},
	}, "status", "capacityUnit")
	conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
	conditions = append(conditions, map[string]any{"type": volumeapi.PoolConditionCapacityIndependent,
		"status": "True", "reason": "Independent", "observedGeneration": int64(1)})
	_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
}

func TestPreparationCapacityProbeFiltersPoolsWithinOneNode(t *testing.T) {
	for _, sibling := range []bool{false, true} {
		t.Run(map[bool]string{false: "no supported Pool", true: "supported sibling Pool"}[sibling], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			setPreparationProbeSupported(t, f, "destination-pool", false)
			if sibling {
				original, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				preparationFixedPool(original, "8:1", 0)
				if err = f.dynamic.Tracker().Update(volumeapi.PoolResource, original, ""); err != nil {
					t.Fatal(err)
				}
				pool := preparationPool("destination", "5Gi")
				pool.SetName("destination-supported")
				pool.SetUID("destination-supported-uid")
				preparationFixedPool(pool, "8:2", 4096)
				_ = unstructured.SetNestedField(pool.Object, "/supported", "spec", "mountPath")
				if err := f.dynamic.Tracker().Create(volumeapi.PoolResource, pool, ""); err != nil {
					t.Fatal(err)
				}
			}
			for range 5 {
				if err := f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				f.schedulePlacements(t)
				if f.move(t, 0).Status.Preparation != nil {
					break
				}
			}
			move := f.move(t, 0)
			assertPreparationOriginals(t, f)
			if sibling {
				if move.Status.Preparation == nil || move.Status.Preparation.Pool.Name != "destination-supported" {
					t.Fatalf("unsupported Pool hid or won over its supported sibling: %+v", move.Status)
				}
				return
			}
			if move.Status.Preparation != nil || move.Status.Reason != "DestinationCapacityProbeUnavailable" {
				t.Fatalf("missing probe was treated as logical capacity exhaustion: %+v", move.Status)
			}
			pods, err := f.client.CoreV1().Pods("system").List(f.ctx, metav1.ListOptions{})
			if err != nil || len(pods.Items) != 0 {
				t.Fatalf("unsupported destination consumed scheduler resources: %+v, %v", pods, err)
			}
			setPreparationProbeSupported(t, f, "destination-pool", true)
			recovered := f.untilPrepared(t, 0)
			if recovered.UID != move.UID {
				t.Fatal("capability recovery replaced the Move")
			}
		})
	}
}

func TestPreparationCapacityProbeRechecksCapabilityBeforeGrant(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	for range 3 {
		if err := f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
		f.schedulePlacements(t)
	}
	f.reconciler.Repository = &preparationPoolReadRepository{Repository: f.registry, beforeReady: func() {
		setPreparationProbeSupported(t, f, "destination-pool", false)
	}}
	if err := f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	if move := f.move(t, 0); move.Status.Preparation != nil || move.Status.Reason != "DestinationCapacityProbeUnavailable" {
		t.Fatalf("advisory capability was reused for a fresh hold: %+v", move.Status)
	}
	assertPreparationOriginals(t, f)
}

func preparationStartingDisruption(t *testing.T, f *preparationRegistryFixture) volumeapi.Move {
	t.Helper()
	move := f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePreparation(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	if move.Status.Phase != "StartingDisruption" || f.state(t, 0).Phase != volumeapi.PhaseReady {
		t.Fatalf("did not persist disruption intent before effects: %+v", move.Status)
	}
	return move
}

func TestPreparedDisruptionProbePendingPreservesSourceAndReservation(t *testing.T) {
	for _, failure := range []string{"unsupported", "timeout", "busy", "missing probe"} {
		t.Run(failure, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			calls := 0
			f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error) {
				calls++
				return poolcapacity.Filesystem{}, errors.Join(ErrCapacityProbePending, context.DeadlineExceeded)
			})
			move := preparationStartingDisruption(t, f)
			if calls != 0 {
				t.Fatal("finishPreparation probed before its durable disruption intent")
			}
			state := f.state(t, 0)
			placement, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			unlock := func() {}
			switch failure {
			case "unsupported":
				setPreparationProbeSupported(t, f, "destination-pool", false)
			case "busy":
				unlock = f.reconciler.PoolLocks.Lock(move.Status.Preparation.Pool.UID)
			case "missing probe":
				f.reconciler.CapacityProbe = nil
			}
			err = f.reconciler.reconcilePreparation(f.ctx, move)
			unlock()
			if err == nil || ((failure == "timeout" || failure == "unsupported") && !errors.Is(err, ErrCapacityProbePending)) ||
				(failure == "busy" && !errors.Is(err, errPoolBusy)) {
				t.Fatalf("unavailable preflight not retained for retry: %v", err)
			}
			if got := f.move(t, 0); !reflect.DeepEqual(got, move) {
				t.Fatalf("pending preflight changed phase, hold or diagnostics: before=%+v after=%+v", move, got)
			}
			if got := f.state(t, 0); !reflect.DeepEqual(got, state) {
				t.Fatalf("pending preflight changed source authority: %+v", got)
			}
			current, err := f.client.CoreV1().Pods("system").Get(f.ctx, placement.Name, metav1.GetOptions{})
			if err != nil || current.UID != placement.UID || current.DeletionTimestamp != nil {
				t.Fatalf("pending preflight lost its scheduler reservation: %+v, %v", current, err)
			}
			assertPreparationOriginals(t, f)
			assertPreparationReserved(t, f, preparationRequestedBytes)
			if failure != "timeout" && calls != 0 {
				t.Fatal("probe ran without its capability, configuration or lock")
			}
			setPreparationProbeSupported(t, f, "destination-pool", true)
			f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(_ context.Context, pool volumeapi.Pool) (poolcapacity.Filesystem, error) {
				if release, acquired := f.reconciler.PoolLocks.TryLock(pool.UID); acquired {
					release()
					t.Fatal("preflight did not retain the shared Pool lock")
				}
				return poolcapacity.Filesystem{AvailableBytes: 1}, nil // Reachability, not approval.
			})
			if err = f.reconciler.reconcilePreparation(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			recovered := f.move(t, 0)
			if recovered.UID != move.UID || recovered.Status.Phase != "WaitingForUnpublish" ||
				recovered.Status.CapacityApproved || recovered.Status.Preparation == nil || f.state(t, 0).Phase != volumeapi.PhaseMoving {
				t.Fatalf("same Move did not resume from its retained hold: %+v", recovered.Status)
			}
		})
	}
}

func TestPreparedDisruptionMovingReentryDoesNotRepeatProbe(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := preparationStartingDisruption(t, f)
	state := f.state(t, 0)
	next := state
	next.Phase = volumeapi.PhaseMoving
	if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, state, next); err != nil {
		t.Fatal(err)
	}
	setPreparationProbeSupported(t, f, "destination-pool", false)
	f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error) {
		t.Fatal("crash recovery repeated the pre-disruption probe")
		return poolcapacity.Filesystem{}, ErrCapacityProbePending
	})
	if err := f.reconciler.reconcilePreparation(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	if got := f.move(t, 0); got.Status.Phase != "WaitingForUnpublish" || got.Status.CapacityApproved {
		t.Fatalf("already disrupted source did not resume: %+v", got.Status)
	}
}

func TestPreparedPreflightDoesNotAuthorizeLaterInsufficientSpace(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := preparationStartingDisruption(t, f)
	calls := 0
	f.reconciler.CapacityProbe = preparationCapacityProbeFunc(func(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error) {
		calls++
		if calls == 1 {
			return poolcapacity.Filesystem{AvailableBytes: 20 << 30}, nil
		}
		return poolcapacity.Filesystem{AvailableBytes: 1}, nil
	})
	if err := f.reconciler.reconcilePreparation(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	state := f.state(t, 0)
	if err := f.registry.ReconcilePublished(f.ctx, move.Spec.VolumeID, "source", *state.CurrentCopy, false); err != nil {
		t.Fatal(err)
	}
	status := move.Status
	status.Phase = "WaitingForCapacity"
	var err error
	move, err = f.registry.CompareAndSetMoveStatus(f.ctx, move, status)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	got := f.move(t, 0)
	if calls != 2 || got.Status.CapacityApproved || got.Status.IncomingCopy != nil ||
		got.Status.Preparation == nil || got.Status.CapacityReason != "DestinationFilesystemSpace" {
		t.Fatalf("preflight bytes became stale execution approval: calls=%d status=%+v", calls, got.Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}
