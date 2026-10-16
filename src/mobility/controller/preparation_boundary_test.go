package controller

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestPreparationFiveGiAdmitsTwoOfFourConsumersWithoutEviction(t *testing.T) {
	f := newPreparationRegistryFixture(t, 4)
	for cycle := range 12 {
		for index := range f.moveNames {
			move := f.move(t, index)
			// Freeze at the durable hold, as the Kind admission policy does.
			if move.Status.Preparation == nil {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
					t.Fatalf("cycle %d consumer %d: %v", cycle, index, err)
				}
			}
		}
		f.schedulePlacements(t)
		assertPreparationOriginals(t, f)
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, move := range moves {
		if move.Status.Preparation != nil {
			count++
		}
		if move.Status.CapacityApproved || move.Status.IncomingCopy != nil || move.Status.DestinationCopy != nil || move.Status.SourceBytes != 0 {
			t.Fatalf("preparation acquired disk authority: %+v", move.Status)
		}
	}
	if count != 2 {
		t.Fatalf("prepared=%d, want exactly two", count)
	}
	assertPreparationReserved(t, f, 4<<30)
	// A fresh reconciler must recover both holds and keep the other originals
	// serving. One pass can persist StartingDisruption but cannot evict yet.
	f.reconciler = newTestReconciler(f.client, f.registry)
	for index := range f.moveNames {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, index)); err != nil {
			t.Fatal(err)
		}
	}
	assertPreparationOriginals(t, f)
	assertPreparationReserved(t, f, 4<<30)
}

func TestPreparationWriteResponseLossStopsBeforeFurtherEffects(t *testing.T) {
	for _, boundary := range []string{"source claim", "preparation", "StartingDisruption"} {
		t.Run(boundary, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			lost := errors.New("stored write response lost")
			fired := false
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
				active, _, _ := unstructured.NestedString(object.Object, "status", "activeMove")
				_, prepared, _ := unstructured.NestedMap(object.Object, "status", "preparation")
				match := boundary == "source claim" && resource == volumeapi.VolumeResource && active == f.moveNames[0] && phase == "Ready" ||
					boundary == "preparation" && resource == volumeapi.MoveResource && prepared ||
					boundary == "StartingDisruption" && resource == volumeapi.MoveResource && phase == "StartingDisruption"
				if !fired && match {
					fired = true
					return lost
				}
				return nil
			}
			for range 20 {
				err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0))
				if fired {
					if !errors.Is(err, lost) {
						t.Fatalf("lost result not returned: %v", err)
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				f.schedulePlacements(t)
			}
			if !fired {
				t.Fatal("boundary was never exercised")
			}
			assertPreparationOriginals(t, f)
			state := f.state(t, 0)
			if state.Phase != volumeapi.PhaseReady || state.ActiveMove != f.moveNames[0] {
				t.Fatalf("source authority changed after ambiguous write: %+v", state)
			}
			f.afterWrite = nil
			if boundary != "StartingDisruption" {
				f.untilPrepared(t, 0)
				assertPreparationOriginals(t, f)
				assertPreparationReserved(t, f, preparationRequestedBytes)
			} else {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				if f.state(t, 0).Phase != volumeapi.PhaseMoving || f.move(t, 0).Status.Phase != "WaitingForUnpublish" {
					t.Fatal("re-observed disruption did not resume")
				}
			}
		})
	}
}

func TestPreparationChangedAuthorityCannotEvictOriginal(t *testing.T) {
	for _, change := range []string{"consumer UID", "claim UID", "PV UID", "source copy", "Pool UID", "placement UID", "template selector"} {
		t.Run(change, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			move := f.untilPrepared(t, 0)
			w := f.workloads[0]
			switch change {
			case "consumer UID":
				pod, err := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.UID = "recreated-consumer"
				if _, err = f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "claim UID":
				claim, err := f.client.CoreV1().PersistentVolumeClaims(w.Namespace.Name).Get(f.ctx, w.Claim.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				claim.UID = "recreated-claim"
				if _, err = f.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(f.ctx, claim, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "PV UID":
				pv, err := f.client.CoreV1().PersistentVolumes().Get(f.ctx, w.PV.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pv.UID = "recreated-pv"
				if _, err = f.client.CoreV1().PersistentVolumes().Update(f.ctx, pv, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "source copy":
				object, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(f.ctx, f.volumeIDs[0], metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				_ = unstructured.SetNestedField(object.Object, "changed-copy", "status", "currentCopy", "copyID")
				if _, err = f.dynamic.Resource(volumeapi.VolumeResource).UpdateStatus(f.ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "Pool UID":
				object, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				object.SetUID("recreated-pool")
				if err = f.dynamic.Tracker().Update(volumeapi.PoolResource, object, ""); err != nil {
					t.Fatal(err)
				}
			case "placement UID":
				pod, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.UID = types.UID("recreated-placement")
				if _, err = f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "template selector":
				rs, err := f.client.AppsV1().ReplicaSets(w.Namespace.Name).Get(f.ctx, w.ReplicaSet.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				rs.Spec.Template.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "source"}
				if _, err = f.client.AppsV1().ReplicaSets(rs.Namespace).Update(f.ctx, rs, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			// Refusal may be a retry, cancellation or identity error; none grants
			// disruption from the stale preparation snapshot.
			_ = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0))
			assertNoEviction(t, f.client)
			if f.state(t, 0).Phase != volumeapi.PhaseReady || f.move(t, 0).Status.Phase == "StartingDisruption" {
				t.Fatalf("%s authorized disruption", change)
			}
		})
	}
}

func TestPreparationConversionRetainsOneLogicalReservation(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := f.untilPrepared(t, 0)
	state := f.state(t, 0)
	next := state
	next.Phase = volumeapi.PhaseMoving
	if err := f.registry.CompareAndSetSourceState(f.ctx, move.Spec.VolumeID, state, next); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.ReconcilePublished(f.ctx, move.Spec.VolumeID, "source", *state.CurrentCopy, false); err != nil {
		t.Fatal(err)
	}
	nextStatus := move.Status
	nextStatus.Phase = "WaitingForCapacity"
	var err error
	move, err = f.registry.CompareAndSetMoveStatus(f.ctx, move, nextStatus)
	if err != nil {
		t.Fatal(err)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
	if err = f.reconciler.ensureCapacity(f.ctx, &move, observation{DestinationNode: "destination"}); err != nil {
		t.Fatal(err)
	}
	stored := f.move(t, 0)
	if stored.Status.Preparation != nil || !stored.Status.CapacityApproved || stored.Status.DestinationCopy == nil {
		t.Fatalf("conversion not stored: %+v", stored.Status)
	}
	assertPreparationReserved(t, f, preparationRequestedBytes)
}

func TestPreparedAndLegacyMovesCoexist(t *testing.T) {
	f := newPreparationRegistryFixture(t, 2)
	object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, f.moveNames[1], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
	if _, err = f.dynamic.Resource(volumeapi.MoveResource).Update(f.ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = f.reconciler.ReconcileAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.move(t, 0).Status.SourceCopy == nil || f.move(t, 0).Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 {
		t.Fatal("prepared route did not initialize")
	}
	if legacy := f.move(t, 1); legacy.Spec.ProtocolVersion != "" || legacy.Status.Phase == "Pending" {
		t.Fatalf("legacy route did not progress: %+v", legacy)
	}
}

func assertPreparationOriginals(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	assertNoEviction(t, f.client)
	for index, w := range f.workloads {
		pod, err := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
		if err != nil || pod.UID != w.Consumer.UID || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			t.Fatalf("original %d disturbed: %+v %v", index, pod, err)
		}
		if state := f.state(t, index); state.Phase != volumeapi.PhaseReady || state.OwnerNode != "source" || state.CurrentCopy == nil {
			t.Fatalf("source %d no longer serving: %+v", index, state)
		}
	}
}

func assertPreparationReserved(t *testing.T, f *preparationRegistryFixture, want int64) {
	t.Helper()
	states, err := f.registry.ListVolumes(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	moves, err := f.registry.ListMoves(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := poolcapacity.ReservedBytesForPool(states, moves, "destination-pool-uid")
	if err != nil || reserved != want {
		t.Fatalf("reserved=%d want=%d: %v", reserved, want, err)
	}
}
