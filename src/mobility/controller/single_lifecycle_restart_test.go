package controller

import (
	"errors"
	"io"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestSingleLifecycleRestartAfterAcceptedBoundaryWrite(t *testing.T) {
	for _, boundary := range []string{"plan", "claim", "hold", "StartingDisruption", "Cancelled"} {
		t.Run(boundary, func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			original := f.state(t, 0)
			if boundary == "Cancelled" {
				singleLifecycleUntil(t, f, func(move volumeapi.Move, _ volumeapi.State) bool { return move.Status.Preparation != nil })
				node, err := f.client.CoreV1().Nodes().Get(f.ctx, "source", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				node.Spec.Unschedulable = false
				if _, err = f.client.CoreV1().Nodes().Update(f.ctx, node, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			lost := false
			f.afterWrite = func(resource schema.GroupVersionResource, object *unstructured.Unstructured) error {
				if !lost && singleLifecycleBoundaryWritten(boundary, resource, object, f.moveNames[0]) {
					lost = true
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			for range 20 {
				err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0))
				if lost {
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("accepted %s write lost its failure cause: %v", boundary, err)
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				f.schedulePlacements(t)
			}
			if !lost {
				t.Fatalf("public lifecycle never persisted %s boundary", boundary)
			}
			assertNoEviction(t, f.client)
			singleLifecycleNoDiskWork(t, f)
			checkpoint, source := f.move(t, 0), f.state(t, 0)
			if source.Phase != volumeapi.PhaseReady || source.OwnerNode != original.OwnerNode ||
				!reflect.DeepEqual(source.CurrentCopy, original.CurrentCopy) || !reflect.DeepEqual(source.PublishedNodes, original.PublishedNodes) ||
				checkpoint.Status.CapacityApproved || checkpoint.Status.IncomingCopy != nil {
				t.Fatalf("accepted %s write crossed its authority boundary: source=%+v move=%+v", boundary, source, checkpoint.Status)
			}
			if boundary == "plan" && source.ActiveMove != "" || boundary != "plan" && source.ActiveMove != checkpoint.Name {
				t.Fatalf("%s write did not retain its exact source claim: %+v", boundary, source)
			}
			if (boundary == "plan" || boundary == "claim") && checkpoint.Status.Preparation != nil {
				t.Fatalf("ambiguous %s write granted a later reservation", boundary)
			}
			if boundary == "hold" && checkpoint.Status.Phase != "Preparing" || boundary == "StartingDisruption" && checkpoint.Status.Phase != "StartingDisruption" {
				t.Fatalf("ambiguous %s write advanced to another intent: %+v", boundary, checkpoint.Status)
			}
			if boundary == "Cancelled" && (checkpoint.Status.Phase != "Cancelled" || checkpoint.Status.Preparation != nil) {
				t.Fatalf("cancelled hold release was not distinct from source unlock: %+v", checkpoint.Status)
			}

			// A restart keeps API state but replaces all process-local reconciler
			// state, including the Pool locker. No test copies a modified Move
			// snapshot into the new controller.
			f.afterWrite = nil
			prior := f.reconciler
			f.reconciler = newTestReconciler(f.client, f.registry, func(r *Reconciler) {
				r.CapacityProbe, r.SourceUsage, r.Cleanups = prior.CapacityProbe, prior.SourceUsage, prior.Cleanups
			})
			if boundary == "Cancelled" {
				// Cancellation already owns its exact source and no disk effects.
				// Workload, binding and source Node availability are not required
				// to finish releasing that old responsibility after restart.
				singleLifecycleRemoveWorkload(t, f)
				singleLifecycleUntil(t, f, func(move volumeapi.Move, state volumeapi.State) bool {
					return move.Status.Phase == "Cancelled" && state.ActiveMove == ""
				})
				settled := f.state(t, 0)
				if settled.UID != original.UID || settled.Phase != volumeapi.PhaseReady || settled.OwnerNode != original.OwnerNode ||
					!reflect.DeepEqual(settled.CurrentCopy, original.CurrentCopy) || !reflect.DeepEqual(settled.PublishedNodes, original.PublishedNodes) {
					t.Fatalf("restarted cancellation changed source data identity: %+v", settled)
				}
				assertPreparationReserved(t, f, 0)
				assertNoEviction(t, f.client)
			} else {
				singleLifecycleUntil(t, f, func(move volumeapi.Move, _ volumeapi.State) bool { return move.Status.Phase == "WaitingForUnpublish" })
				resumed, source := f.move(t, 0), f.state(t, 0)
				if resumed.UID != checkpoint.UID || source.ActiveMove != resumed.Name || source.Phase != volumeapi.PhaseMoving ||
					source.OwnerNode != original.OwnerNode || !reflect.DeepEqual(source.CurrentCopy, original.CurrentCopy) ||
					!reflect.DeepEqual(source.PublishedNodes, original.PublishedNodes) || resumed.Status.Preparation == nil || resumed.Status.CapacityApproved {
					t.Fatalf("restart lost the exact prepared transaction: source=%+v move=%+v", source, resumed.Status)
				}
				assertPreparationReserved(t, f, preparationRequestedBytes)
			}
			singleLifecycleNoDiskWork(t, f)
			moves, err := f.registry.ListMoves(f.ctx)
			if err != nil || len(moves) != 1 || moves[0].UID != checkpoint.UID {
				t.Fatalf("restart replaced or duplicated its transaction: moves=%+v error=%v", moves, err)
			}
		})
	}
}

func singleLifecycleBoundaryWritten(boundary string, resource schema.GroupVersionResource, object *unstructured.Unstructured, moveName string) bool {
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	if resource == volumeapi.VolumeResource {
		active, _, _ := unstructured.NestedString(object.Object, "status", "activeMove")
		return boundary == "claim" && phase == volumeapi.PhaseReady && active == moveName
	}
	if resource != volumeapi.MoveResource {
		return false
	}
	switch boundary {
	case "plan":
		_, present, _ := unstructured.NestedMap(object.Object, "status", "sourceCopy")
		return phase == "Preparing" && present
	case "hold":
		_, present, _ := unstructured.NestedMap(object.Object, "status", "preparation")
		return present
	default:
		return phase == boundary
	}
}

func singleLifecycleUntil(t *testing.T, f *preparationRegistryFixture, ready func(volumeapi.Move, volumeapi.State) bool) {
	t.Helper()
	for range 20 {
		move, state := f.move(t, 0), f.state(t, 0)
		if ready(move, state) {
			return
		}
		if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
			t.Fatal(err)
		}
		f.schedulePlacements(t)
	}
	t.Fatalf("lifecycle did not converge: move=%+v source=%+v", f.move(t, 0), f.state(t, 0))
}

func singleLifecycleRemoveWorkload(t *testing.T, f *preparationRegistryFixture) {
	t.Helper()
	w := f.workloads[0]
	for _, target := range []struct {
		resource, namespace, name string
	}{
		{"pods", w.Consumer.Namespace, w.Consumer.Name},
		{"persistentvolumeclaims", w.Claim.Namespace, w.Claim.Name},
		{"persistentvolumes", "", w.PV.Name},
		{"nodes", "", "source"},
	} {
		if err := f.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource(target.resource), target.namespace, target.name); err != nil {
			t.Fatal(err)
		}
	}
}
