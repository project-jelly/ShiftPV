package volumeapi

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

const unstartedCancellationID = "shiftpv-0123456789abcdef0123456789abcdef"

// Model the real DELETE boundary: UID/RV CAS stores deletionTimestamp while
// protection exists, and only the last finalizer removal deletes the object.
func cancellationFixture(t *testing.T, loss string) (*Registry, *dynamicfake.FakeDynamicClient, *unstructured.Unstructured) {
	t.Helper()
	object := newVolumeObject(unstartedCancellationID, "pvc-request", "worker-a", 64, Pool{Name: "pool-a", UID: "pool-uid"})
	object.SetUID("volume-uid")
	object.SetResourceVersion("1")
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	lost := false
	api.PrependReactor("delete", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		action := a.(k8stesting.DeleteAction)
		tracked, err := api.Tracker().Get(VolumeResource, "", action.GetName())
		if err != nil {
			return true, nil, err
		}
		current := tracked.(*unstructured.Unstructured).DeepCopy()
		pre := action.GetDeleteOptions().Preconditions
		if pre == nil || pre.UID == nil || *pre.UID != current.GetUID() || pre.ResourceVersion == nil || *pre.ResourceVersion != current.GetResourceVersion() {
			return true, nil, apierrors.NewConflict(VolumeResource.GroupResource(), action.GetName(), errors.New("UID/RV changed"))
		}
		now := metav1.Now()
		current.SetDeletionTimestamp(&now)
		current.SetResourceVersion("2")
		if err := api.Tracker().Update(VolumeResource, current, ""); err != nil {
			t.Fatal(err)
		}
		if loss == "delete response" && !lost {
			lost = true
			return true, nil, apierrors.NewTimeoutError("accepted DELETE response lost", 1)
		}
		return true, nil, nil
	})
	api.PrependReactor("update", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "" {
			t.Fatal("unstarted cancellation wrote execution status")
		}
		update := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		tracked, err := api.Tracker().Get(VolumeResource, "", update.GetName())
		if err != nil {
			return true, nil, err
		}
		current := tracked.(*unstructured.Unstructured)
		if update.GetResourceVersion() != current.GetResourceVersion() || update.GetUID() != current.GetUID() {
			return true, nil, apierrors.NewConflict(VolumeResource.GroupResource(), update.GetName(), errors.New("finalizer RV changed"))
		}
		if update.GetDeletionTimestamp() == nil || len(update.GetFinalizers()) != 0 {
			t.Fatalf("unexpected cancellation mutation: %+v", update)
		}
		if err := api.Tracker().Delete(VolumeResource, "", update.GetName()); err != nil {
			t.Fatal(err)
		}
		if loss == "finalizer response" && !lost {
			lost = true
			return true, nil, apierrors.NewTimeoutError("accepted finalizer response lost", 1)
		}
		return true, update, nil
	})
	return &Registry{Client: api}, api, object
}

func TestUnstartedCancellationReplaysAcceptedResponses(t *testing.T) {
	for _, loss := range []string{"none", "delete response", "finalizer response"} {
		t.Run(loss, func(t *testing.T) {
			r, _, _ := cancellationFixture(t, loss)
			ctx := context.Background()
			err := r.CancelUnstartedCreation(ctx, unstartedCancellationID, "volume-uid")
			if loss == "none" && err != nil || loss != "none" && !apierrors.IsTimeout(err) {
				t.Fatalf("initial cancellation=%v", err)
			}
			if loss == "delete response" {
				held, err := r.Get(ctx, unstartedCancellationID)
				if err != nil || !held.Deleting || !held.HasInitialReservation() || held.CurrentCopy != nil || held.CreationReceipt != nil {
					t.Fatalf("accepted DELETE dropped reservation or invented execution: %+v err=%v", held, err)
				}
			}
			// A fresh controller needs no in-memory progress from the first call.
			r = &Registry{Client: r.Client}
			if err := r.CancelUnstartedCreation(ctx, unstartedCancellationID, "volume-uid"); err != nil {
				t.Fatalf("replay=%v", err)
			}
			if _, err := r.Get(ctx, unstartedCancellationID); !apierrors.IsNotFound(err) {
				t.Fatalf("unstarted object remained: %v", err)
			}
		})
	}
}

func TestUnstartedCancellationCannotOvertakeInitialization(t *testing.T) {
	r, api, object := cancellationFixture(t, "none")
	api.PrependReactor("delete", "shiftpvvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: unstartedCancellationID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "worker-a", Role: volume.RoleServing}
		winner := object.DeepCopy()
		setState(winner, State{UID: "volume-uid", Phase: PhasePending, OwnerNode: "worker-a", CurrentCopy: &copy, CreationOperationID: "create-volume-uid"})
		winner.SetResourceVersion("2")
		if err := api.Tracker().Update(VolumeResource, winner, ""); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); !apierrors.IsConflict(err) {
		t.Fatalf("stale no-effects observation authorized DELETE: %v", err)
	}
	current, err := r.Get(context.Background(), unstartedCancellationID)
	if err != nil || current.Phase != PhasePending || current.Deleting || current.CurrentCopy == nil {
		t.Fatalf("initialization winner changed: %+v err=%v", current, err)
	}
}

func TestUnstartedCancellationTimestampFencesLateInitialization(t *testing.T) {
	r, _, _ := cancellationFixture(t, "delete response")
	if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); !apierrors.IsTimeout(err) {
		t.Fatal(err)
	}
	if _, err := r.BeginCreateInPool(context.Background(), unstartedCancellationID, "pvc-request", "worker-a", 64, "pool-a", "pool-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("late initialization crossed deletion fence: %v", err)
	}
	if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); err != nil {
		t.Fatal(err)
	}
}

func TestUnstartedCancellationCanCloseLegacyIntentWithoutReservation(t *testing.T) {
	r, api, object := cancellationFixture(t, "none")
	unstructured.RemoveNestedField(object.Object, "spec", "initialPool")
	if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelUnstartedCreation(context.Background(), object.GetName(), string(object.GetUID())); err != nil {
		t.Fatal(err)
	}
}

func TestUnstartedCancellationPreservesUnknownAndForeignState(t *testing.T) {
	for _, change := range []string{"UID", "phase", "owner", "executor", "receipt", "cleanup", "publication", "protection", "missing RV"} {
		t.Run(change, func(t *testing.T) {
			r, api, object := cancellationFixture(t, "none")
			switch change {
			case "UID":
				object.SetUID("replacement-uid")
			case "phase":
				_ = unstructured.SetNestedField(object.Object, PhasePending, "status", "phase")
			case "owner":
				_ = unstructured.SetNestedField(object.Object, "worker-a", "status", "ownerNode")
			case "executor":
				_ = unstructured.SetNestedMap(object.Object, map[string]any{"podUID": "late-executor"}, "status", "creationExecutor")
			case "receipt":
				_ = unstructured.SetNestedMap(object.Object, map[string]any{"operationID": "unknown"}, "status", "creationReceipt")
			case "cleanup":
				_ = unstructured.SetNestedMap(object.Object, map[string]any{}, "status", "cleanup")
			case "publication":
				_ = unstructured.SetNestedStringSlice(object.Object, []string{"worker-a"}, "status", "publishedNodes")
			case "protection":
				object.SetFinalizers(nil)
			case "missing RV":
				object.SetResourceVersion("")
			}
			if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
				t.Fatal(err)
			}
			api.ClearActions()
			if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); err == nil {
				t.Fatal("ambiguous execution was cancelled")
			}
			for _, action := range api.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("unknown state mutated: %v", action)
				}
			}
		})
	}
}

func TestUnstartedCancellationReadErrorsCannotReleaseProtection(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			r, api, _ := cancellationFixture(t, "none")
			reads := 0
			api.PrependReactor("get", "shiftpvvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == failAt {
					return true, nil, apierrors.NewTimeoutError("unknown live state", 1)
				}
				return false, nil, nil
			})
			if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); !apierrors.IsTimeout(err) {
				t.Fatal(err)
			}
			state, err := r.Get(context.Background(), unstartedCancellationID)
			if err != nil || !state.HasInitialReservation() || state.CurrentCopy != nil {
				t.Fatalf("unknown read released reservation: %+v err=%v", state, err)
			}
			if err := r.CancelUnstartedCreation(context.Background(), unstartedCancellationID, "volume-uid"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnstartedCancellationRetainsProtectionForForeignFinalizer(t *testing.T) {
	r, api, object := cancellationFixture(t, "none")
	object.SetFinalizers(append(object.GetFinalizers(), "example.com/protection"))
	if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelUnstartedCreation(context.Background(), object.GetName(), "volume-uid"); !errors.Is(err, ErrVolumeFinalizersPending) {
		t.Fatal(err)
	}
	state, err := r.Get(context.Background(), object.GetName())
	if err != nil || !state.Deleting || !state.HasInitialReservation() || len(state.Finalizers) != 2 {
		t.Fatalf("foreign protection lost reservation: %+v err=%v", state, err)
	}
	tracked, _ := api.Tracker().Get(VolumeResource, "", object.GetName())
	current := tracked.(*unstructured.Unstructured).DeepCopy()
	current.SetFinalizers([]string{VolumeProtectionFinalizer})
	current.SetResourceVersion("3")
	if err := api.Tracker().Update(VolumeResource, current, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelUnstartedCreation(context.Background(), object.GetName(), "volume-uid"); err != nil {
		t.Fatal(err)
	}
}

func TestUnstartedCancellationRechecksAfterFinalizationConflict(t *testing.T) {
	r, api, object := cancellationFixture(t, "none")
	changed := false
	api.PrependReactor("update", "shiftpvvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !changed {
			changed = true
			tracked, err := api.Tracker().Get(VolumeResource, "", object.GetName())
			if err != nil {
				t.Fatal(err)
			}
			current := tracked.(*unstructured.Unstructured).DeepCopy()
			current.SetAnnotations(map[string]string{"example.test/concurrent-writer": "won"})
			current.SetResourceVersion("3")
			if err := api.Tracker().Update(VolumeResource, current, ""); err != nil {
				t.Fatal(err)
			}
		}
		return false, nil, nil
	})
	if err := r.CancelUnstartedCreation(context.Background(), object.GetName(), "volume-uid"); err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("finalization CAS conflict was not exercised")
	}
	if _, err := r.Get(context.Background(), object.GetName()); !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}
