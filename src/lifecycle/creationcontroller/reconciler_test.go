package creationcontroller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/provisioning/reclaim"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const testID = "shiftpv-0123456789abcdef0123456789abcdef"

type inspectFunc func(context.Context, volume.CopyIdentity) (bool, error)

func (f inspectFunc) CreationExists(ctx context.Context, copy volume.CopyIdentity) (bool, error) {
	return f(ctx, copy)
}

func fixture(t *testing.T) (*Reconciler, *dynamicfake.FakeDynamicClient, *volumeapi.Registry, *int) {
	t.Helper()
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", NodeName: "worker", VolumeID: testID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", Role: volume.RoleServing}
	identity, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": testID, "uid": copy.VolumeUID, "resourceVersion": "1", "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": testID, "requestName": "pvc-original", "initialNode": "worker", "capacityBytes": int64(64), "initialPool": map[string]any{"name": "pool", "uid": "pool-uid"}, "creationClaim": map[string]any{"namespace": "ns", "name": "data", "uid": "original"}},
		"status":   map[string]any{"phase": volumeapi.PhasePending, "ownerNode": "worker", "currentCopy": identity, "creationOperationID": "create-volume-uid"},
	}}
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": "pool", "uid": "pool-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":   map[string]any{"nodeName": "worker", "scanEpoch": int64(0)},
		"status": map[string]any{"observedGeneration": int64(1), "inventory": map[string]any{"valid": true, "truncated": false, "copies": []any{}}},
	}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.PoolResource: "ShiftPVPoolList"}, object, pool)
	// Preserve the API's finalizer lifetime; fake Delete otherwise ignores it.
	api.PrependReactor("delete", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
		current, err := api.Tracker().Get(volumeapi.VolumeResource, "", a.(ktesting.DeleteAction).GetName())
		if err != nil {
			return true, nil, err
		}
		object := current.(*unstructured.Unstructured).DeepCopy()
		now := metav1.Now()
		object.SetDeletionTimestamp(&now)
		return true, nil, api.Tracker().Update(volumeapi.VolumeResource, object, "")
	})
	api.PrependReactor("update", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if a.GetSubresource() == "" && object.GetDeletionTimestamp() != nil && len(object.GetFinalizers()) == 0 {
			return true, object, api.Tracker().Delete(volumeapi.VolumeResource, "", object.GetName())
		}
		return false, nil, nil
	})
	api.PrependReactor("update", "shiftpvpools", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if a.GetSubresource() == "" {
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	registry := &volumeapi.Registry{Client: api}
	store := &cleanupapi.Store{Client: api}
	calls := 0
	settler := &reclaim.Settler{Store: store, Reclaim: func(ctx context.Context, cleanup cleanupapi.Cleanup, journal cleanupapi.Journal) (cleanupapi.Cleanup, error) {
		calls++
		executor := &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: "system", PodName: "node", PodUID: "node-uid", NodeName: "worker"}
		if err := journal.UpdateStatus(ctx, cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
			return cleanupapi.Cleanup{}, err
		}
		running, err := journal.Get(ctx, cleanup.Spec.Authority)
		if err != nil {
			return cleanupapi.Cleanup{}, err
		}
		receipt := &cleanupapi.Receipt{OperationID: cleanup.Spec.OperationID, ExecutorUID: executor.PodUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
		if err := journal.UpdateStatus(ctx, running, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
			return cleanupapi.Cleanup{}, err
		}
		return journal.Get(ctx, cleanup.Spec.Authority)
	}}
	cancel := &creation.Canceller{Volumes: registry, Inspector: inspectFunc(func(context.Context, volume.CopyIdentity) (bool, error) { return false, nil }), Cleanups: settler}
	r := &Reconciler{Volumes: registry, Claims: creation.RetirementInspector{Reader: creation.KubernetesRetirementReader{Client: fake.NewClientset()}}, Cancel: cancel, Interval: time.Second, WorkTimeout: time.Second, Workers: 2}
	return r, api, registry, &calls
}

func TestRetirementSettlesWithoutAnotherCreateOrDeleteRequest(t *testing.T) {
	r, api, registry, effects := fixture(t)
	ctx := context.Background()
	before, _ := registry.Get(ctx, testID)
	if complete, err := r.Reconcile(ctx, testID, before.UID); complete || err != nil {
		t.Fatal(complete, err)
	}
	after, err := registry.Get(ctx, testID)
	if err != nil || !after.CancellingCreation() || after.CreationReceipt != nil || after.CurrentCopy == nil || *after.CurrentCopy != *before.CurrentCopy || after.CapacityBytes != before.CapacityBytes {
		t.Fatal(after, err)
	}
	store := &cleanupapi.Store{Client: api}
	journal, err := store.Get(ctx, cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: before.UID})
	if err != nil || journal.Status.Phase != cleanupapi.PhaseConfirmingAbsence || journal.Status.Receipt == nil {
		t.Fatal(journal, err)
	}
	if complete, err := r.Reconcile(ctx, testID, before.UID); complete || err != nil {
		t.Fatal("stale scan released hold", complete, err)
	}
	states, _ := registry.ListVolumes(ctx)
	if reserved, err := poolcapacity.ReservedBytesForPool(states, nil, "pool-uid"); reserved != 64 || err != nil {
		t.Fatal("hold released before proof", reserved, err)
	}
	pool, _ := api.Resource(volumeapi.PoolResource).Get(ctx, "pool", metav1.GetOptions{})
	_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
	if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// A restart reconstructs its decision from the durable claim and fence.
	restarted := *r
	if complete, err := restarted.Reconcile(ctx, testID, before.UID); !complete || err != nil {
		t.Fatal(complete, err)
	}
	if _, err := registry.Get(ctx, testID); !apierrors.IsNotFound(err) {
		t.Fatal("reservation remained", err)
	}
	if *effects != 1 {
		t.Fatalf("effects repeated: %d", *effects)
	}
	states, _ = registry.ListVolumes(ctx)
	if reserved, err := poolcapacity.ReservedBytesForPool(states, nil, "pool-uid"); reserved != 0 || err != nil {
		t.Fatal("settled hold leaked", reserved, err)
	}
}

func TestUnstartedRetirementKeepsForeignProtectionAndAccounting(t *testing.T) {
	r, api, registry, effects := fixture(t)
	ctx := context.Background()
	object, _ := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	delete(object.Object, "status")
	object.SetFinalizers([]string{volumeapi.VolumeProtectionFinalizer, "foreign.example/protection"})
	if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	if complete, err := r.Reconcile(ctx, testID, "volume-uid"); complete || !errors.Is(err, volumeapi.ErrVolumeFinalizersPending) {
		t.Fatal(complete, err)
	}
	states, _ := registry.ListVolumes(ctx)
	if reserved, err := poolcapacity.ReservedBytesForPool(states, nil, "pool-uid"); reserved != 64 || err != nil {
		t.Fatal("foreign protection released capacity", reserved, err)
	}
	object, _ = api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if object.GetDeletionTimestamp() == nil {
		t.Fatal("initialization was not fenced")
	}
	object.SetFinalizers([]string{volumeapi.VolumeProtectionFinalizer})
	if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	if complete, err := r.Reconcile(ctx, testID, "volume-uid"); !complete || err != nil {
		t.Fatal(complete, err)
	}
	if _, err := registry.Get(ctx, testID); !apierrors.IsNotFound(err) || *effects != 0 {
		t.Fatal(err, *effects)
	}
}

func TestRetirementPreservesUnknownReadyAndChangedAuthority(t *testing.T) {
	for _, change := range []string{"unknown claim", "Ready Retain", "UID replacement", "published", "move", "legacy helper", "unknown helper"} {
		t.Run(change, func(t *testing.T) {
			r, api, registry, effects := fixture(t)
			ctx := context.Background()
			object, _ := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			switch change {
			case "unknown claim":
				unstructured.RemoveNestedField(object.Object, "spec", "creationClaim")
			case "Ready Retain":
				_ = unstructured.SetNestedField(object.Object, volumeapi.PhaseReady, "status", "phase")
			case "UID replacement":
				object.SetUID("replacement")
			case "published":
				_ = unstructured.SetNestedStringSlice(object.Object, []string{"worker"}, "status", "publishedNodes")
			case "move":
				_ = unstructured.SetNestedField(object.Object, "move", "status", "activeMove")
			case "legacy helper":
				r.Cancel.(*creation.Canceller).Inspector = inspectFunc(func(context.Context, volume.CopyIdentity) (bool, error) { return true, nil })
			case "unknown helper":
				r.Cancel.(*creation.Canceller).Inspector = inspectFunc(func(context.Context, volume.CopyIdentity) (bool, error) { return false, errors.New("API uncertain") })
			}
			if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
				t.Fatal(err)
			}
			before, _ := registry.Get(ctx, testID)
			_, err := r.Reconcile(ctx, testID, "volume-uid")
			if (err != nil) != (change == "legacy helper" || change == "unknown helper") {
				t.Fatal(err)
			}
			after, _ := registry.Get(ctx, testID)
			if !reflect.DeepEqual(before, after) || *effects != 0 {
				t.Fatalf("preservation failed: before=%+v after=%+v effects=%d", before, after, *effects)
			}
		})
	}
}

func TestRetirementLosesToCompletionAndReplaysAcceptedCancellation(t *testing.T) {
	for _, boundary := range []string{"completion before cancel", "completion CAS winner", "cancel response lost"} {
		t.Run(boundary, func(t *testing.T) {
			r, api, registry, effects := fixture(t)
			makeReady := func() {
				stored, _ := api.Tracker().Get(volumeapi.VolumeResource, "", testID)
				object := stored.(*unstructured.Unstructured).DeepCopy()
				_ = unstructured.SetNestedField(object.Object, volumeapi.PhaseReady, "status", "phase")
				if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "completion before cancel" {
				r.Cancel.(*creation.Canceller).Inspector = inspectFunc(func(context.Context, volume.CopyIdentity) (bool, error) { makeReady(); return false, nil })
			} else {
				first := true
				api.PrependReactor("update", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
					if !first || a.GetSubresource() != "status" {
						return false, nil, nil
					}
					first = false
					if boundary == "completion CAS winner" {
						makeReady()
						return true, nil, apierrors.NewConflict(volumeapi.VolumeResource.GroupResource(), testID, errors.New("completion won"))
					}
					object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
					if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
						t.Fatal(err)
					}
					return true, nil, apierrors.NewTimeoutError("accepted fence response lost", 1)
				})
			}
			if _, err := r.Reconcile(context.Background(), testID, "volume-uid"); err == nil {
				t.Fatal("expected uncertain/rejected first result")
			}
			state, _ := registry.Get(context.Background(), testID)
			if boundary != "cancel response lost" {
				if state.Phase != volumeapi.PhaseReady || *effects != 0 {
					t.Fatal(state, *effects)
				}
				if done, err := r.Reconcile(context.Background(), testID, "volume-uid"); !done || err != nil {
					t.Fatal(done, err)
				}
			} else {
				if !state.CancellingCreation() || *effects != 0 {
					t.Fatal(state, *effects)
				}
				if done, err := r.Reconcile(context.Background(), testID, "volume-uid"); done || err != nil || *effects != 1 {
					t.Fatal(done, err, *effects)
				}
			}
		})
	}
}

type queueRepository struct{ states map[string]volumeapi.State }

func (r queueRepository) Get(_ context.Context, id string) (volumeapi.State, error) {
	return r.states[id], nil
}
func (r queueRepository) ListCreationIntents(context.Context) ([]volumeapi.CreationReference, error) {
	var intents []volumeapi.CreationReference
	for id, state := range r.states {
		intents = append(intents, volumeapi.CreationReference{Name: id, UID: state.UID})
	}
	return intents, nil
}

func TestDiscoveryDamagedVolumeDoesNotHideValidRetirement(t *testing.T) {
	r, api, _, _ := fixture(t)
	object, _ := api.Resource(volumeapi.VolumeResource).Get(context.Background(), testID, metav1.GetOptions{})
	bad := object.DeepCopy()
	bad.SetName("shiftpv-1123456789abcdef0123456789abcdef")
	bad.SetUID("bad-uid")
	_ = unstructured.SetNestedField(bad.Object, "invalid/copy", "status", "currentCopy", "copyID")
	if err := api.Tracker().Create(volumeapi.VolumeResource, bad, ""); err != nil {
		t.Fatal(err)
	}
	intents, err := r.Volumes.ListCreationIntents(context.Background())
	if err != nil || len(intents) != 2 {
		t.Fatal("damaged volume poisoned discovery", intents, err)
	}
	if done, err := r.Reconcile(context.Background(), bad.GetName(), string(bad.GetUID())); done || err == nil {
		t.Fatal("damaged copy was adopted", done, err)
	}
	if done, err := r.Reconcile(context.Background(), testID, "volume-uid"); done || err != nil {
		t.Fatal("valid work did not reach settlement", done, err)
	}
}

type retirementFunc func(context.Context, string, volumeapi.State) (bool, error)

func (f retirementFunc) Retired(ctx context.Context, id string, state volumeapi.State) (bool, error) {
	return f(ctx, id, state)
}

type cancelFunc func(context.Context, string, string) (bool, error)

func (f cancelFunc) Advance(ctx context.Context, id, uid string) (bool, error) {
	return f(ctx, id, uid)
}

func TestRunUnavailableClaimDoesNotBlockIndependentRetirement(t *testing.T) {
	_, _, registry, _ := fixture(t)
	state, _ := registry.Get(context.Background(), testID)
	other := "shiftpv-1123456789abcdef0123456789abcdef"
	progress := make(chan string, 1)
	blocked := make(chan struct{}, 1)
	r := Reconciler{Volumes: queueRepository{states: map[string]volumeapi.State{testID: state, other: state}},
		Claims: retirementFunc(func(ctx context.Context, id string, _ volumeapi.State) (bool, error) {
			if id == testID {
				select {
				case blocked <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return false, ctx.Err()
			}
			return true, nil
		}),
		Cancel: cancelFunc(func(_ context.Context, id, uid string) (bool, error) {
			if id == other && uid == state.UID {
				select {
				case progress <- id:
				default:
				}
			}
			return true, nil
		}),
		Interval: time.Hour, WorkTimeout: time.Second, Workers: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked fixture did not start")
	}
	select {
	case <-progress:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("independent claim was held by blocked observation")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not join cancellation")
	}
}
