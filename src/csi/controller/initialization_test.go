package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func initializationFixture(t *testing.T) (*dynamicfake.FakeDynamicClient, *volumeapi.Registry, func() *Service, *fakePoolCapacityProbe) {
	t.Helper()
	objects := []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"},
	}}}
	for _, suffix := range []string{"a", "b"} {
		objects = append(objects, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
			"metadata": map[string]any{"name": "pool-" + suffix, "uid": "pool-" + suffix + "-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
			"spec":     map[string]any{"nodeName": "worker-" + suffix, "mountPath": "/var/lib/shiftpv", "capacity": map[string]any{"limit": "1Gi"}},
			"status": map[string]any{"registrationApproved": true, "observedGeneration": int64(1), "lastProbeTime": time.Now().UTC().Format(time.RFC3339Nano),
				"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "PoolReady", "observedGeneration": int64(1), "lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano)}},
				"inventory":  map[string]any{"valid": true, "observedAt": time.Now().UTC().Format(time.RFC3339Nano)}},
		}})
	}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, objects...)
	api.PrependReactor("create", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		object := a.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
		if object.GetUID() == "" {
			object.SetUID(types.UID("uid-" + object.GetName()))
		}
		return false, nil, nil
	})
	registry := &volumeapi.Registry{Client: api}
	probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}}
	newService := func() *Service {
		return configuredService(&Service{
			Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: &fakeDirectoryOperator{},
			Volumes: registry, CapacityPools: registry, CapacityProbe: probe, Cleanups: &cleanupapi.Store{Client: api},
		})
	}
	return api, registry, newService, probe
}

func TestCreateInitializationRecoversAPIWriteBoundaries(t *testing.T) {
	for _, failure := range []string{"status rejected", "create response lost", "status response lost"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			api, registry, newService, probe := initializationFixture(t)
			req := validCreateRequest("worker-a")
			id, _ := volume.IDFromName(req.Name)
			injected := false
			api.PrependReactor("*", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
				if injected {
					return false, nil, nil
				}
				if failure == "create response lost" && a.GetVerb() == "create" {
					object := a.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured)
					object.SetUID(types.UID("uid-" + id))
					if err := api.Tracker().Create(volumeapi.VolumeResource, object, ""); err != nil {
						t.Fatal(err)
					}
					injected = true
				} else if failure != "create response lost" && a.GetVerb() == "update" && a.GetSubresource() == "status" {
					if failure == "status response lost" {
						if err := api.Tracker().Update(volumeapi.VolumeResource, a.(k8stesting.UpdateAction).GetObject(), ""); err != nil {
							t.Fatal(err)
						}
					}
					injected = true
				} else {
					return false, nil, nil
				}
				return true, nil, apierrors.NewTimeoutError("injected API write interruption", 1)
			})
			if _, err := newService().CreateVolume(ctx, req); status.Code(err) != codes.Unavailable {
				t.Fatalf("write failure=%v", err)
			}
			before, err := registry.Get(ctx, id)
			if err != nil || before.InitialPool == nil || before.InitialPool.UID != "pool-a-uid" {
				t.Fatalf("lost initial reservation: %+v %v", before, err)
			}
			assertInitialReservation(t, registry, "pool-a-uid", req.CapacityRange.RequiredBytes)
			assertInitialReservation(t, registry, "pool-b-uid", 0)
			other := validCreateRequest("worker-b")
			other.Name = "pvc-other-pool"
			if _, err := newService().CreateVolume(ctx, other); err != nil {
				t.Fatalf("unrelated Pool blocked: %v", err)
			}
			competitor := validCreateRequest("worker-a")
			competitor.Name, competitor.CapacityRange.RequiredBytes = "pvc-competing", 1<<30
			if _, err := newService().CreateVolume(ctx, competitor); err == nil {
				t.Fatal("initial reservation allowed oversubscription")
			}
			// A new scan invalidates admission freshness; an already reserved
			// initialization still resumes without repeating the capacity probe.
			pool, _ := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
			pool.SetGeneration(2)
			if _, err := api.Resource(volumeapi.PoolResource).Update(ctx, pool, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			calls := probe.callCount()
			if _, err := newService().CreateVolume(ctx, req); err != nil {
				t.Fatalf("fresh Controller could not resume: %v", err)
			}
			after, _ := registry.Get(ctx, id)
			if after.UID != before.UID || after.Phase != volumeapi.PhaseReady || after.CurrentCopy.PoolUID != before.InitialPool.UID || probe.callCount() != calls {
				t.Fatalf("resume changed identity or repeated admission: %+v calls=%d/%d", after, calls, probe.callCount())
			}
			assertInitialReservation(t, registry, "pool-a-uid", req.CapacityRange.RequiredBytes)
		})
	}
}

func assertInitialReservation(t *testing.T, registry *volumeapi.Registry, pool string, want int64) {
	t.Helper()
	states, err := registry.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := poolcapacity.ReservedBytesForPool(states, nil, pool)
	if err != nil || got != want {
		t.Fatalf("pool=%s reserved=%d want=%d err=%v", pool, got, want, err)
	}
}

func legacyInitialization(t *testing.T, api *dynamicfake.FakeDynamicClient, name string) string {
	t.Helper()
	id, _ := volume.IDFromName(name)
	_, err := api.Resource(volumeapi.VolumeResource).Create(context.Background(), &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": id, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": id, "requestName": name, "initialNode": "worker-a", "capacityBytes": int64(64 << 20)},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestLegacyInitializationsReadmitWithoutBlockingEachOther(t *testing.T) {
	api, registry, newService, probe := initializationFixture(t)
	legacyInitialization(t, api, "pvc-first")
	legacyInitialization(t, api, "pvc-second")
	assertInitialReservation(t, registry, "pool-a-uid", 0)
	for i, name := range []string{"pvc-first", "pvc-second"} {
		req := validCreateRequest("worker-a")
		req.Name = name
		if _, err := newService().CreateVolume(context.Background(), req); err != nil {
			t.Fatalf("legacy initialization %s: %v", name, err)
		}
		assertInitialReservation(t, registry, "pool-a-uid", int64(i+1)*req.CapacityRange.RequiredBytes)
	}
	if probe.callCount() != 2 {
		t.Fatalf("legacy requests skipped fresh admission: %d", probe.callCount())
	}
}

func TestCapacityWaitRetainsLegacyInitialNode(t *testing.T) {
	for _, shortage := range []string{"quota", "available bytes"} {
		for _, lifecycle := range []string{"legacy initialization", "new request"} {
			t.Run(shortage+"/"+lifecycle, func(t *testing.T) {
				ctx := context.Background()
				api, registry, newService, probe := initializationFixture(t)
				req := validCreateRequest("worker-a")
				req.Parameters = map[string]string{PVCNamespaceKey: "test", PVCNameKey: "claim"}
				id, _ := volume.IDFromName(req.Name)
				var original volumeapi.State
				if lifecycle == "legacy initialization" {
					legacyInitialization(t, api, req.Name)
					original, _ = registry.Get(ctx, id)
				}
				if shortage == "quota" {
					competitor := validCreateRequest("worker-a")
					competitor.Name, competitor.CapacityRange.RequiredBytes = "pvc-full-pool", 1<<30
					if _, err := newService().CreateVolume(ctx, competitor); err != nil {
						t.Fatalf("prepare occupied Pool: %v", err)
					}
				} else {
					probe.stats.AvailableBytes = 0
				}
				service := newService()
				service.ConsumerPlacement = &placementStub{result: consumer.Result{Placement: consumer.Reschedulable, Reason: "scheduler-owned consumer"}}
				retries := &retryRecorder{}
				service.RetryRequests = retries
				_, err := service.CreateVolume(ctx, req)
				if lifecycle == "new request" {
					if status.Code(err) != codes.ResourceExhausted || len(retries.calls) != 0 {
						t.Fatalf("new request lost rescheduling: err=%v retries=%+v", err, retries.calls)
					}
					if _, err := registry.Get(ctx, id); !apierrors.IsNotFound(err) {
						t.Fatalf("denied new request acquired an intent: %v", err)
					}
					return
				}
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("legacy immutable initialNode would be released: %v", err)
				}
				want := consumer.Request{Namespace: "test", Name: "claim", UID: "uid", Node: "worker-a"}
				if len(retries.calls) != 1 || retries.calls[0] != want || retries.bytes[0] != original.CapacityBytes || retries.groups[0] != volumeapi.DefaultPoolGroup {
					t.Fatalf("legacy capacity wait lost its original placement: %+v", retries)
				}
				waiting, err := registry.Get(ctx, id)
				if err != nil || waiting.UID != original.UID || !waiting.Initializing() || waiting.InitialPool != nil || service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
					t.Fatalf("capacity wait gained execution authority: state=%+v err=%v", waiting, err)
				}
				// Recovery on the same node must converge using the same Volume UID.
				pool, _ := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
				if err := unstructured.SetNestedField(pool.Object, "2Gi", "spec", "capacity", "limit"); err != nil {
					t.Fatal(err)
				}
				if _, err := api.Resource(volumeapi.PoolResource).Update(ctx, pool, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				probe.stats.AvailableBytes = 1 << 30
				if _, err := service.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := service.CreateVolume(ctx, req); err != nil {
					t.Fatalf("legacy capacity wait did not resume: %v", err)
				}
				completed, _ := registry.Get(ctx, id)
				if completed.UID != original.UID || completed.Phase != volumeapi.PhaseReady || completed.InitialNode != "worker-a" || completed.CurrentCopy == nil || completed.CurrentCopy.PoolUID != "pool-a-uid" {
					t.Fatalf("capacity recovery changed placement: %+v", completed)
				}
			})
		}
	}
}

func TestInitializationNeverAdoptsExecutionEvidence(t *testing.T) {
	for _, field := range []string{"creationOperationID", "ownerNode", "phase", "cleanup"} {
		t.Run(field, func(t *testing.T) {
			api, registry, newService, probe := initializationFixture(t)
			id := legacyInitialization(t, api, "pvc-uid")
			object, _ := api.Resource(volumeapi.VolumeResource).Get(context.Background(), id, metav1.GetOptions{})
			value := any("unexpected")
			if field == "cleanup" {
				value = map[string]any{}
			}
			object.Object["status"] = map[string]any{field: value}
			if _, err := api.Resource(volumeapi.VolumeResource).UpdateStatus(context.Background(), object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := newService().CreateVolume(context.Background(), validCreateRequest("worker-a")); err == nil {
				t.Fatal("accepted execution evidence as an unstarted intent")
			}
			states, _ := registry.ListVolumes(context.Background())
			if _, err := poolcapacity.ReservedBytesForPool(states, nil, "pool-b-uid"); err == nil || probe.callCount() != 0 {
				t.Fatalf("malformed state was ignored: %v", err)
			}
		})
	}
}

func TestLegacyInitializationRequiresUpgradedCRD(t *testing.T) {
	api, _, newService, _ := initializationFixture(t)
	legacyInitialization(t, api, "pvc-uid")
	api.PrependReactor("update", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "" {
			// Simulate the old structural schema pruning the unknown spec field.
			object := a.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			unstructured.RemoveNestedField(object.Object, "spec", "initialPool")
		}
		return false, nil, nil
	})
	if _, err := newService().CreateVolume(context.Background(), validCreateRequest("worker-a")); err == nil || !strings.Contains(err.Error(), "upgrade the Volume CRD") {
		t.Fatalf("pruned reservation did not fail closed: %v", err)
	}
}

func TestInitializingReservationRejectsPoolReplacement(t *testing.T) {
	api, registry, newService, _ := initializationFixture(t)
	api.PrependReactor("update", "shiftpvvolumes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "status" {
			return true, nil, errors.New("initial status unavailable")
		}
		return false, nil, nil
	})
	req := validCreateRequest("worker-a")
	if _, err := newService().CreateVolume(context.Background(), req); err == nil {
		t.Fatal("expected interrupted initialization")
	}
	pool, _ := api.Resource(volumeapi.PoolResource).Get(context.Background(), "pool-a", metav1.GetOptions{})
	pool.SetUID("replacement-pool")
	if _, err := api.Resource(volumeapi.PoolResource).Update(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := newService().CreateVolume(context.Background(), req); err == nil {
		t.Fatal("initial reservation moved to a replacement Pool")
	}
	assertInitialReservation(t, registry, "pool-a-uid", req.CapacityRange.RequiredBytes)
	assertInitialReservation(t, registry, "replacement-pool", 0)
}
