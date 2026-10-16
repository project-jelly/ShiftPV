package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type readyReplayFixture struct {
	service *Service
	api     *dynamicfake.FakeDynamicClient
	client  *fake.Clientset
	request *csi.CreateVolumeRequest
	parent  *unstructured.Unstructured
	pool    *unstructured.Unstructured
	probe   *fakePoolCapacityProbe
	effects *replayEffects
}

type replayEffects struct{ calls int }

func (e *replayEffects) CreateCopy(context.Context, volume.CopyIdentity) error {
	e.calls++
	return errors.New("Ready replay must not execute creation")
}

func (e *replayEffects) FinalizeCreate(context.Context, volume.CopyIdentity) error {
	e.calls++
	return errors.New("Ready replay must not finalize creation")
}

func newReadyReplayFixture(t *testing.T, native, moved bool) *readyReplayFixture {
	t.Helper()
	request := validCreateRequest("worker-a")
	id, err := volume.IDFromName(request.Name)
	if err != nil {
		t.Fatal(err)
	}
	owner, poolName := "worker-a", "pool-a"
	if moved {
		owner, poolName = "worker-b", "pool-b"
	}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: poolName, PoolUID: poolName + "-uid", VolumeID: id, VolumeUID: "volume-uid", CopyID: "serving-copy", NodeName: owner, Role: volume.RoleServing}
	parent := cleanupParentVolume(id, copy)
	parent.Object["spec"] = map[string]any{"volumeID": id, "requestName": request.Name, "initialNode": "worker-a", "capacityBytes": int64(64 << 20), "initialPool": map[string]any{"name": "pool-a", "uid": "pool-a-uid"}}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	if err != nil {
		t.Fatal(err)
	}
	parent.Object["status"] = map[string]any{"phase": volumeapi.PhaseReady, "ownerNode": owner, "currentCopy": encoded, "publishedNodes": []any{owner}}
	if native {
		executor := volumeapi.NodeExecutor{Namespace: "shiftpv-system", PodName: "original-creator", PodUID: "creator-uid", NodeName: "worker-a"}
		receipt := volumeapi.CreationReceipt{OperationID: "create-volume-uid", ExecutorUID: executor.PodUID, ObservedAt: "2026-10-16T00:00:00Z", LocalReceiptDigest: strings.Repeat("a", 64)}
		for key, value := range map[string]any{"creationExecutor": &executor, "creationReceipt": &receipt} {
			encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := unstructured.SetNestedMap(parent.Object, encoded, "status", key); err != nil {
				t.Fatal(err)
			}
		}
		if err := unstructured.SetNestedField(parent.Object, "create-volume-uid", "status", "creationOperationID"); err != nil {
			t.Fatal(err)
		}
	}
	pool := cleanupPool(copy, false)
	pool.Object["spec"] = map[string]any{"nodeName": owner, "mountPath": "/mnt/shiftpv", "poolGroup": "storage"}
	// No current inventory/readiness is available; replay is not an allocation.
	pool.Object["status"] = map[string]any{"observedGeneration": int64(1), "conditions": []any{map[string]any{"type": "Ready", "status": "False", "reason": "NoSpace", "observedGeneration": int64(1)}}}
	pool.SetGeneration(3)
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, parent, pool)
	client := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workload", Labels: map[string]string{MobilityAdmissionLabel: mobilityEnabledValue}}})
	request.Parameters[PoolGroupKey] = "storage"
	request.Parameters[PVCNamespaceKey], request.Parameters[PVCNameKey] = "workload", "claim"
	registry := &volumeapi.Registry{Client: api}
	effects := &replayEffects{}
	probe := &fakePoolCapacityProbe{err: errors.New("Ready replay must not inspect physical capacity")}
	service := configuredService(&Service{Client: client, Namespace: "shiftpv-system", Operator: effects, Volumes: registry, CapacityPools: registry, CapacityProbe: probe, Cleanups: &cleanupapi.Store{Client: api}})
	return &readyReplayFixture{service: service, api: api, client: client, request: request, parent: parent, pool: pool, probe: probe, effects: effects}
}

func (f *readyReplayFixture) assertReadOnly(t *testing.T) {
	t.Helper()
	if f.effects.calls != 0 || f.probe.callCount() != 0 {
		t.Fatalf("effects=%d probes=%d", f.effects.calls, f.probe.callCount())
	}
	for _, actions := range [][]k8stesting.Action{f.api.Actions(), f.client.Actions()} {
		for _, action := range actions {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatalf("Ready replay mutated API: %#v", action)
			}
			if action.GetResource().Resource == "nodes" || action.GetResource().Resource == "pods" || action.GetResource().Resource == "persistentvolumeclaims" {
				t.Fatalf("Ready replay inspected original workload/Node: %#v", action)
			}
			if get, ok := action.(k8stesting.GetAction); ok && action.GetResource().Resource == "shiftpvpools" && get.GetName() != f.pool.GetName() {
				t.Fatalf("Ready replay inspected initial Pool: %#v", action)
			}
		}
	}
	stored, err := f.api.Resource(volumeapi.VolumeResource).Get(context.Background(), f.parent.GetName(), metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(stored, f.parent) {
		t.Fatalf("Ready Volume changed: err=%v before=%v after=%v", err, f.parent, stored)
	}
}

func TestReadyReplayUsesCurrentCopyWithoutNewEffects(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, moved := range []bool{false, true} {
			name := "legacy"
			if native {
				name = "native"
			}
			if moved {
				name += " moved"
			}
			t.Run(name, func(t *testing.T) {
				f := newReadyReplayFixture(t, native, moved)
				response, err := f.service.CreateVolume(context.Background(), f.request)
				if err != nil {
					t.Fatal(err)
				}
				owner, _, _ := unstructured.NestedString(f.parent.Object, "status", "ownerNode")
				if response.GetVolume().GetVolumeId() != f.parent.GetName() || response.GetVolume().GetCapacityBytes() != 64<<20 || response.GetVolume().GetVolumeContext()[NodeContextKey] != owner {
					t.Fatalf("wrong replay response: %v", response)
				}
				if !reflect.DeepEqual(response.GetVolume().GetAccessibleTopology(), retryTopology(nil, []string{owner}).Requisite) {
					t.Fatalf("wrong current topology: %v", response)
				}
				f.assertReadOnly(t)
			})
		}
	}
}

func TestReadyReplayRequiresCurrentOwnerRequisite(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		preferred, requisite []string
		want                 codes.Code
	}{
		{"preference remains on original owner", []string{"worker-a"}, []string{"worker-a", "worker-b"}, codes.OK},
		{"only current owner required", []string{"worker-b"}, []string{"worker-b"}, codes.OK},
		{"current owner excluded", []string{"worker-a"}, []string{"worker-a"}, codes.AlreadyExists},
		{"preference without requisite is advisory", []string{"worker-a"}, nil, codes.OK},
		{"invalid preferred subset", []string{"worker-a"}, []string{"worker-b"}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReadyReplayFixture(t, true, true)
			f.request.AccessibilityRequirements = retryTopology(tc.preferred, tc.requisite)
			_, err := f.service.CreateVolume(context.Background(), f.request)
			if status.Code(err) != tc.want {
				t.Fatalf("error=%v want=%s", err, tc.want)
			}
			f.assertReadOnly(t)
		})
	}
}

func TestReadyReplayAlsoValidatesWithoutCapacityAdmission(t *testing.T) {
	f := newReadyReplayFixture(t, true, true)
	f.service.CapacityPools, f.service.CapacityProbe = nil, nil
	if _, err := f.service.CreateVolume(context.Background(), f.request); err != nil {
		t.Fatal(err)
	}
	f.assertReadOnly(t)
}

func TestReadyReplayRejectsChangedIdentityOrCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*readyReplayFixture)
		want   codes.Code
	}{
		{"stored request name", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "other-request", "spec", "requestName")
		}, codes.AlreadyExists},
		{"capacity range", func(f *readyReplayFixture) { f.request.CapacityRange.RequiredBytes = 65 << 20 }, codes.AlreadyExists},
		{"requested group", func(f *readyReplayFixture) { f.request.Parameters[PoolGroupKey] = "other" }, codes.AlreadyExists},
		{"current Pool group", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.pool.Object, "other", "spec", "poolGroup")
		}, codes.AlreadyExists},
		{"current Pool UID", func(f *readyReplayFixture) { f.pool.SetUID("replacement-pool") }, codes.FailedPrecondition},
		{"current Pool Node", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.pool.Object, "worker-c", "spec", "nodeName")
		}, codes.FailedPrecondition},
		{"copy Volume ID", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "status", "currentCopy", "volumeID")
		}, codes.FailedPrecondition},
		{"copy Volume UID", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "other-volume", "status", "currentCopy", "volumeUID")
		}, codes.FailedPrecondition},
		{"current owner", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "worker-c", "status", "ownerNode")
		}, codes.FailedPrecondition},
		{"copy role", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, string(volume.RoleIncoming), "status", "currentCopy", "role")
		}, codes.FailedPrecondition},
		{"receipt executor UID", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "other-executor", "status", "creationReceipt", "executorUID")
		}, codes.FailedPrecondition},
		{"receipt digest", func(f *readyReplayFixture) {
			_ = unstructured.SetNestedField(f.parent.Object, "invalid", "status", "creationReceipt", "localReceiptDigest")
		}, codes.FailedPrecondition},
		{"missing receipt", func(f *readyReplayFixture) {
			unstructured.RemoveNestedField(f.parent.Object, "status", "creationReceipt")
		}, codes.FailedPrecondition},
		{"missing executor", func(f *readyReplayFixture) {
			unstructured.RemoveNestedField(f.parent.Object, "status", "creationExecutor")
		}, codes.FailedPrecondition},
		{"terminating Volume", func(f *readyReplayFixture) { now := metav1.Now(); f.parent.SetDeletionTimestamp(&now) }, codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReadyReplayFixture(t, true, true)
			tc.change(f)
			if err := f.api.Tracker().Update(volumeapi.VolumeResource, f.parent, ""); err != nil {
				t.Fatal(err)
			}
			if err := f.api.Tracker().Update(volumeapi.PoolResource, f.pool, ""); err != nil {
				t.Fatal(err)
			}
			_, err := f.service.CreateVolume(context.Background(), f.request)
			if status.Code(err) != tc.want {
				t.Fatalf("error=%v want=%s", err, tc.want)
			}
			f.assertReadOnly(t)
		})
	}
}

func TestReadyReplayCannotGuessMissingPoolOrFailedObservation(t *testing.T) {
	for _, missing := range []bool{false, true} {
		f := newReadyReplayFixture(t, true, true)
		want := codes.Unavailable
		if missing {
			want = codes.FailedPrecondition
			if err := f.api.Tracker().Delete(volumeapi.PoolResource, "", f.pool.GetName()); err != nil {
				t.Fatal(err)
			}
		} else {
			f.api.PrependReactor("get", "shiftpvpools", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewServiceUnavailable("Pool observation unavailable")
			})
		}
		_, err := f.service.CreateVolume(context.Background(), f.request)
		if status.Code(err) != want {
			t.Fatalf("missing=%t error=%v want=%s", missing, err, want)
		}
		f.assertReadOnly(t)
	}
}
