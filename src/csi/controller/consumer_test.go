package controller

import (
	"context"
	"errors"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
)

type placementStub struct {
	result consumer.Result
	err    error
	calls  []consumer.Request
}

func (s *placementStub) Inspect(_ context.Context, q consumer.Request) (consumer.Result, error) {
	s.calls = append(s.calls, q)
	return s.result, s.err
}

type retryRecorder struct {
	calls  []consumer.Request
	bytes  []int64
	groups []string
}

func (r *retryRecorder) Register(ns, name, uid, node, group string, size int64) {
	r.calls = append(r.calls, consumer.Request{Namespace: ns, Name: name, UID: uid, Node: node})
	r.bytes = append(r.bytes, size)
	r.groups = append(r.groups, group)
}

func TestCapacityDenialUsesInjectedPlacementContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		placement consumer.Placement
		size      int64
		want      codes.Code
		err       error
		hint      string
	}{
		{name: "scheduler may reschedule", placement: consumer.Reschedulable, want: codes.ResourceExhausted},
		{name: "fixed consumer retries", placement: consumer.Fixed, want: codes.Unavailable},
		{name: "unknown consumer retries", placement: consumer.Unknown, want: codes.Unavailable},
		{name: "fixed oversized request retains node", placement: consumer.Fixed, size: 129 << 20, want: codes.Unavailable, hint: "change capacity configuration"},
		{name: "unknown oversized request retains node", placement: consumer.Unknown, size: 129 << 20, want: codes.Unavailable},
		{name: "scheduler oversized request may reschedule", placement: consumer.Reschedulable, size: 129 << 20, want: codes.ResourceExhausted},
		{name: "observation failure retries", err: errors.New("injected outage"), want: codes.Unavailable},
		{name: "identity conflict cannot reschedule", err: consumer.ErrIdentity, want: codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			service := capacityService(client, "128Mi", map[string]volumeapi.State{"old": {UID: "old", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 80 << 20, CurrentCopy: capacityCopy("pool-uid", "worker-a")}}, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30}})
			inspection := &placementStub{result: consumer.Result{Placement: tc.placement, Reason: "injected contract"}, err: tc.err}
			service.ConsumerPlacement = inspection
			retries := &retryRecorder{}
			service.RetryRequests = retries
			req := validCreateRequest("worker-a")
			req.Parameters = map[string]string{PVCNamespaceKey: "test", PVCNameKey: "claim"}
			if tc.size != 0 {
				req.CapacityRange.RequiredBytes = tc.size
			}
			_, err := service.CreateVolume(context.Background(), req)
			if status.Code(err) != tc.want {
				t.Fatalf("%v want %s", err, tc.want)
			}
			expected := consumer.Request{Namespace: "test", Name: "claim", UID: "uid", Node: "worker-a"}
			if len(inspection.calls) != 1 || inspection.calls[0] != expected {
				t.Fatalf("injection did not receive exact identity: %v", inspection.calls)
			}
			if len(client.Actions()) != 0 {
				t.Fatalf("CSI bypassed injected inspector: %v", client.Actions())
			}
			if len(service.Volumes.(*capacityTrackingVolumeRegistry).volumes) != 1 || service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("capacity denial created an intent or copy")
			}
			if tc.hint != "" && !strings.Contains(err.Error(), tc.hint) {
				t.Fatalf("missing actionable hint: %v", err)
			}
			wantsWake := tc.want == codes.Unavailable && tc.err == nil
			if wantsWake {
				if len(retries.calls) != 1 || retries.calls[0] != expected || retries.bytes[0] != req.CapacityRange.RequiredBytes || retries.groups[0] != "default" {
					t.Fatalf("retry identity mismatch: %+v", retries)
				}
			} else if len(retries.calls) != 0 {
				t.Fatal("invalid observation registered a waiter")
			}
		})
	}
}

func TestPrimeCapacityRetryBeforeAndAfterImporterCreation(t *testing.T) {
	for _, kind := range []string{"VolumeImportSource", "VolumeUploadSource"} {
		t.Run(kind, func(t *testing.T) {
			pvc := scheduledScratchPVC("worker-a")
			control := true
			pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "target", UID: "target-uid", Controller: &control}}
			pvc.Annotations[consumer.PopulatorKind] = kind
			group := "cdi.kubevirt.io"
			target := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "vmtest", UID: "target-uid", Annotations: map[string]string{consumer.SelectedNode: "worker-a", consumer.PrimeName: pvc.Name}}, Spec: corev1.PersistentVolumeClaimSpec{DataSourceRef: &corev1.TypedObjectReference{APIGroup: &group, Kind: kind, Name: "source"}}}
			client := fake.NewClientset(pvc, target, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "vmtest"}})
			service := capacityService(client, "128Mi", map[string]volumeapi.State{"old": {UID: "old", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 80 << 20, CurrentCopy: capacityCopy("pool-uid", "worker-a")}}, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}})
			req := validCreateRequest("worker-a")
			req.Parameters = map[string]string{PVCNameKey: pvc.Name, PVCNamespaceKey: pvc.Namespace}
			assertDenial := func() {
				t.Helper()
				if _, err := service.CreateVolume(context.Background(), req); status.Code(err) != codes.Unavailable {
					t.Fatalf("prime lost same-node retry: %v", err)
				}
			}
			assertDenial() // No importer exists yet.
			pod := scratchConsumerPod("worker-a")
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: pvc.Name, UID: pvc.UID, Controller: &control}}
			if _, err := client.CoreV1().Pods("vmtest").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			assertDenial() // Reverse ownership does not change the decision.
			registry := service.Volumes.(*capacityTrackingVolumeRegistry)
			delete(registry.volumes, "old")
			for range 2 {
				if _, err := service.CreateVolume(context.Background(), req); err != nil {
					t.Fatalf("capacity return did not converge: %v", err)
				}
			}
			id, _ := volume.IDFromName(req.Name)
			if len(registry.volumes) != 1 || registry.volumes[id].CurrentCopy.PoolUID != "pool-uid" {
				t.Fatal("retry duplicated or moved capacity intent")
			}
			observed, err := client.CoreV1().PersistentVolumeClaims("vmtest").Get(context.Background(), pvc.Name, metav1.GetOptions{})
			if err != nil || observed.Annotations[consumer.SelectedNode] != "worker-a" {
				t.Fatalf("selected node changed: %v", err)
			}
		})
	}
}

func TestSchedulerManagedClaimStillReschedules(t *testing.T) {
	pvc := scheduledScratchPVC("worker-a")
	pvc.OwnerReferences = nil
	pod := scratchConsumerPod("")
	service := capacityService(fake.NewClientset(pvc, pod), "128Mi", map[string]volumeapi.State{"old": {UID: "old", Phase: volumeapi.PhaseReady, OwnerNode: "worker-a", CapacityBytes: 80 << 20, CurrentCopy: capacityCopy("pool-uid", "worker-a")}}, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30}})
	req := validCreateRequest("worker-a")
	req.Parameters = map[string]string{PVCNameKey: pvc.Name, PVCNamespaceKey: pvc.Namespace}
	if _, err := service.CreateVolume(context.Background(), req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("scheduler rescheduling regressed: %v", err)
	}
}
