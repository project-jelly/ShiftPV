package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPoolObservationRecoveryNotifiesWaitingClaim(t *testing.T) {
	for _, failure := range []string{"not ready", "no matching pool", "probe outdated", "invalid configuration"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "claim", UID: types.UID("uid"), Annotations: map[string]string{selectedNodeAnnotation: "worker-a"}}}
			kube := fake.NewClientset(pvc)
			probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}}
			service := capacityService(kube, "1Gi", nil, probe)
			pools := service.CapacityPools.(*fakePoolCapacityRegistry)
			retries := provisioning.NewRetries(kube, pools)
			service.RetryRequests = retries
			service.ConsumerPlacement = &placementStub{result: consumer.Result{Placement: consumer.Fixed}}
			request := validCreateRequest("worker-a")
			request.Parameters = map[string]string{PVCNamespaceKey: "test", PVCNameKey: "claim"}
			switch failure {
			case "not ready":
				pools.err = volumeapi.ErrPoolNotReady
			case "no matching pool":
				pools.pool.NodeName = "other"
			case "probe outdated":
				probe.err = fmt.Errorf("generation changed: %w", volumeapi.ErrPoolNotReady)
			case "invalid configuration":
				pools.err = volumeapi.ErrPoolConfiguration
			}
			if _, err := service.CreateVolume(ctx, request); err == nil {
				t.Fatal("missing admission failure")
			}
			if service.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("failed observation created a directory")
			}
			if failure == "probe outdated" {
				pools.err = volumeapi.ErrPoolNotReady
			}
			if err := retries.Reconcile(ctx); err != nil && !errors.Is(err, pools.err) {
				t.Fatal(err)
			}
			pools.err, probe.err, pools.pool.NodeName = nil, nil, "worker-a"
			// Recovery notification is independent of additional quota. Actual
			// admission below must still reject this now-full Pool.
			pools.pool.CapacityLimit = "32Mi"
			pools.pool.Status.LastProbeTime = metav1.Now()
			if err := retries.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			current, err := kube.CoreV1().PersistentVolumeClaims("test").Get(ctx, "claim", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			wantWake := failure != "invalid configuration"
			if (current.Annotations["shiftpv.io/capacity-retry"] != "") != wantWake {
				t.Fatalf("recovery notification=%v want=%v", current.Annotations, wantWake)
			}
			if current.Annotations[selectedNodeAnnotation] != "worker-a" {
				t.Fatal("observation wait changed selected node")
			}
			if _, err := service.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
				t.Fatalf("notification bypassed capacity admission: %v", err)
			}
		})
	}
}
