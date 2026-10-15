package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

type placementSnapshotRegistry struct {
	PoolCapacityRegistry
	snapshot volumeapi.PoolSnapshot
}

func (r placementSnapshotRegistry) ObservePools(context.Context) (volumeapi.PoolSnapshot, error) {
	return r.snapshot, nil
}

func TestMissingPoolUsesPlacementAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		placement  consumer.Placement
		err        error
		registered bool
		want       codes.Code
		wantWake   bool
	}{
		{name: "scheduler can select another node", placement: consumer.Reschedulable, want: codes.ResourceExhausted},
		{name: "fixed consumer waits for registration", placement: consumer.Fixed, want: codes.Unavailable, wantWake: true},
		{name: "unknown consumer waits for registration", placement: consumer.Unknown, want: codes.Unavailable, wantWake: true},
		{name: "consumer observation failure", err: errors.New("consumer API unavailable"), want: codes.Unavailable},
		{name: "changed PVC identity", err: consumer.ErrIdentity, want: codes.FailedPrecondition},
		{name: "registered Pool waits for readiness", registered: true, placement: consumer.Reschedulable, want: codes.Unavailable, wantWake: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}
			service := capacityService(fake.NewClientset(), "1Gi", nil, probe)
			registered := []volumeapi.Pool{
				{Name: "other-group", NodeName: "worker-a", PoolGroup: "default"},
				{Name: "other-node", NodeName: "worker-b", PoolGroup: "fast"},
			}
			if tc.registered {
				registered = append(registered, volumeapi.Pool{Name: "recovering", NodeName: "worker-a", PoolGroup: "fast"})
			}
			service.CapacityPools = placementSnapshotRegistry{PoolCapacityRegistry: service.CapacityPools, snapshot: volumeapi.PoolSnapshot{Registered: registered}}
			inspection := &placementStub{result: consumer.Result{Placement: tc.placement, Reason: "injected placement"}, err: tc.err}
			service.ConsumerPlacement = inspection
			retries := &retryRecorder{}
			service.RetryRequests = retries
			request := validCreateRequest("worker-a")
			request.Parameters = map[string]string{PVCNamespaceKey: "test", PVCNameKey: "claim", PoolGroupKey: "fast"}
			_, err := service.CreateVolume(context.Background(), request)
			if status.Code(err) != tc.want {
				t.Fatalf("error = %v, want %s", err, tc.want)
			}
			if !strings.Contains(err.Error(), `group "fast"`) {
				t.Fatalf("missing group diagnosis: %v", err)
			}
			expected := consumer.Request{Namespace: "test", Name: "claim", UID: "uid", Node: "worker-a"}
			if tc.registered {
				if len(inspection.calls) != 0 {
					t.Fatal("temporary readiness failure inspected consumers to authorize rescheduling")
				}
			} else if len(inspection.calls) != 1 || inspection.calls[0] != expected {
				t.Fatalf("consumer identity = %v", inspection.calls)
			}
			if tc.wantWake {
				if len(retries.calls) != 1 || retries.calls[0] != expected || retries.groups[0] != "fast" || retries.bytes[0] != 0 {
					t.Fatalf("observation retry = %+v", retries)
				}
			} else if len(retries.calls) != 0 {
				t.Fatalf("registered an unauthorized waiter: %+v", retries)
			}
			if probe.callCount() != 0 || service.Operator.(*fakeDirectoryOperator).createCalls != 0 || len(service.Volumes.(*capacityTrackingVolumeRegistry).volumes) != 0 {
				t.Fatal("missing eligible Pool triggered capacity measurement, intent or filesystem work")
			}
		})
	}
}

func TestMissingPoolCannotRescheduleWithoutConsumerInspector(t *testing.T) {
	service := capacityService(fake.NewClientset(), "1Gi", nil, &fakePoolCapacityProbe{})
	service.CapacityPools = placementSnapshotRegistry{PoolCapacityRegistry: service.CapacityPools}
	service.ConsumerPlacement = nil
	if _, err := service.CreateVolume(context.Background(), validCreateRequest("worker-a")); status.Code(err) != codes.Unavailable {
		t.Fatalf("missing inspector allowed node selection: %v", err)
	}
}
