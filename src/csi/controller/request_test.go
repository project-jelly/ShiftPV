package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateRejectsUnsupportedInputBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*csi.CreateVolumeRequest)
	}{
		{"snapshot source", func(r *csi.CreateVolumeRequest) {
			r.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snapshot"}}}
		}},
		{"clone source", func(r *csi.CreateVolumeRequest) {
			r.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"}}}
		}},
		{"empty source", func(r *csi.CreateVolumeRequest) { r.VolumeContentSource = &csi.VolumeContentSource{} }},
		{"mutable parameters", func(r *csi.CreateVolumeRequest) { r.MutableParameters = map[string]string{"tier": "fast"} }},
		{"negative minimum", func(r *csi.CreateVolumeRequest) { r.CapacityRange.RequiredBytes = -1 }},
		{"negative maximum", func(r *csi.CreateVolumeRequest) { r.CapacityRange.LimitBytes = -1 }},
		{"negative maximum without minimum", func(r *csi.CreateVolumeRequest) { r.CapacityRange = &csi.CapacityRange{LimitBytes: -1} }},
		{"inverted range", func(r *csi.CreateVolumeRequest) { r.CapacityRange.LimitBytes = r.CapacityRange.RequiredBytes - 1 }},
		{"preference outside requisite", func(r *csi.CreateVolumeRequest) {
			r.AccessibilityRequirements = retryTopology([]string{"worker-a"}, []string{"worker-b"})
		}},
		{"unsupported topology domain", func(r *csi.CreateVolumeRequest) {
			r.AccessibilityRequirements.Preferred[0].Segments["other/domain"] = "zone"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			probe := &fakePoolCapacityProbe{err: errors.New("admission must not run")}
			s := capacityService(client, "128Mi", nil, probe)
			r := validCreateRequest("worker-a")
			tc.change(r)
			if _, err := s.CreateVolume(context.Background(), r); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
			if len(s.Volumes.(*capacityTrackingVolumeRegistry).volumes) != 0 || probe.callCount() != 0 || s.Operator.(*fakeDirectoryOperator).createCalls != 0 || len(client.Actions()) != 0 {
				t.Fatal("invalid input reached admission, recorded an intent, or ran an effect")
			}
		})
	}
}

func TestCreateSupportsCapacityUpperBoundOnly(t *testing.T) {
	s := capacityService(fake.NewClientset(), "128Mi", nil, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}})
	r := validCreateRequest("worker-a")
	r.CapacityRange = &csi.CapacityRange{LimitBytes: 48 << 20}
	response, err := s.CreateVolume(context.Background(), r)
	if err != nil || response.GetVolume().GetCapacityBytes() != 48<<20 {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestCreateRetryUsesCompatibilityWithoutChangingIntent(t *testing.T) {
	for _, phase := range []string{volumeapi.PhasePending, volumeapi.PhaseNodeCreating, volumeapi.PhaseReady} {
		for _, tc := range []struct {
			name   string
			change func(*csi.CreateVolumeRequest)
			want   codes.Code
		}{
			{"identical", func(*csi.CreateVolumeRequest) {}, codes.OK},
			{"smaller minimum", func(r *csi.CreateVolumeRequest) { r.CapacityRange.RequiredBytes = 32 << 20 }, codes.OK},
			{"enclosing range", func(r *csi.CreateVolumeRequest) {
				r.CapacityRange = &csi.CapacityRange{RequiredBytes: 32 << 20, LimitBytes: 64 << 20}
			}, codes.OK},
			{"upper bound only", func(r *csi.CreateVolumeRequest) { r.CapacityRange = &csi.CapacityRange{LimitBytes: 96 << 20} }, codes.OK},
			{"larger minimum", func(r *csi.CreateVolumeRequest) { r.CapacityRange.RequiredBytes = 65 << 20 }, codes.AlreadyExists},
			{"smaller maximum", func(r *csi.CreateVolumeRequest) {
				r.CapacityRange = &csi.CapacityRange{RequiredBytes: 32 << 20, LimitBytes: 63 << 20}
			}, codes.AlreadyExists},
			{"changed preference within requisite", func(r *csi.CreateVolumeRequest) {
				r.AccessibilityRequirements = retryTopology([]string{"worker-b", "worker-a"}, []string{"worker-a", "worker-b"})
			}, codes.OK},
			{"required owner excluded", func(r *csi.CreateVolumeRequest) {
				r.AccessibilityRequirements = retryTopology([]string{"worker-b"}, []string{"worker-b"})
			}, codes.AlreadyExists},
			{"selected node changed", func(r *csi.CreateVolumeRequest) {
				r.AccessibilityRequirements = retryTopology([]string{"worker-b"}, nil)
			}, codes.AlreadyExists},
			{"changed Pool group", func(r *csi.CreateVolumeRequest) { r.Parameters[PoolGroupKey] = "other" }, codes.AlreadyExists},
		} {
			t.Run(phase+"/"+tc.name, func(t *testing.T) {
				probe := &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{TotalBytes: 1 << 30, AvailableBytes: 1 << 30}}
				s := capacityService(fake.NewClientset(), "128Mi", nil, probe)
				r := validCreateRequest("worker-a")
				first, err := s.CreateVolume(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
				registry := s.Volumes.(*capacityTrackingVolumeRegistry)
				id := first.Volume.VolumeId
				stored := registry.volumes[id]
				stored.Phase = phase
				stored.CreationOperationID = "create-" + stored.UID
				if phase != volumeapi.PhasePending {
					stored.CreationExecutor = &volumeapi.NodeExecutor{Namespace: "shiftpv-system", PodName: "node-a", PodUID: "pod-uid", NodeName: stored.OwnerNode}
					stored.CreationReceipt = &volumeapi.CreationReceipt{OperationID: stored.CreationOperationID, ExecutorUID: "pod-uid", ObservedAt: "2026-10-08T00:00:00Z", LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
					if !volumeapi.ValidCreationReceipt(stored) {
						t.Fatal("fixture has no valid creation receipt")
					}
				}
				registry.volumes[id] = stored
				s.Operator.(*fakeDirectoryOperator).createCalls = 0
				// A retry must use the existing hold even if new admission is impossible.
				probe.err = errors.New("new capacity admission must not run")
				calls := probe.callCount()
				tc.change(r)
				second, err := s.CreateVolume(context.Background(), r)
				if status.Code(err) != tc.want {
					t.Fatalf("response=%v err=%v want=%s", second, err, tc.want)
				}
				expected := stored
				if tc.want == codes.OK {
					expected.Phase = volumeapi.PhaseReady
					if !reflect.DeepEqual(first, second) {
						t.Fatalf("retry changed actual capacity, owner or volume ID: first=%v second=%v", first, second)
					}
				} else if s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
					t.Fatal("incompatible request ran an effect")
				}
				if len(registry.volumes) != 1 || !reflect.DeepEqual(expected, registry.volumes[id]) || probe.callCount() != calls {
					t.Fatal("retry changed stored Pool, copy, UID, operation or capacity hold")
				}
			})
		}
	}
}

func retryTopology(preferred, requisite []string) *csi.TopologyRequirement {
	topologies := func(nodes []string) []*csi.Topology {
		result := make([]*csi.Topology, 0, len(nodes))
		for _, node := range nodes {
			result = append(result, &csi.Topology{Segments: map[string]string{TopologyKey: node}})
		}
		return result
	}
	return &csi.TopologyRequirement{Preferred: topologies(preferred), Requisite: topologies(requisite)}
}

func TestCompatibleTopologyRetryChecksInjectedPVCPlacement(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result consumer.Placement
		err    error
		want   codes.Code
	}{
		{"fixed on original node", consumer.Fixed, nil, codes.OK},
		{"scheduler on original node", consumer.Reschedulable, nil, codes.OK},
		{"selected node changed", consumer.Unknown, consumer.ErrIdentity, codes.FailedPrecondition},
		{"unproven placement", consumer.Unknown, nil, codes.Unavailable},
		{"observation outage", consumer.Unknown, errors.New("lookup failed"), codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := capacityService(fake.NewClientset(), "128Mi", nil, &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}})
			r := validCreateRequest("worker-a")
			first, err := s.CreateVolume(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			registry := s.Volumes.(*capacityTrackingVolumeRegistry)
			before := registry.volumes[first.Volume.VolumeId]
			s.Operator.(*fakeDirectoryOperator).createCalls = 0
			inspection := &placementStub{result: consumer.Result{Placement: tc.result}, err: tc.err}
			s.ConsumerPlacement = inspection
			r.Parameters[PVCNamespaceKey], r.Parameters[PVCNameKey] = "test", "claim"
			r.AccessibilityRequirements = retryTopology([]string{"worker-b", "worker-a"}, []string{"worker-a", "worker-b"})
			// Existing namespace labels still control the response's mobility scope.
			_, err = s.Client.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			second, err := s.CreateVolume(context.Background(), r)
			if status.Code(err) != tc.want {
				t.Fatalf("response=%v err=%v", second, err)
			}
			expected := consumer.Request{Namespace: "test", Name: "claim", UID: "uid", Node: "worker-a"}
			if len(inspection.calls) != 1 || inspection.calls[0] != expected || !reflect.DeepEqual(before, registry.volumes[first.Volume.VolumeId]) {
				t.Fatal("retry bypassed placement identity or changed its intent")
			}
			if tc.want == codes.OK {
				if !reflect.DeepEqual(first, second) {
					t.Fatalf("response changed: %v", second)
				}
			} else if s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("unproven placement ran an effect")
			}
		})
	}
}

func TestCompatibleRetryPreservesRealRegistryIntentAcrossRestart(t *testing.T) {
	ctx := context.Background()
	r := validCreateRequest("worker-a")
	id, err := volume.IDFromName(r.Name)
	if err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: id, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "worker-a", Role: volume.RoleServing}
	parent := cleanupParentVolume(id, copy)
	_ = unstructured.SetNestedField(parent.Object, r.Name, "spec", "requestName")
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	if err != nil {
		t.Fatal(err)
	}
	parent.Object["status"] = map[string]any{"phase": volumeapi.PhasePending, "ownerNode": copy.NodeName, "creationOperationID": "create-" + copy.VolumeUID, "currentCopy": encoded}
	pool := cleanupPool(copy, false)
	_ = unstructured.SetNestedField(pool.Object, "/mnt/pool", "spec", "mountPath")
	_ = unstructured.SetNestedField(pool.Object, true, "status", "registrationApproved")
	identity := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": "installation"}}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, parent, pool, identity)
	probe := &fakePoolCapacityProbe{err: errors.New("existing intent must not re-enter capacity admission")}
	service := func(op *fakeDirectoryOperator, capacityAdmission bool) *Service {
		registry := &volumeapi.Registry{Client: api}
		s := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: op, Volumes: registry})
		if capacityAdmission {
			s.CapacityPools, s.CapacityProbe = registry, probe
		}
		return s
	}
	first := service(&fakeDirectoryOperator{createErr: retryableDirectoryError{errors.New("node response lost")}}, true)
	if _, err := first.CreateVolume(ctx, r); status.Code(err) != codes.Unavailable {
		t.Fatalf("first create=%v", err)
	}
	before, err := first.Volumes.Get(ctx, id)
	if err != nil || before.Phase != volumeapi.PhasePending {
		t.Fatalf("intent=%+v err=%v", before, err)
	}
	r.CapacityRange = &csi.CapacityRange{RequiredBytes: 32 << 20, LimitBytes: 96 << 20}
	r.AccessibilityRequirements = retryTopology([]string{"worker-b", "worker-a"}, []string{"worker-a", "worker-b"})
	for _, capacityAdmission := range []bool{false, true} {
		// Each service and registry is new; the durable API intent is the only shared state.
		restarted := service(&fakeDirectoryOperator{}, capacityAdmission)
		response, err := restarted.CreateVolume(ctx, r)
		if err != nil || response.GetVolume().GetCapacityBytes() != 64<<20 || response.GetVolume().GetVolumeContext()[NodeContextKey] != "worker-a" {
			t.Fatalf("response=%v err=%v", response, err)
		}
		after, err := restarted.Volumes.Get(ctx, id)
		expected := before
		expected.Phase = volumeapi.PhaseReady
		// API status encoding may turn an absent publication list into an empty list.
		expected.PublishedNodes = append([]string(nil), expected.PublishedNodes...)
		after.PublishedNodes = append([]string(nil), after.PublishedNodes...)
		if err != nil || !reflect.DeepEqual(after, expected) {
			t.Fatalf("intent changed: before=%+v after=%+v err=%v", expected, after, err)
		}
	}
	objects, err := api.Resource(volumeapi.VolumeResource).List(ctx, metav1.ListOptions{})
	if err != nil || len(objects.Items) != 1 || !reflect.DeepEqual(objects.Items[0].Object["spec"], parent.Object["spec"]) || probe.callCount() != 0 {
		t.Fatal("retry changed the durable spec, duplicated a volume or admitted capacity again")
	}
}
