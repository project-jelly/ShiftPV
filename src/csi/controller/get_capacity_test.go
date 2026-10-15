package controller

import (
	"context"
	"errors"
	"maps"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

type capacityQueryRegistry struct {
	snapshot volumeapi.PoolSnapshot
	volumes  map[string]volumeapi.State
	moves    []volumeapi.Move
	failAt   string
	err      error
	calls    map[string]int
}

func (r *capacityQueryRegistry) read(operation string) error {
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[operation]++
	if r.failAt == operation {
		return r.err
	}
	return nil
}

func (r *capacityQueryRegistry) ObservePools(context.Context) (volumeapi.PoolSnapshot, error) {
	return r.snapshot, r.read("pools")
}

func (r *capacityQueryRegistry) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return r.volumes, r.read("volumes")
}

func (r *capacityQueryRegistry) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return r.moves, r.read("moves")
}

func (*capacityQueryRegistry) PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	panic("capacity query must not resolve live execution authority")
}

func queryPool(uid, node, group string, limit, available int64) volumeapi.Pool {
	return volumeapi.Pool{Name: uid, UID: uid, NodeName: node, PoolGroup: group, CapacityLimit: strconv.FormatInt(limit, 10),
		Status: volumeapi.PoolStatus{FilesystemTotalBytes: math.MaxInt64,
			FilesystemAvailableBytes: ptr.To(available), FilesystemAvailableInodes: ptr.To(int64(100))}}
}

func queryOwner(pool, node string, size int64) volumeapi.State {
	return volumeapi.State{OwnerNode: node, CapacityBytes: size, CurrentCopy: &volume.CopyIdentity{PoolUID: pool, NodeName: node}}
}

func assertCapacityQuery(t *testing.T, service *Service, request *csi.GetCapacityRequest, total, maximum int64) {
	t.Helper()
	response, err := service.GetCapacity(context.Background(), request)
	if err != nil || response == nil || response.AvailableCapacity != total || response.MaximumVolumeSize == nil || response.MaximumVolumeSize.Value != maximum {
		t.Fatalf("capacity response=%+v err=%v, want total=%d maximum=%d", response, err, total, maximum)
	}
}

func TestGetCapacityAggregatesIndependentPoolHeadroom(t *testing.T) {
	pools := []volumeapi.Pool{
		queryPool("a", "node-a", "default", 100, 80),
		queryPool("b", "node-a", "default", 100, 40),
		queryPool("c", "node-b", "default", 50, 60),
		queryPool("d", "node-a", "other", 100, 80),
	}
	for _, test := range []struct {
		name    string
		request *csi.GetCapacityRequest
		total   int64
		maximum int64
	}{
		{"optional fields omitted", &csi.GetCapacityRequest{}, 120, 50},
		{"empty topology", &csi.GetCapacityRequest{AccessibleTopology: &csi.Topology{}}, 120, 50},
		{"node aggregate and single volume differ", &csi.GetCapacityRequest{AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: "node-a"}}}, 70, 40},
		{"different group", &csi.GetCapacityRequest{Parameters: map[string]string{PoolGroupKey: "other"}}, 80, 80},
		{"missing group", &csi.GetCapacityRequest{Parameters: map[string]string{PoolGroupKey: "missing"}}, 0, 0},
		{"missing node", &csi.GetCapacityRequest{AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: "missing"}}}, 0, 0},
		{"explicit supported request", &csi.GetCapacityRequest{Parameters: map[string]string{CapacityEnforcementKey: "none"}, VolumeCapabilities: validCreateRequest("node-a").VolumeCapabilities}, 120, 50},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Registered: pools, Ready: pools},
				volumes: map[string]volumeapi.State{"owned": queryOwner("a", "node-a", 70)}}
			// No Kubernetes client, operator, consumer, or live probe is wired.
			assertCapacityQuery(t, &Service{CapacityPools: registry}, test.request, test.total, test.maximum)
			wantLedgerReads := 1
			if test.total == 0 {
				wantLedgerReads = 0
			}
			if registry.calls["pools"] != 1 || registry.calls["volumes"] != wantLedgerReads || registry.calls["moves"] != wantLedgerReads {
				t.Fatalf("capacity query repeated API snapshots: %v", registry.calls)
			}
		})
	}
}

func TestGetCapacitySkipsUnknownOrInvalidSpace(t *testing.T) {
	for name, change := range map[string]func(*volumeapi.Pool){
		"missing available": func(p *volumeapi.Pool) { p.Status.FilesystemAvailableBytes = nil },
		"missing inodes":    func(p *volumeapi.Pool) { p.Status.FilesystemAvailableInodes = nil },
		"missing total":     func(p *volumeapi.Pool) { p.Status.FilesystemTotalBytes = 0 },
		"negative total":    func(p *volumeapi.Pool) { p.Status.FilesystemTotalBytes = -1 },
		"negative available": func(p *volumeapi.Pool) {
			p.Status.FilesystemAvailableBytes = ptr.To(int64(-1))
		},
		"available exceeds total": func(p *volumeapi.Pool) { p.Status.FilesystemTotalBytes = 1 },
		"zero bytes":              func(p *volumeapi.Pool) { p.Status.FilesystemAvailableBytes = ptr.To(int64(0)) },
		"zero inodes":             func(p *volumeapi.Pool) { p.Status.FilesystemAvailableInodes = ptr.To(int64(0)) },
		"negative inodes":         func(p *volumeapi.Pool) { p.Status.FilesystemAvailableInodes = ptr.To(int64(-1)) },
		"invalid limit":           func(p *volumeapi.Pool) { p.CapacityLimit = "invalid" },
		"zero limit":              func(p *volumeapi.Pool) { p.CapacityLimit = "0" },
		"inexact limit":           func(p *volumeapi.Pool) { p.CapacityLimit = "0.1" },
	} {
		t.Run(name, func(t *testing.T) {
			unavailable := queryPool("unknown", "node-a", "default", 100, 50)
			change(&unavailable)
			registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{
				unavailable, queryPool("healthy", "node-b", "default", 10, 5),
			}}}
			assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, 5, 5)
		})
	}
}

func TestGetCapacityKeepsDurableReservationsAcrossLifecycle(t *testing.T) {
	source := &volume.CopyIdentity{PoolUID: "source", NodeName: "node-a"}
	destination := &volume.CopyIdentity{PoolUID: "destination", NodeName: "node-b"}
	for _, phase := range []string{"initializing", "legacy unbound", "retained", "deleting", "move precommit", "move postcommit", "move terminal without proof", "move cleanup settled", "rollback settled"} {
		t.Run(phase, func(t *testing.T) {
			state := queryOwner("source", "node-a", 30)
			wantSource, wantDestination := int64(70), int64(100)
			var moves []volumeapi.Move
			switch phase {
			case "initializing", "legacy unbound":
				state = volumeapi.State{UID: "volume-uid", RequestName: "pvc-uid", InitialNode: "node-a", CapacityBytes: 30,
					Finalizers: []string{volumeapi.VolumeProtectionFinalizer}}
				if phase == "initializing" {
					state.InitialPool = &volumeapi.PoolReference{Name: "source", UID: "source"}
				} else {
					wantSource = 100
				}
			case "retained":
				state.Phase = volumeapi.PhaseReady // There need not be a live PVC.
			case "deleting":
				state.Phase, state.Deleting = volumeapi.PhaseDeleting, true
			default:
				state.ActiveMove = "move"
				move := volumeapi.Move{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "v"}, Status: volumeapi.MoveStatus{
					SourceCopy: source, DestinationPoolUID: "destination", CapacityApproved: true,
				}}
				wantDestination = 70
				if phase == "move postcommit" || phase == "move terminal without proof" || phase == "move cleanup settled" {
					state.OwnerNode, state.CurrentCopy = "node-b", destination
					// The immutable original reservation is history after initialization.
					state.InitialPool = &volumeapi.PoolReference{Name: "source", UID: "source"}
				}
				if phase == "move terminal without proof" || phase == "move cleanup settled" {
					move.Status.Phase = "Succeeded"
				}
				if phase == "move cleanup settled" {
					move.Status.CleanupPhase, state.ActiveMove = "Completed", ""
					wantSource = 100
				}
				if phase == "rollback settled" {
					move.Status.Phase, move.Status.RecoveryPhase = "Blocked", "Recovered"
					move.Status.CapacityApproved, move.Status.CapacityReason = false, "RecoverySettled"
					state.ActiveMove, wantDestination = "", 100
				}
				moves = []volumeapi.Move{move}
			}
			registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{
				queryPool("source", "node-a", "default", 100, 100), queryPool("destination", "node-b", "default", 100, 100),
			}}, volumes: map[string]volumeapi.State{"v": state}, moves: moves}
			service := &Service{CapacityPools: registry}
			assertCapacityQuery(t, service, &csi.GetCapacityRequest{}, wantSource+wantDestination, max(wantSource, wantDestination))
			assertCapacityQuery(t, service, &csi.GetCapacityRequest{AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: "node-a"}}}, wantSource, wantSource)
		})
	}
}

func TestGetCapacityHandlesOvercommitAndSaturatesAggregate(t *testing.T) {
	const perPool int64 = 5_000_000_000_000_000_000
	registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{
		queryPool("a", "node-a", "default", perPool, perPool),
		queryPool("b", "node-b", "default", perPool, perPool),
	}}}
	// Kubernetes Quantity.AsInt64 accepts this exact decimal exponent, while
	// a 19-digit decimal literal uses its non-integer internal representation.
	registry.snapshot.Ready[0].CapacityLimit, registry.snapshot.Ready[1].CapacityLimit = "5E", "5E"
	assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, math.MaxInt64, perPool)
	registry.volumes = map[string]volumeapi.State{"a": queryOwner("a", "node-a", 100), "b": queryOwner("b", "node-b", 100)}
	registry.snapshot.Ready[0].CapacityLimit, registry.snapshot.Ready[1].CapacityLimit = "99", "100"
	assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, 0, 0)
}

func TestGetCapacityRejectsInconsistentOwnership(t *testing.T) {
	for name, registry := range map[string]*capacityQueryRegistry{
		"owner identity missing": {volumes: map[string]volumeapi.State{"v": {CapacityBytes: 30}}},
		"move volume missing": {moves: []volumeapi.Move{{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "missing"},
			Status: volumeapi.MoveStatus{CapacityApproved: true}}}},
	} {
		t.Run(name, func(t *testing.T) {
			registry.snapshot.Ready = []volumeapi.Pool{queryPool("a", "node-a", "default", 100, 100)}
			response, err := (&Service{CapacityPools: registry}).GetCapacity(context.Background(), &csi.GetCapacityRequest{})
			if response != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("inconsistent ledger advertised free space: %+v %v", response, err)
			}
		})
	}
}

func TestGetCapacityPreservesAPIErrorClassification(t *testing.T) {
	for _, operation := range []string{"pools", "volumes", "moves"} {
		for name, err := range map[string]error{
			"conflict": apierrors.NewConflict(schema.GroupResource{Resource: "shiftpvpools"}, "pool", errors.New("modified")),
			"timeout":  apierrors.NewTimeoutError("API unavailable", 1), "cancelled": context.Canceled,
		} {
			t.Run(operation+"/"+name, func(t *testing.T) {
				registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{queryPool("a", "node-a", "default", 100, 100)}}, failAt: operation, err: err}
				response, got := (&Service{CapacityPools: registry}).GetCapacity(context.Background(), &csi.GetCapacityRequest{})
				want := codes.Unavailable
				if name == "cancelled" {
					want = codes.Canceled
				}
				if response != nil || status.Code(got) != want {
					t.Fatalf("API failure became a capacity result: %+v %v", response, got)
				}
			})
		}
	}
}

func TestGetCapacityRejectsUnsupportedRequestsBeforeReadingPools(t *testing.T) {
	for name, request := range map[string]*csi.GetCapacityRequest{
		"parameter":                         {Parameters: map[string]string{"unknown": "value"}},
		"reserved typo":                     {Parameters: map[string]string{"csi.storage.k8s.io/fs-type": "ext4"}},
		"reserved metadata in StorageClass": {Parameters: map[string]string{PVCNameKey: "pvc"}},
		"deprecated secret parameter":       {Parameters: map[string]string{"csiProvisionerSecretName": "credentials"}},
		"enforcement":                       {Parameters: map[string]string{CapacityEnforcementKey: "quota"}},
		"group syntax":                      {Parameters: map[string]string{PoolGroupKey: "INVALID"}},
		"topology key":                      {AccessibleTopology: &csi.Topology{Segments: map[string]string{"unknown": "node-a"}}},
		"empty node":                        {AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: ""}}},
		"extra topology":                    {AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: "node-a", "zone": "a"}}},
		"nil capability":                    {VolumeCapabilities: []*csi.VolumeCapability{nil}},
		"block capability":                  {VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			registry := &capacityQueryRegistry{}
			original := maps.Clone(request.Parameters)
			response, err := (&Service{CapacityPools: registry}).GetCapacity(context.Background(), request)
			if response != nil || status.Code(err) != codes.InvalidArgument || len(registry.calls) != 0 {
				t.Fatalf("unsupported request reached capacity observation: %+v %v calls=%v", response, err, registry.calls)
			}
			if !maps.Equal(request.Parameters, original) {
				t.Fatal("capacity query changed the rejected request parameters")
			}
		})
	}
}

func TestGetCapacityAcceptsProvisionerStorageClassParameters(t *testing.T) {
	for _, suffix := range []string{
		"fstype", "secret-name", "secret-namespace",
		"provisioner-secret-name", "provisioner-secret-namespace",
		"controller-publish-secret-name", "controller-publish-secret-namespace",
		"node-stage-secret-name", "node-stage-secret-namespace",
		"node-publish-secret-name", "node-publish-secret-namespace",
		"controller-expand-secret-name", "controller-expand-secret-namespace",
		"node-expand-secret-name", "node-expand-secret-namespace",
		"controller-modify-secret-name", "controller-modify-secret-namespace",
	} {
		t.Run(suffix, func(t *testing.T) {
			parameters := map[string]string{
				PoolGroupKey: "other", CapacityEnforcementKey: "none",
				"csi.storage.k8s.io/fstype": "ext4", "csi.storage.k8s.io/" + suffix: "${pvc.namespace}",
			}
			if suffix == "fstype" {
				parameters["csi.storage.k8s.io/fstype"] = "ext4"
			}
			original := maps.Clone(parameters)
			registry := &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{
				queryPool("default", "node-a", "default", 100, 80), queryPool("other", "node-a", "other", 100, 40),
			}}}
			assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{Parameters: parameters}, 40, 40)
			if !maps.Equal(parameters, original) {
				t.Fatal("capacity query changed the caller's StorageClass parameters")
			}
		})
	}
}

func TestGetCapacityUsesRegistryEligibilityWithoutMutations(t *testing.T) {
	for _, unavailable := range []string{"NotReady", "stale probe", "future probe", "old generation", "invalid inventory", "deleting", "missing observation"} {
		t.Run(unavailable, func(t *testing.T) {
			api, registry, _, _ := initializationFixture(t)
			for _, name := range []string{"pool-a", "pool-b"} {
				pool, err := api.Resource(volumeapi.PoolResource).Get(context.Background(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for field, value := range map[string]int64{"filesystemTotalBytes": 100, "filesystemAvailableBytes": 80, "filesystemAvailableInodes": 10} {
					if err := unstructured.SetNestedField(pool.Object, value, "status", field); err != nil {
						t.Fatal(err)
					}
				}
				if name == "pool-b" {
					switch unavailable {
					case "NotReady":
						conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
						conditions[0].(map[string]any)["status"] = "False"
						_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
					case "stale probe":
						_ = unstructured.SetNestedField(pool.Object, time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), "status", "lastProbeTime")
					case "future probe":
						_ = unstructured.SetNestedField(pool.Object, time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "status", "lastProbeTime")
					case "old generation":
						pool.SetGeneration(2)
					case "invalid inventory":
						_ = unstructured.SetNestedField(pool.Object, false, "status", "inventory", "valid")
					case "deleting":
						now := metav1.Now()
						pool.SetDeletionTimestamp(&now)
					case "missing observation":
						unstructured.RemoveNestedField(pool.Object, "status", "filesystemAvailableBytes")
					}
				}
				if err := api.Tracker().Update(volumeapi.PoolResource, pool, ""); err != nil {
					t.Fatal(err)
				}
			}
			api.ClearActions()
			assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, 80, 80)
			for _, action := range api.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("capacity query mutated Kubernetes: %v", action)
				}
			}
		})
	}
}

func TestGetCapacityWithEmptyRegistry(t *testing.T) {
	api, registry, _, _ := initializationFixture(t)
	for _, name := range []string{"pool-a", "pool-b"} {
		if err := api.Tracker().Delete(volumeapi.PoolResource, "", name); err != nil {
			t.Fatal(err)
		}
	}
	assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, 0, 0)
}

func TestGetCapacityChecksIndependentBackingAcrossGroups(t *testing.T) {
	for _, test := range []struct {
		name, secondGroup string
		secondStart       uint64
		total             int64
	}{
		{"independent same group", "default", 1024, 160},
		{"independent other group", "other", 1024, 80},
		{"overlap outside requested group", "other", 512, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, registry, _, _ := initializationFixture(t)
			for _, name := range []string{"pool-a", "pool-b"} {
				pool, err := api.Resource(volumeapi.PoolResource).Get(context.Background(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				unit := &volumeapi.PoolCapacityUnit{Device: "253:0", Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 0, Sectors: 1024}}}
				if name == "pool-b" {
					unit.Device, unit.Extents[0].Start = "253:1", test.secondStart
				}
				data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(unit)
				if err != nil {
					t.Fatal(err)
				}
				_ = unstructured.SetNestedMap(pool.Object, data, "status", "capacityUnit")
				_ = unstructured.SetNestedField(pool.Object, volumeapi.PoolCapacityPolicyFixedBlock, "spec", "capacityPolicy")
				_ = unstructured.SetNestedField(pool.Object, "worker-a", "spec", "nodeName")
				_ = unstructured.SetNestedField(pool.Object, "/mnt/"+name, "spec", "mountPath")
				_ = unstructured.SetNestedField(pool.Object, int64(100), "status", "filesystemTotalBytes")
				_ = unstructured.SetNestedField(pool.Object, int64(80), "status", "filesystemAvailableBytes")
				_ = unstructured.SetNestedField(pool.Object, int64(10), "status", "filesystemAvailableInodes")
				if name == "pool-b" {
					_ = unstructured.SetNestedField(pool.Object, test.secondGroup, "spec", "poolGroup")
				}
				conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
				conditions = append(conditions, map[string]any{"type": volumeapi.PoolConditionCapacityIndependent, "status": "True", "observedGeneration": int64(1)})
				_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
				if err := api.Tracker().Update(volumeapi.PoolResource, pool, ""); err != nil {
					t.Fatal(err)
				}
			}
			assertCapacityQuery(t, &Service{CapacityPools: registry}, &csi.GetCapacityRequest{}, test.total, min(test.total, 80))
		})
	}
}
