package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

func preparedCapacityRegistry() *capacityQueryRegistry {
	source := &volume.CopyIdentity{InstallationID: "installation", PoolName: "source", PoolUID: "source", NodeName: "node-a",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "source-copy", Role: volume.RoleServing}
	move := volumeapi.Move{Name: "move", UID: "move-uid", Spec: volumeapi.MoveSpec{
		ProtocolVersion: volumeapi.MoveProtocolPreparedV1, VolumeID: source.VolumeID, SourceNode: source.NodeName},
		Status: volumeapi.MoveStatus{Phase: "Pending", SourceCopy: source,
			PersistentVolumeName: "pv", PersistentVolumeUID: "pv-uid", ClaimNamespace: "app", ClaimName: "claim", ClaimUID: "claim-uid",
			ConsumerName: "consumer", ConsumerUID: "consumer-uid", PlacementPodUID: "placement-uid",
			Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: "destination", UID: "destination"}, NodeName: "node-b", ReservedBytes: 30},
		}}
	return &capacityQueryRegistry{snapshot: volumeapi.PoolSnapshot{Ready: []volumeapi.Pool{
		queryPool("source", "node-a", "default", 100, 100), queryPool("destination", "node-b", "default", 100, 100),
		queryPool("other", "node-b", "other", 100, 100),
	}}, moves: []volumeapi.Move{move}, volumes: map[string]volumeapi.State{source.VolumeID: {
		UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: source.NodeName, CurrentCopy: source, CapacityBytes: 30, ActiveMove: move.Name,
	}}}
}

func TestGetCapacityAndCreationSharePreparationReservation(t *testing.T) {
	registry := preparedCapacityRegistry()
	service := &Service{CapacityPools: registry}
	for _, approved := range []bool{false, true} {
		if approved {
			move := &registry.moves[0]
			move.Status.Preparation, move.Status.CapacityApproved = nil, true
			move.Status.DestinationNode, move.Status.DestinationPoolUID = "node-b", "destination"
		}
		assertCapacityQuery(t, service, &csi.GetCapacityRequest{}, 140, 70)
		assertCapacityQuery(t, service, &csi.GetCapacityRequest{
			AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyKey: "node-b"}},
		}, 70, 70)
		assertCapacityQuery(t, service, &csi.GetCapacityRequest{Parameters: map[string]string{PoolGroupKey: "other"}}, 100, 100)
		reserved, err := service.poolReservedBytes(context.Background(), "destination", "new-volume")
		if err != nil || reserved != 30 {
			t.Fatalf("CreateVolume admission ledger differs: approved=%t reserved=%d err=%v", approved, reserved, err)
		}
	}
}

func TestCreateVolumeCannotSpendPreparedMoveCapacity(t *testing.T) {
	query := preparedCapacityRegistry()
	move := &query.moves[0]
	move.Status.Preparation.ReservedBytes = 30 << 20
	state := query.volumes[move.Spec.VolumeID]
	state.CapacityBytes = move.Status.Preparation.ReservedBytes
	query.volumes[move.Spec.VolumeID] = state
	registry := &fakePoolCapacityRegistry{volumes: query.volumes, moves: query.moves,
		pool: volumeapi.Pool{Name: "destination", UID: "destination", NodeName: "node-b", MountPath: "/pool", CapacityLimit: "64Mi"}}
	operator := &fakeDirectoryOperator{}
	service := configuredService(&Service{Client: fake.NewClientset(), Namespace: "shiftpv-system", Operator: operator,
		CapacityPools: registry, CapacityProbe: &fakePoolCapacityProbe{stats: poolcapacity.Filesystem{AvailableBytes: 1 << 30}}})
	response, err := service.CreateVolume(context.Background(), validCreateRequest("node-b"))
	if response != nil || status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "reservation limit exceeded") || operator.createCalls != 0 {
		t.Fatalf("CreateVolume spent the preparation reservation: response=%+v err=%v creates=%d", response, err, operator.createCalls)
	}
}

func TestGetCapacityAndCreationFailClosedWhenPreparationLosesOwner(t *testing.T) {
	registry := preparedCapacityRegistry()
	volumeID := registry.moves[0].Spec.VolumeID
	state := registry.volumes[volumeID]
	state.ActiveMove = ""
	registry.volumes[volumeID] = state
	service := &Service{CapacityPools: registry}
	response, err := service.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("scheduler received capacity from an orphan hold: response=%+v err=%v", response, err)
	}
	if _, err := service.poolReservedBytes(context.Background(), "destination", "new-volume"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateVolume accepted an orphan preparation: %v", err)
	}
}
