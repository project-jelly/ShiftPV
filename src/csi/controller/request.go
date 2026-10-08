package controller

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation"
)

// CSI compatibility is separate from the registry's immutable creation identity.
// A retry may change its range or preferences without changing the stored copy.
func compatibleCreateRequest(existing volumeapi.State, request createRequest) error {
	if existing.RequestName != request.name {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists with incompatible requestName", request.name)
	}
	if existing.CapacityBytes < request.capacityRange.GetRequiredBytes() ||
		(request.capacityRange.GetLimitBytes() > 0 && existing.CapacityBytes > request.capacityRange.GetLimitBytes()) {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists outside the requested capacity range", request.name)
	}
	if !creationNodeAllowed(existing.InitialNode, request.topology) {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists outside the requested topology", request.name)
	}
	return nil
}

func creationNodeAllowed(node string, requirements *csi.TopologyRequirement) bool {
	candidates := requirements.GetRequisite()
	if len(candidates) == 0 {
		// ShiftPV's single Preferred node is the WFFC placement contract.
		candidates = requirements.GetPreferred()
	}
	for _, topology := range candidates {
		segments := topology.GetSegments()
		if len(segments) == 1 && segments[TopologyKey] == node {
			return true
		}
	}
	return false
}

// createRequest keeps CSI compatibility constraints beside the initial placement.
type createRequest struct {
	id            string
	name          string
	nodeName      string
	capacity      int64
	capacityRange *csi.CapacityRange
	topology      *csi.TopologyRequirement
}

func parseCreateRequest(req *csi.CreateVolumeRequest) (createRequest, error) {
	if req.GetName() == "" {
		return createRequest{}, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetVolumeContentSource() != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, "snapshot restore and volume cloning are not supported")
	}
	if len(req.GetMutableParameters()) != 0 {
		return createRequest{}, status.Error(codes.InvalidArgument, "mutable volume parameters are not supported")
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateParameters(req.GetParameters()); err != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	capacity, err := requestedCapacity(req.GetCapacityRange())
	if err != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	nodeName, err := selectedNode(req.GetAccessibilityRequirements())
	if err != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	id, err := volume.IDFromName(req.GetName())
	if err != nil {
		return createRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return createRequest{id: id, name: req.GetName(), nodeName: nodeName, capacity: capacity,
		capacityRange: req.GetCapacityRange(), topology: req.GetAccessibilityRequirements()}, nil
}

func requestedCapacity(capacityRange *csi.CapacityRange) (int64, error) {
	if capacityRange == nil {
		return 0, fmt.Errorf("capacity range is required")
	}
	required := capacityRange.GetRequiredBytes()
	limit := capacityRange.GetLimitBytes()
	if required < 0 || limit < 0 {
		return 0, fmt.Errorf("capacity bounds must not be negative")
	}
	if required == 0 {
		if limit == 0 {
			return 0, fmt.Errorf("at least one positive capacity bound is required")
		}
		return limit, nil
	}
	if limit > 0 && required > limit {
		return 0, fmt.Errorf("required capacity exceeds the limit")
	}
	return required, nil
}

func validateParameters(parameters map[string]string) error {
	for key, value := range parameters {
		switch key {
		case PVCNameKey, PVCNamespaceKey, PVNameKey:
			// Added by csi-provisioner --extra-create-metadata, not by the StorageClass.
		case CapacityEnforcementKey:
			// Kept as a no-op because StorageClass parameters are immutable.
			if value != capacityEnforcementNone {
				return fmt.Errorf("unsupported StorageClass parameter %q value %q", key, value)
			}
		case PoolGroupKey:
			if len(validation.IsDNS1123Label(value)) != 0 {
				return fmt.Errorf("invalid StorageClass pool group %q", value)
			}
		default:
			return fmt.Errorf("unsupported StorageClass parameter %q", key)
		}
	}
	return nil
}

func validateCapabilities(capabilities []*csi.VolumeCapability) error {
	if len(capabilities) == 0 {
		return fmt.Errorf("at least one volume capability is required")
	}
	for _, capability := range capabilities {
		if capability.GetMount() == nil {
			return fmt.Errorf("only filesystem volumes are supported")
		}
		if capability.GetAccessMode() == nil || capability.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return fmt.Errorf("only SINGLE_NODE_WRITER is supported")
		}
	}
	return nil
}

func selectedNode(requirements *csi.TopologyRequirement) (string, error) {
	if requirements == nil {
		return "", fmt.Errorf("selected topology is required; use WaitForFirstConsumer")
	}
	for _, candidates := range [][]*csi.Topology{requirements.GetPreferred(), requirements.GetRequisite()} {
		for _, topology := range candidates {
			segments := topology.GetSegments()
			if len(segments) != 1 || segments[TopologyKey] == "" {
				return "", fmt.Errorf("topology must contain only %q with a node value", TopologyKey)
			}
		}
	}
	for _, topology := range requirements.GetPreferred() {
		if !creationNodeAllowed(topology.GetSegments()[TopologyKey], requirements) {
			return "", fmt.Errorf("preferred topology must be included in requisite topology")
		}
	}
	for _, candidates := range [][]*csi.Topology{requirements.GetPreferred(), requirements.GetRequisite()} {
		for _, topology := range candidates {
			if node := topology.GetSegments()[TopologyKey]; node != "" {
				return node, nil
			}
		}
	}
	return "", fmt.Errorf("selected topology does not contain %q", TopologyKey)
}
