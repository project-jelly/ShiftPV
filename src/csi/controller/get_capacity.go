package controller

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// GetCapacity reports an advisory snapshot. Creation still measures live space
// and records its reservation under the Pool UID lock before executing effects.
func (s *Service) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	group, node, err := capacityQueryScope(req)
	if err != nil {
		return nil, err
	}
	if s == nil || s.CapacityPools == nil {
		return nil, status.Error(codes.Internal, "Pool capacity observation is not configured")
	}
	snapshot, err := s.CapacityPools.ObservePools(ctx)
	if err != nil {
		return nil, kubernetesAPIError("observe capacity Pools", err)
	}
	response := &csi.GetCapacityResponse{MaximumVolumeSize: wrapperspb.Int64(0)}
	var candidates []volumeapi.Pool
	for _, pool := range snapshot.Ready {
		if poolGroup(pool) == group && (node == "" || pool.NodeName == node) {
			candidates = append(candidates, pool)
		}
	}
	if len(candidates) == 0 {
		return response, nil
	}
	volumes, err := s.CapacityPools.ListVolumes(ctx)
	if err != nil {
		return nil, kubernetesAPIError("list capacity volume owners", err)
	}
	moves, err := s.CapacityPools.ListMoves(ctx)
	if err != nil {
		return nil, kubernetesAPIError("list capacity Move holds", err)
	}
	for _, pool := range candidates {
		reserved, err := poolcapacity.ReservedBytesForPool(volumes, moves, pool.UID)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "capacity ownership observation is inconsistent: %v", err)
		}
		available := observedPoolHeadroom(pool, reserved)
		if available > math.MaxInt64-response.AvailableCapacity {
			response.AvailableCapacity = math.MaxInt64
		} else {
			response.AvailableCapacity += available
		}
		response.MaximumVolumeSize.Value = max(response.MaximumVolumeSize.Value, available)
	}
	return response, nil
}

func capacityQueryScope(req *csi.GetCapacityRequest) (group, node string, err error) {
	parameters, err := capacityQueryParameters(req.GetParameters())
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	if len(req.GetVolumeCapabilities()) != 0 {
		if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
			return "", "", status.Error(codes.InvalidArgument, err.Error())
		}
	}
	segments := req.GetAccessibleTopology().GetSegments()
	if len(segments) != 0 && (len(segments) != 1 || segments[TopologyKey] == "") {
		return "", "", status.Errorf(codes.InvalidArgument, "topology must contain only %q with a node value", TopologyKey)
	}
	return requestedPoolGroup(parameters), segments[TopologyKey], nil
}

// GetCapacity receives raw StorageClass parameters. Match external-provisioner
// v6.3.0 removePrefixedParameters before applying the same rules as CreateVolume.
func capacityQueryParameters(parameters map[string]string) (map[string]string, error) {
	pluginParameters := make(map[string]string, len(parameters))
	for key, value := range parameters {
		if suffix, reserved := strings.CutPrefix(key, "csi.storage.k8s.io/"); reserved {
			switch suffix {
			case "fstype", "secret-name", "secret-namespace",
				"provisioner-secret-name", "provisioner-secret-namespace",
				"controller-publish-secret-name", "controller-publish-secret-namespace",
				"node-stage-secret-name", "node-stage-secret-namespace",
				"node-publish-secret-name", "node-publish-secret-namespace",
				"controller-expand-secret-name", "controller-expand-secret-namespace",
				"node-expand-secret-name", "node-expand-secret-namespace",
				"controller-modify-secret-name", "controller-modify-secret-namespace":
				continue
			default:
				return nil, fmt.Errorf("unsupported reserved StorageClass parameter %q", key)
			}
		}
		pluginParameters[key] = value
	}
	return pluginParameters, validateParameters(pluginParameters)
}

func observedPoolHeadroom(pool volumeapi.Pool, reserved int64) int64 {
	limit, err := poolcapacity.LimitBytes(pool)
	if err != nil || reserved < 0 || reserved >= limit {
		return 0
	}
	available, inodes := pool.Status.FilesystemAvailableBytes, pool.Status.FilesystemAvailableInodes
	if available == nil || inodes == nil || pool.Status.FilesystemTotalBytes <= 0 ||
		*available < 0 || *available > pool.Status.FilesystemTotalBytes || *inodes <= 0 {
		return 0
	}
	return min(limit-reserved, *available)
}
