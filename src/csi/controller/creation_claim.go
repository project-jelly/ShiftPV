package controller

import (
	"context"
	"errors"
	"strings"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) newCreationRequest(ctx context.Context, request createRequest, parameters map[string]string) (volumeapi.CreationRequest, error) {
	intent := volumeapi.CreationRequest{VolumeID: request.id, RequestName: request.name, NodeName: request.nodeName, CapacityBytes: request.capacity}
	// Extra metadata is optional CSI input. Standalone/opaque requests keep
	// unknown provenance and cannot later authorize automatic claim retirement.
	if parameters[PVCNamespaceKey] == "" || parameters[PVCNameKey] == "" || !strings.HasPrefix(request.name, "pvc-") {
		return intent, nil
	}
	if pv := parameters[PVNameKey]; pv != "" && pv != request.name {
		return volumeapi.CreationRequest{}, status.Error(codes.FailedPrecondition, "PV metadata differs from the creation request")
	}
	claim, err := s.inspectCreationClaim(ctx, creation.ClaimRequest{Namespace: parameters[PVCNamespaceKey], Name: parameters[PVCNameKey], RequestName: request.name, NodeName: request.nodeName})
	intent.Claim = claim
	return intent, err
}

func (s *Service) validateCreationClaim(ctx context.Context, existing volumeapi.State) error {
	if existing.CreationClaim == nil {
		return nil
	}
	claim := existing.CreationClaim
	observed, err := s.inspectCreationClaim(ctx, creation.ClaimRequest{Namespace: claim.Namespace, Name: claim.Name, RequestName: existing.RequestName, NodeName: existing.InitialNode})
	if err != nil {
		return err
	}
	if observed == nil || *observed != *claim {
		return status.Error(codes.FailedPrecondition, "recorded original creation claim changed")
	}
	return nil
}

func (s *Service) inspectCreationClaim(ctx context.Context, request creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
	if s.CreationClaims == nil {
		return nil, status.Error(codes.Unavailable, "creation claim inspection is not configured")
	}
	claim, err := s.CreationClaims.Inspect(ctx, request)
	if errors.Is(err, creation.ErrClaimIdentity) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, kubernetesAPIError("verify original creation claim", err)
		}
		// A previous native approval may still be running. Unknown observation
		// must retain the provisioner's in-progress classification.
		return nil, status.Errorf(codes.Unavailable, "verify original creation claim: %v", err)
	}
	if claim == nil || !claim.Valid() {
		return nil, status.Error(codes.Unavailable, "original creation claim identity is unproven")
	}
	if claim.Namespace != request.Namespace || claim.Name != request.Name || request.RequestName != "pvc-"+claim.UID {
		return nil, status.Error(codes.FailedPrecondition, "verified creation claim differs from the requested original")
	}
	return claim, nil
}
