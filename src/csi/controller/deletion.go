package controller

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/provisioning/reclaim"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Service) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if err := volume.ValidateID(req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.validate(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	unlock := s.lifecycles.Lock(req.GetVolumeId())
	defer unlock()
	volumeState, exists, stateErr := s.deletableState(ctx, req.GetVolumeId())
	if stateErr != nil {
		return nil, stateErr
	}
	if !exists {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if volumeState.UnstartedCreation() || volumeState.CancellableCreation() || volumeState.CancellingCreation() {
		cancel := creation.Canceller{Volumes: s.Volumes, Inspector: s.CreationInspector, Cleanups: s.cleanupSettler()}
		complete, err := cancel.Advance(ctx, req.GetVolumeId(), volumeState.UID)
		if err != nil {
			return nil, deletionAdmissionError(err)
		}
		if !complete {
			return nil, status.Error(codes.Unavailable, "creation cancellation is waiting for cleanup settlement")
		}
		return &csi.DeleteVolumeResponse{}, nil
	}
	fenced, fenceErr := s.Volumes.BeginDelete(ctx, req.GetVolumeId(), volumeState.UID, *volumeState.CurrentCopy)
	if fenceErr != nil {
		return nil, deletionAdmissionError(fenceErr)
	}
	if err := s.settleVolumeCleanup(ctx, req.GetVolumeId(), fenced); err != nil {
		return nil, err
	}
	if err := s.Volumes.Delete(ctx, req.GetVolumeId(), fenced.UID); err != nil {
		return nil, kubernetesAPIError("delete volume state", err)
	}
	if err := s.Volumes.RemoveVolumeFinalizer(ctx, req.GetVolumeId(), fenced.UID); err != nil {
		return nil, kubernetesAPIError("release settled volume cleanup protection", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// deletableState reads the live volume state and reports whether a deletion
// still has work to do. An absent object is an already completed deletion.
func (s *Service) deletableState(ctx context.Context, volumeID string) (volumeapi.State, bool, error) {
	state, stateErr := s.Volumes.Get(ctx, volumeID)
	if stateErr != nil && !apierrors.IsNotFound(stateErr) {
		return volumeapi.State{}, false, kubernetesAPIError("read volume state", stateErr)
	}
	if stateErr != nil {
		return volumeapi.State{}, false, nil
	}
	if state.UnstartedCreation() {
		return state, true, nil
	}
	if state.OwnerNode == "" {
		return volumeapi.State{}, false, status.Error(codes.FailedPrecondition, "volume has no owner node")
	}
	if state.UID == "" || state.CurrentCopy == nil || state.CurrentCopy.Role != volume.RoleServing {
		return volumeapi.State{}, false, status.Error(codes.FailedPrecondition, "identified volume state is required for cleanup")
	}
	return state, true, nil
}

// settleVolumeCleanup records the cleanup intent, runs the node-local effect
// while it is still pending, and requires a fresh post-receipt absence proof
// before the caller may remove the volume's durable state.
func (s *Service) settleVolumeCleanup(ctx context.Context, volumeID string, fenced volumeapi.State) error {
	defer s.observeStep("delete_settle", volumeID)()
	complete, err := s.cleanupSettler().Advance(ctx, cleanupapi.Spec{
		OperationID: fenced.DeletionOperationID,
		Target:      *fenced.CurrentCopy,
		Reason:      volumeCleanupReason(fenced),
		Authority: cleanupapi.Authority{
			Kind: "ShiftPVVolume", Name: volumeID, UID: fenced.UID,
		},
	})
	if err != nil {
		return cleanupSettlementError(err)
	}
	if !complete {
		return status.Error(codes.Unavailable, "cleanup is waiting for execution or a fresh post-receipt Pool absence proof")
	}
	return nil
}

func (s *Service) cleanupSettler() *reclaim.Settler {
	return &reclaim.Settler{Store: s.Cleanups, Reclaim: s.CleanupOperator.Reclaim, AbsenceWait: s.CleanupAbsenceWait}
}

func cleanupSettlementError(err error) error {
	var effect *reclaim.EffectError
	if errors.As(err, &effect) {
		return directoryOperationError("reclaim identified volume copy", effect.Err)
	}
	if errors.Is(err, cleanupapi.ErrConflict) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return kubernetesAPIError("settle volume cleanup", err)
}

func volumeCleanupReason(state volumeapi.State) string {
	if state.CancellingCreation() {
		return "CreationCancelled"
	}
	return "VolumeDelete"
}

// deletionAdmissionError is the CSI retry policy for repository admission.
func deletionAdmissionError(err error) error {
	var effect *reclaim.EffectError
	if errors.As(err, &effect) || errors.Is(err, cleanupapi.ErrConflict) {
		return cleanupSettlementError(err)
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, volumeapi.ErrVolumeFinalizersPending) {
		return status.Errorf(codes.Unavailable, "wait for other Volume protections before cancellation: %v", err)
	}
	if errors.Is(err, creation.ErrInspectionUnavailable) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, creation.ErrLegacyCreation) || errors.Is(err, creation.ErrNotCancellable) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, volumeapi.ErrVolumePublished) {
		return status.Errorf(codes.Unavailable, "wait for volume unpublish before deletion: %v", err)
	}
	if errors.Is(err, volumeapi.ErrStateConflict) {
		return status.Errorf(codes.FailedPrecondition, "fence volume deletion: %v", err)
	}
	return kubernetesAPIError("fence volume deletion", err)
}
