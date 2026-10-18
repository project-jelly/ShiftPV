package controller

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
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
	if volumeState.UnstartedCreation() {
		if err := s.Volumes.CancelUnstartedCreation(ctx, req.GetVolumeId(), volumeState.UID); err != nil {
			return nil, deletionAdmissionError(err)
		}
		return &csi.DeleteVolumeResponse{}, nil
	}
	fenced, fenceErr := s.fenceDeletion(ctx, req.GetVolumeId(), volumeState)
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

func (s *Service) fenceDeletion(ctx context.Context, id string, state volumeapi.State) (volumeapi.State, error) {
	if !state.CancellableCreation() && !state.CancellingCreation() {
		return s.Volumes.BeginDelete(ctx, id, state.UID, *state.CurrentCopy)
	}
	if s.CreationInspector == nil {
		return volumeapi.State{}, status.Error(codes.Unavailable, "creation executor inspection is unavailable")
	}
	exists, err := s.CreationInspector.CreationExists(ctx, *state.CurrentCopy)
	if err != nil {
		return volumeapi.State{}, kubernetesAPIError("inspect creation responsibility before cancellation", err)
	}
	if exists {
		return volumeapi.State{}, status.Error(codes.FailedPrecondition, "legacy creation helper responsibility must settle before cancellation")
	}
	return s.Volumes.BeginCreationCancellation(ctx, id, state.UID, *state.CurrentCopy)
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
	cleanup, ensureErr := s.Cleanups.Ensure(ctx, cleanupapi.Spec{
		OperationID: fenced.DeletionOperationID,
		Target:      *fenced.CurrentCopy,
		Reason:      volumeCleanupReason(fenced),
		Authority: cleanupapi.Authority{
			Kind: "ShiftPVVolume", Name: volumeID, UID: fenced.UID,
		},
	})
	if ensureErr != nil {
		return kubernetesAPIError("record volume cleanup intent", ensureErr)
	}
	jobReceipt := cleanup.Status.Executor != nil && cleanup.Status.Executor.Kind == "" &&
		(cleanup.Status.Phase == cleanupapi.PhaseVerifying || cleanup.Status.Phase == cleanupapi.PhaseConfirmingAbsence || cleanup.Status.Phase == cleanupapi.PhaseCompleted)
	if jobReceipt || cleanup.Status.Phase == cleanupapi.PhasePending || cleanup.Status.Phase == cleanupapi.PhaseRunning {
		cleanup, ensureErr = s.CleanupOperator.Reclaim(ctx, cleanup, s.Cleanups)
		if ensureErr != nil {
			return directoryOperationError("reclaim identified volume copy", ensureErr)
		}
	}
	if cleanup.Status.Phase == cleanupapi.PhaseNeedsReview {
		return status.Errorf(codes.FailedPrecondition, "cleanup %q requires review: %s", cleanup.Name, cleanup.Status.Message)
	}
	if cleanup.Status.Phase != cleanupapi.PhaseVerifying && cleanup.Status.Phase != cleanupapi.PhaseConfirmingAbsence && cleanup.Status.Phase != cleanupapi.PhaseCompleted {
		return status.Errorf(codes.FailedPrecondition, "cleanup is phase=%q", cleanup.Status.Phase)
	}
	cleanup, settled, settleErr := s.Cleanups.ReconcileAbsence(ctx, cleanup)
	if settleErr == nil && !settled {
		cleanup, settled, settleErr = s.Cleanups.WaitForAbsence(ctx, cleanup, s.CleanupAbsenceWait)
	}
	if settleErr != nil {
		if errors.Is(settleErr, cleanupapi.ErrConflict) {
			return status.Errorf(codes.FailedPrecondition, "verify exact cleanup absence: %v", settleErr)
		}
		return kubernetesAPIError("verify exact cleanup absence", settleErr)
	}
	if !settled || cleanup.Status.Phase != cleanupapi.PhaseCompleted {
		return status.Errorf(codes.Unavailable, "cleanup %q is waiting for a fresh post-receipt Pool absence proof", cleanup.Name)
	}
	return nil
}

func volumeCleanupReason(state volumeapi.State) string {
	if state.CancellingCreation() {
		return "CreationCancelled"
	}
	return "VolumeDelete"
}

// deletionAdmissionError is the CSI retry policy for repository admission.
func deletionAdmissionError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, volumeapi.ErrVolumeFinalizersPending) {
		return status.Errorf(codes.Unavailable, "wait for other Volume protections before cancellation: %v", err)
	}
	if errors.Is(err, volumeapi.ErrVolumePublished) {
		return status.Errorf(codes.Unavailable, "wait for volume unpublish before deletion: %v", err)
	}
	if errors.Is(err, volumeapi.ErrStateConflict) {
		return status.Errorf(codes.FailedPrecondition, "fence volume deletion: %v", err)
	}
	return kubernetesAPIError("fence volume deletion", err)
}
