package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	TopologyKey             = volume.TopologyKey
	NodeContextKey          = "shiftpv.io/node"
	CapacityEnforcementKey  = "shiftpv.io/capacity-enforcement"
	PoolGroupKey            = "shiftpv.io/pool-group"
	PVCNameKey              = "csi.storage.k8s.io/pvc/name"
	PVCNamespaceKey         = "csi.storage.k8s.io/pvc/namespace"
	PVNameKey               = "csi.storage.k8s.io/pv/name"
	MobilityAdmissionLabel  = "shiftpv.io/admission"
	capacityEnforcementNone = "none"
	mobilityEnabledValue    = "enabled"
)

type DirectoryOperator interface {
	CreateCopy(context.Context, volume.CopyIdentity) error
	FinalizeCreate(context.Context, volume.CopyIdentity) error
}

type VolumeRegistry interface {
	Get(context.Context, string) (volumeapi.State, error)
	Delete(context.Context, string, string) error
	RemoveVolumeFinalizer(context.Context, string, string) error
	PoolNodes(context.Context) ([]string, error)
	PoolNodesForGroup(context.Context, string) ([]string, error)
	BeginCreate(context.Context, string, string, string, int64) (volumeapi.State, error)
	BeginCreateInPool(context.Context, string, string, string, int64, string, string) (volumeapi.State, error)
	CompleteCreate(context.Context, string, string, volume.CopyIdentity) error
	BeginDelete(context.Context, string, string, volume.CopyIdentity) (volumeapi.State, error)
}

type ProvisioningGate interface {
	Enter() (func(), error)
}

type cleanupOperator interface {
	Reclaim(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, error)
}

type Service struct {
	csi.UnimplementedControllerServer
	Client                  kubernetes.Interface
	Namespace               string
	Operator                DirectoryOperator
	Volumes                 VolumeRegistry
	ConsumerPlacement       consumer.Inspection
	CapacityPools           PoolCapacityRegistry
	CapacityProbe           PoolCapacityProbe
	PoolLocks               *poolcapacity.Locker
	PoolReadinessStaleAfter time.Duration
	CleanupAbsenceWait      time.Duration
	ObserveStep             func(string, time.Duration)
	RetryRequests           interface {
		Register(namespace, name, uid, node, group string, requested int64)
	}
	ProvisioningGate ProvisioningGate
	Cleanups         *cleanupapi.Store
	CleanupOperator  cleanupOperator
	lifecycles       poolcapacity.Locker
	poolLifecycles   poolcapacity.Locker
}

func (s *Service) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	request, err := parseCreateRequest(req)
	if err != nil {
		return nil, err
	}
	if err := s.validate(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if s.ProvisioningGate != nil {
		leave, err := s.ProvisioningGate.Enter()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		defer leave()
	}
	unlock := s.lifecycles.Lock(request.id)
	defer unlock()
	if err := s.ensureNoCleanupFence(ctx, request.id); err != nil {
		return nil, err
	}

	state, beginErr := s.beginCreate(ctx, request.id, request.name, request.nodeName, request.capacity, req.GetParameters())
	if beginErr != nil {
		if errors.Is(beginErr, volumeapi.ErrPoolCopyConflict) {
			return nil, status.Error(codes.FailedPrecondition, beginErr.Error())
		}
		if _, ok := status.FromError(beginErr); ok {
			return nil, beginErr
		}
		return nil, kubernetesAPIError("record volume creation intent", beginErr)
	}
	if err := s.createServingCopy(ctx, request.id, state); err != nil {
		return nil, err
	}
	poolNodes, poolErr := s.Volumes.PoolNodesForGroup(ctx, requestedPoolGroup(req.GetParameters()))
	if poolErr != nil {
		return nil, kubernetesAPIError("list volume topology", poolErr)
	}
	if !slices.Contains(poolNodes, request.nodeName) {
		return nil, status.Errorf(codes.FailedPrecondition, "selected node %q has no registered ShiftPVPool", request.nodeName)
	}
	accessibleNodes, err := s.accessibleNodes(ctx, req.GetParameters(), request.nodeName, poolNodes)
	if err != nil {
		return nil, err
	}

	return volumeResponse(request.id, request.nodeName, accessibleNodes, request.capacity), nil
}

// createRequest is the exact volume identity a validated CSI request names.
type createRequest struct {
	id       string
	name     string
	nodeName string
	capacity int64
}

func parseCreateRequest(req *csi.CreateVolumeRequest) (createRequest, error) {
	if req.GetName() == "" {
		return createRequest{}, status.Error(codes.InvalidArgument, "name is required")
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
	return createRequest{id: id, name: req.GetName(), nodeName: nodeName, capacity: capacity}, nil
}

// createServingCopy performs the node-local create effect and records its
// completion, in the order a crashed create must be able to resume from.
func (s *Service) createServingCopy(ctx context.Context, id string, state volumeapi.State) error {
	defer s.observeStep("create_effect", id)()
	if state.CurrentCopy == nil {
		return status.Error(codes.FailedPrecondition, "volume creation has no copy identity")
	}
	if createErr := s.Operator.CreateCopy(ctx, *state.CurrentCopy); createErr != nil {
		return directoryOperationError("prepare identified volume directory", createErr)
	}
	if completeErr := s.Volumes.CompleteCreate(ctx, id, state.UID, *state.CurrentCopy); completeErr != nil {
		return kubernetesAPIError("complete volume creation", completeErr)
	}
	if finalizeErr := s.Operator.FinalizeCreate(ctx, *state.CurrentCopy); finalizeErr != nil {
		return directoryOperationError("settle volume creation helper", finalizeErr)
	}
	return nil
}

func (s *Service) ensureNoCleanupFence(ctx context.Context, volumeID string) error {
	cleanups, err := s.Cleanups.ListForVolume(ctx, volumeID)
	if err != nil {
		return kubernetesAPIError("list volume cleanup fences", err)
	}
	for _, cleanup := range cleanups {
		if cleanup.Spec.Target.VolumeID == volumeID && cleanup.Status.Phase != cleanupapi.PhaseCompleted {
			return status.Errorf(codes.FailedPrecondition, "volume %q has unresolved cleanup %q in phase %q", volumeID, cleanup.Name, cleanup.Status.Phase)
		}
	}
	return nil
}

func (s *Service) accessibleNodes(ctx context.Context, parameters map[string]string, owner string, poolNodes []string) ([]string, error) {
	namespaceName := parameters[PVCNamespaceKey]
	if namespaceName == "" {
		return []string{owner}, nil
	}
	namespace, err := s.Client.CoreV1().Namespaces().Get(ctx, namespaceName, metav1.GetOptions{})
	if err != nil {
		return nil, kubernetesAPIError("read PVC namespace for mobility topology", err)
	}
	if namespace.Labels[MobilityAdmissionLabel] != mobilityEnabledValue {
		return []string{owner}, nil
	}
	return poolNodes, nil
}

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
	fenced, fenceErr := s.Volumes.BeginDelete(ctx, req.GetVolumeId(), volumeState.UID, *volumeState.CurrentCopy)
	if fenceErr != nil {
		if errors.Is(fenceErr, volumeapi.ErrStateConflict) {
			return nil, status.Errorf(codes.FailedPrecondition, "fence volume deletion: %v", fenceErr)
		}
		return nil, kubernetesAPIError("fence volume deletion", fenceErr)
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
		Reason:      "VolumeDelete",
		Authority: cleanupapi.Authority{
			Kind: "ShiftPVVolume", Name: volumeID, UID: fenced.UID,
		},
	})
	if ensureErr != nil {
		return kubernetesAPIError("record volume cleanup intent", ensureErr)
	}
	if cleanup.Status.Phase == cleanupapi.PhasePending || cleanup.Status.Phase == cleanupapi.PhaseRunning {
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

func (s *Service) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: []*csi.ControllerServiceCapability{{
		Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME}},
	}}}, nil
}

func (s *Service) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if err := volume.ValidateID(req.GetVolumeId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: req.GetVolumeCapabilities(),
		Parameters:         req.GetParameters(),
		VolumeContext:      req.GetVolumeContext(),
	}}, nil
}

func (s *Service) beginCreate(ctx context.Context, id, requestName, nodeName string, capacity int64, parameters map[string]string) (volumeapi.State, error) {
	if s.CapacityPools != nil || s.CapacityProbe != nil {
		if s.CapacityPools == nil || s.CapacityProbe == nil {
			return volumeapi.State{}, status.Error(codes.Internal, "Pool capacity admission is incompletely configured")
		}
		return s.beginCreateWithinPool(ctx, id, requestName, nodeName, capacity, parameters)
	}
	if requestedPoolGroup(parameters) != volumeapi.DefaultPoolGroup {
		return volumeapi.State{}, status.Error(codes.FailedPrecondition, "Pool group selection requires Pool capacity admission")
	}
	return s.Volumes.BeginCreate(ctx, id, requestName, nodeName, capacity)
}

func (s *Service) validate() error {
	if s == nil || s.Client == nil || s.Namespace == "" || s.Operator == nil || s.Volumes == nil || s.Cleanups == nil || s.CleanupOperator == nil {
		return fmt.Errorf("controller exact lifecycle is not configured")
	}
	return nil
}

func kubernetesAPIError(operation string, err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsTooManyRequests(err), apierrors.IsServiceUnavailable(err):
		code = codes.Unavailable
	}
	return status.Errorf(code, "%s: %v", operation, err)
}

func directoryOperationError(operation string, err error) error {
	code := codes.Internal
	var retryable interface{ Retryable() bool }
	if errors.As(err, &retryable) && retryable.Retryable() {
		code = codes.Unavailable
	}
	return status.Errorf(code, "%s: %v", operation, err)
}

func volumeResponse(id, nodeName string, poolNodes []string, capacity int64) *csi.CreateVolumeResponse {
	topologies := make([]*csi.Topology, 0, len(poolNodes))
	for _, poolNode := range poolNodes {
		topologies = append(topologies, &csi.Topology{Segments: map[string]string{TopologyKey: poolNode}})
	}
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      id,
		CapacityBytes: capacity,
		VolumeContext: map[string]string{
			NodeContextKey: nodeName,
		},
		AccessibleTopology: topologies,
	}}
}

func requestedCapacity(capacityRange *csi.CapacityRange) (int64, error) {
	if capacityRange == nil {
		return 0, fmt.Errorf("capacity range is required")
	}
	required := capacityRange.GetRequiredBytes()
	limit := capacityRange.GetLimitBytes()
	if required <= 0 {
		return 0, fmt.Errorf("required capacity must be greater than zero")
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
			if node := topology.GetSegments()[TopologyKey]; node != "" {
				return node, nil
			}
		}
	}
	return "", fmt.Errorf("selected topology does not contain %q", TopologyKey)
}

func (s *Service) observeStep(step, volumeID string) func() {
	started := time.Now()
	return func() {
		elapsed := time.Since(started)
		klog.V(2).InfoS("Provisioning step completed", "step", step, "volumeID", volumeID, "duration", elapsed)
		if s.ObserveStep != nil {
			s.ObserveStep(step, elapsed)
		}
	}
}
