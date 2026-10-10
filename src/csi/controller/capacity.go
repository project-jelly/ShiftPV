package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
)

const selectedNodeAnnotation = consumer.SelectedNode

type PoolCapacityRegistry interface {
	ReadyPools(context.Context) ([]volumeapi.Pool, error)
	PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
}

type PoolCapacityProbe interface {
	StatFSForPool(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error)
}

func (s *Service) beginCreateWithinPool(ctx context.Context, request createRequest, parameters map[string]string) (volumeapi.State, error) {
	id, requestName, nodeName, requestedBytes := request.id, request.name, request.nodeName, request.capacity
	defer s.observeStep("capacity_admission", id)()
	existing, err := s.readCreationIntent(ctx, id)
	if err == nil {
		return s.resumeExistingCreate(ctx, existing, request, parameters)
	}
	if !apierrors.IsNotFound(err) {
		return volumeapi.State{}, kubernetesAPIError("read volume creation intent", err)
	}

	return s.placeNewCreate(ctx, id, requestName, nodeName, requestedBytes, parameters)
}

func (s *Service) readCreationIntent(ctx context.Context, id string) (volumeapi.State, error) {
	defer s.observeStep("create_intent_read", id)()
	return s.Volumes.Get(ctx, id)
}

func requestedPoolGroup(parameters map[string]string) string {
	if group := parameters[PoolGroupKey]; group != "" {
		return group
	}
	return volumeapi.DefaultPoolGroup
}

func poolGroup(pool volumeapi.Pool) string {
	if pool.PoolGroup != "" {
		return pool.PoolGroup
	}
	return volumeapi.DefaultPoolGroup
}

func (s *Service) resumeExistingCreate(ctx context.Context, existing volumeapi.State, request createRequest, parameters map[string]string) (volumeapi.State, error) {
	defer s.observeStep("create_resume", request.id)()
	if err := s.validateExistingCreate(ctx, existing, request, parameters); err != nil {
		return volumeapi.State{}, err
	}
	if existing.CurrentCopy == nil {
		return volumeapi.State{}, status.Error(codes.FailedPrecondition, "existing volume has no Pool copy identity")
	}
	copy := existing.CurrentCopy
	pool, err := s.CapacityPools.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return volumeapi.State{}, poolSelectionError(err)
	}
	if poolGroup(pool) != requestedPoolGroup(parameters) {
		return volumeapi.State{}, status.Errorf(codes.AlreadyExists, "volume %q belongs to Pool group %q", request.name, poolGroup(pool))
	}
	// Keep the registry's exact identity checks and the original capacity hold.
	return s.Volumes.BeginCreateInPool(ctx, request.id, existing.RequestName, existing.InitialNode, existing.CapacityBytes, pool.Name, pool.UID)
}

func poolSelectionError(err error) error {
	if errors.Is(err, volumeapi.ErrPoolConfiguration) || errors.Is(err, volumeapi.ErrPoolNotFound) || errors.Is(err, volumeapi.ErrPoolNotReady) {
		return status.Errorf(codes.FailedPrecondition, "read selected Pool: %v", err)
	}
	return kubernetesAPIError("read selected Pool", err)
}

func (s *Service) placeNewCreate(ctx context.Context, id, requestName, nodeName string, requestedBytes int64, parameters map[string]string) (volumeapi.State, error) {
	poolsDone := s.observeStep("create_pool_list", id)
	pools, err := s.CapacityPools.ReadyPools(ctx)
	poolsDone()
	if err != nil {
		return volumeapi.State{}, poolSelectionError(err)
	}
	group := requestedPoolGroup(parameters)
	for _, pool := range pools {
		if pool.NodeName == nodeName && poolGroup(pool) == group && volumeapi.PoolHasServingVolume(pool, id) {
			return volumeapi.State{}, status.Errorf(codes.FailedPrecondition, "Pool %q already contains a serving copy for volume %q; wait for orphan cleanup", pool.Name, id)
		}
	}
	var denied error
	found := false
	for _, pool := range pools {
		if pool.NodeName != nodeName || poolGroup(pool) != group {
			continue
		}
		found = true
		created, fit, err := s.attemptCreateInPool(ctx, pool, id, requestName, requestedBytes, parameters)
		if err != nil {
			return volumeapi.State{}, err
		}
		if fit.allowed {
			return created, nil
		}
		if denied == nil || status.Code(fit.denial) == codes.Unavailable {
			denied = fit.denial
		}
	}
	if !found {
		return volumeapi.State{}, status.Errorf(codes.FailedPrecondition, "node %q has no Ready Pool in group %q", nodeName, group)
	}
	return volumeapi.State{}, denied
}

// Admission and its durable intent share the Pool UID lock used by mobility
// holds and deregistration. Registration already rejects overlapping Pools.
func (s *Service) attemptCreateInPool(ctx context.Context, pool volumeapi.Pool, id, requestName string, requestedBytes int64, parameters map[string]string) (volumeapi.State, poolFit, error) {
	locks := s.PoolLocks
	if locks == nil {
		locks = &s.poolLifecycles
	}
	unlock := s.observeLock("create_pool_lock_wait", id, locks, pool.UID)
	defer unlock()
	fit, err := s.poolFits(ctx, pool, id, requestName, requestedBytes, parameters)
	if err != nil || !fit.allowed {
		return volumeapi.State{}, fit, err
	}
	intentDone := s.observeStep("create_intent_record", id)
	created, err := s.Volumes.BeginCreateInPool(ctx, id, requestName, pool.NodeName, requestedBytes, pool.Name, pool.UID)
	intentDone()
	return created, fit, err
}

type poolFit struct {
	allowed bool
	denial  error
}

func (s *Service) poolFits(ctx context.Context, pool volumeapi.Pool, id, requestName string, requestedBytes int64, parameters map[string]string) (poolFit, error) {
	limitBytes, err := poolLimitBytes(pool)
	if err != nil {
		return poolFit{denial: status.Errorf(codes.FailedPrecondition, "Pool %q capacity limit is invalid: %v", pool.Name, err)}, nil
	}
	reservedBytes, err := s.poolReservedBytes(ctx, pool.UID, id)
	if err != nil {
		return poolFit{}, err
	}
	if !poolcapacity.FitsReservation(requestedBytes, reservedBytes, limitBytes) {
		return poolFit{denial: s.capacityDenied(ctx, parameters, id, requestName, pool, requestedBytes, limitBytes, nil,
			"Pool %q reservation limit exceeded: requested=%d reserved=%d limit=%d",
			pool.Name, requestedBytes, reservedBytes, limitBytes)}, nil
	}

	stats, err := s.inspectCapacity(ctx, pool, id)
	if err != nil {
		return poolFit{denial: capacityProbeError("inspect Pool filesystem capacity", err)}, nil
	}
	if requestedBytes > stats.AvailableBytes {
		return poolFit{denial: s.capacityDenied(ctx, parameters, id, requestName, pool, requestedBytes, limitBytes, &stats,
			"Pool %q filesystem space is insufficient: requested=%d available=%d",
			pool.Name, requestedBytes, stats.AvailableBytes)}, nil
	}
	return poolFit{allowed: true}, nil
}

// Capacity size and placement ownership are independent. Only a scheduler-managed
// claim may release its selected node; fixed or unknown placement must retry.
func (s *Service) capacityDenied(ctx context.Context, parameters map[string]string, id, requestName string, pool volumeapi.Pool, requestedBytes, limitBytes int64, stats *poolcapacity.Filesystem, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	if s.ConsumerPlacement == nil {
		return status.Error(codes.Unavailable, "consumer placement inspection is not configured")
	}
	placement, err := s.ConsumerPlacement.Inspect(ctx, consumer.Request{Namespace: parameters[PVCNamespaceKey], Name: parameters[PVCNameKey], UID: strings.TrimPrefix(requestName, "pvc-"), Node: pool.NodeName})
	if errors.Is(err, consumer.ErrIdentity) {
		return status.Errorf(codes.FailedPrecondition, "%s; %v", message, err)
	}
	if err != nil {
		return status.Errorf(codes.Unavailable, "%s; inspect PVC consumer: %v", message, err)
	}
	if placement.Placement != consumer.Reschedulable {
		if requestedBytes > limitBytes {
			message += "; request exceeds Pool limit; change capacity configuration or request size"
		}
		if requestedBytes <= limitBytes && placement.Placement == consumer.Fixed && stats == nil && !s.observedTotalCanFit(pool, requestedBytes) {
			observed, err := s.inspectCapacity(ctx, pool, id)
			if err != nil {
				return capacityProbeError("inspect Pool filesystem capacity", err)
			}
			stats = &observed
		}
		if stats != nil && poolcapacity.ExceedsTotal(requestedBytes, stats.TotalBytes) {
			message += "; request exceeds filesystem total; change capacity configuration or request size"
		}
		if s.RetryRequests != nil {
			s.RetryRequests.Register(parameters[PVCNamespaceKey], parameters[PVCNameKey], strings.TrimPrefix(requestName, "pvc-"), pool.NodeName, poolGroup(pool), requestedBytes)
		}
		return status.Errorf(codes.Unavailable, "%s; %s; preserve node %q and retry", message, placement.Reason, pool.NodeName)
	}
	return status.Error(codes.ResourceExhausted, message)
}

func (s *Service) inspectCapacity(ctx context.Context, pool volumeapi.Pool, id string) (poolcapacity.Filesystem, error) {
	defer s.observeStep("create_filesystem_capacity", id)()
	return s.CapacityProbe.StatFSForPool(ctx, pool)
}

func (s *Service) poolReservedBytes(ctx context.Context, poolUID, volumeID string) (int64, error) {
	defer s.observeStep("create_capacity_ledger", volumeID)()
	volumesDone := s.observeStep("create_volume_list", volumeID)
	volumes, err := s.CapacityPools.ListVolumes(ctx)
	volumesDone()
	if err != nil {
		return 0, kubernetesAPIError("list volume owners", err)
	}
	movesDone := s.observeStep("create_move_list", volumeID)
	moves, err := s.CapacityPools.ListMoves(ctx)
	movesDone()
	if err != nil {
		return 0, kubernetesAPIError("list capacity-approved moves", err)
	}

	total, err := poolcapacity.ReservedBytesForPool(volumes, moves, poolUID)
	if err != nil {
		return 0, status.Error(codes.FailedPrecondition, err.Error())
	}
	return total, nil
}

func poolLimitBytes(pool volumeapi.Pool) (int64, error) {
	if pool.CapacityLimit == "" {
		return 0, fmt.Errorf("spec.capacity.limit is required")
	}
	return poolcapacity.LimitBytes(pool)
}

func capacityProbeError(operation string, err error) error {
	code := codes.Internal
	var retryable interface{ Retryable() bool }
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.As(err, &retryable) && retryable.Retryable():
		code = codes.Unavailable
	}
	return status.Errorf(code, "%s: %v", operation, err)
}

// A fresh node observation can justify a cheap temporary rejection. An
// oversized or unknown observation still requires a live probe; it cannot
// incorrectly turn a request into a permanent rejection after expansion.
func (s *Service) observedTotalCanFit(pool volumeapi.Pool, requested int64) bool {
	staleAfter := s.PoolReadinessStaleAfter
	if staleAfter <= 0 {
		staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
	}
	ready, _ := pool.ReadyAt(time.Now(), staleAfter)
	return ready && pool.Status.FilesystemTotalBytes > 0 && requested <= pool.Status.FilesystemTotalBytes
}
