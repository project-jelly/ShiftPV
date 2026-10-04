package controller

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

const selectedNodeAnnotation = "volume.kubernetes.io/selected-node"

type PoolCapacityRegistry interface {
	ReadyPoolForNode(context.Context, string) (volumeapi.Pool, error)
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
}

type PoolCapacityProbe interface {
	StatFS(context.Context, string) (poolcapacity.Filesystem, error)
}

func (s *Service) beginCreateWithinPool(ctx context.Context, id, requestName, nodeName string, requestedBytes int64, parameters map[string]string) (volumeapi.State, error) {
	existing, err := s.Volumes.Get(ctx, id)
	if err == nil {
		if err := validateCreateIntent(existing, requestName, nodeName, requestedBytes); err != nil {
			return volumeapi.State{}, err
		}
		return s.Volumes.BeginCreate(ctx, id, requestName, nodeName, requestedBytes)
	}
	if !apierrors.IsNotFound(err) {
		return volumeapi.State{}, kubernetesAPIError("read volume creation intent", err)
	}

	unlock := s.poolLifecycles.Lock(nodeName)
	if s.PoolLocks != nil {
		unlock()
		unlock = s.PoolLocks.Lock(nodeName)
	}
	defer unlock()

	existing, err = s.Volumes.Get(ctx, id)
	if err == nil {
		if err := validateCreateIntent(existing, requestName, nodeName, requestedBytes); err != nil {
			return volumeapi.State{}, err
		}
		return s.Volumes.BeginCreate(ctx, id, requestName, nodeName, requestedBytes)
	}
	if !apierrors.IsNotFound(err) {
		return volumeapi.State{}, kubernetesAPIError("read volume creation intent", err)
	}

	pool, err := s.CapacityPools.ReadyPoolForNode(ctx, nodeName)
	if err != nil {
		if errors.Is(err, volumeapi.ErrPoolConfiguration) || errors.Is(err, volumeapi.ErrPoolNotFound) || errors.Is(err, volumeapi.ErrPoolNotReady) {
			return volumeapi.State{}, status.Errorf(codes.FailedPrecondition, "read selected Pool: %v", err)
		}
		return volumeapi.State{}, kubernetesAPIError("read selected Pool", err)
	}
	if volumeapi.PoolHasServingVolume(pool, id) {
		return volumeapi.State{}, status.Errorf(codes.FailedPrecondition, "Pool %q already contains a serving copy for volume %q; wait for orphan cleanup", pool.Name, id)
	}
	limitBytes, err := poolLimitBytes(pool)
	if err != nil {
		return volumeapi.State{}, status.Errorf(codes.FailedPrecondition, "Pool %q capacity limit is invalid: %v", pool.Name, err)
	}
	reservedBytes, err := s.poolReservedBytes(ctx, nodeName)
	if err != nil {
		return volumeapi.State{}, err
	}
	logicalFree := int64(0)
	if reservedBytes < limitBytes {
		logicalFree = limitBytes - reservedBytes
	}
	if requestedBytes > logicalFree {
		return volumeapi.State{}, s.capacityDenied(ctx, parameters, requestName, nodeName, requestedBytes <= limitBytes,
			"Pool %q reservation limit exceeded: requested=%d reserved=%d limit=%d",
			pool.Name, requestedBytes, reservedBytes, limitBytes)
	}

	stats, err := s.CapacityProbe.StatFS(ctx, nodeName)
	if err != nil {
		return volumeapi.State{}, capacityProbeError("inspect Pool filesystem capacity", err)
	}
	if requestedBytes > stats.AvailableBytes {
		return volumeapi.State{}, s.capacityDenied(ctx, parameters, requestName, nodeName, stats.TotalBytes == 0 || requestedBytes <= stats.TotalBytes,
			"Pool %q filesystem space is insufficient: requested=%d available=%d",
			pool.Name, requestedBytes, stats.AvailableBytes)
	}
	return s.Volumes.BeginCreate(ctx, id, requestName, nodeName, requestedBytes)
}

// A scheduler-owned PVC can be moved to another node after ResourceExhausted.
// A Pod-owned PVC created after its consumer was assigned a node cannot: CDI
// scratch PVCs are one example. Keep its selected node so provisioning retries
// there when a reservation or filesystem space becomes available.
func (s *Service) capacityDenied(ctx context.Context, parameters map[string]string, requestName, nodeName string, canFitLater bool, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	if !canFitLater {
		return status.Error(codes.ResourceExhausted, message)
	}
	fixed, err := s.hasScheduledPodConsumer(ctx, parameters, requestName, nodeName)
	if err != nil {
		return status.Errorf(codes.Unavailable, "%s; inspect PVC consumer: %v", message, err)
	}
	if fixed {
		return status.Errorf(codes.Unavailable, "%s; scheduled consumer on node %q requires same-node retry", message, nodeName)
	}
	return status.Error(codes.ResourceExhausted, message)
}

func (s *Service) hasScheduledPodConsumer(ctx context.Context, parameters map[string]string, requestName, nodeName string) (bool, error) {
	name, namespace := parameters[PVCNameKey], parameters[PVCNamespaceKey]
	if name == "" || namespace == "" {
		return false, nil
	}
	pvc, err := s.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if requestName != "pvc-"+string(pvc.UID) || pvc.Annotations[selectedNodeAnnotation] != nodeName {
		return false, nil
	}
	for _, owner := range pvc.OwnerReferences {
		if owner.APIVersion != "v1" || owner.Kind != "Pod" || owner.Name == "" {
			continue
		}
		pod, err := s.Client.CoreV1().Pods(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if pod.UID != owner.UID || pod.Spec.NodeName != nodeName {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Service) poolReservedBytes(ctx context.Context, nodeName string) (int64, error) {
	volumes, err := s.CapacityPools.ListVolumes(ctx)
	if err != nil {
		return 0, kubernetesAPIError("list volume owners", err)
	}
	moves, err := s.CapacityPools.ListMoves(ctx)
	if err != nil {
		return 0, kubernetesAPIError("list capacity-approved moves", err)
	}

	total, err := poolcapacity.ReservedBytes(volumes, moves, nodeName)
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

func validateCreateIntent(existing volumeapi.State, requestName, nodeName string, capacityBytes int64) error {
	if existing.RequestName != requestName {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists with incompatible requestName", requestName)
	}
	if existing.InitialNode != nodeName {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists with incompatible initialNode", requestName)
	}
	if existing.CapacityBytes != capacityBytes {
		return status.Errorf(codes.AlreadyExists, "volume %q already exists with incompatible capacityBytes", requestName)
	}
	return nil
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
