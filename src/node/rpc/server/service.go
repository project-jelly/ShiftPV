package server

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type Executor interface {
	Execute(context.Context, *protocol.EffectRequest, protocol.Operation) error
}
type Pools interface {
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
}
type Probe interface {
	StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error)
}

type Service struct {
	protocol.UnimplementedNodeServer
	Identity       volumeapi.NodeExecutor
	Executor       Executor
	Pools          Pools
	Probe          Probe
	VerifyIdentity func(context.Context) error
}

func (s *Service) CreateCopy(ctx context.Context, req *protocol.EffectRequest) (*emptypb.Empty, error) {
	return s.effect(ctx, req, protocol.Operation_CREATE)
}
func (s *Service) ReclaimCopy(ctx context.Context, req *protocol.EffectRequest) (*emptypb.Empty, error) {
	return s.effect(ctx, req, protocol.Operation_RECLAIM)
}
func (s *Service) effect(ctx context.Context, req *protocol.EffectRequest, operation protocol.Operation) (*emptypb.Empty, error) {
	if req == nil || req.VolumeName == "" || req.VolumeUid == "" || req.OperationId == "" {
		return nil, status.Error(codes.InvalidArgument, "missing operation authority")
	}
	if err := s.identity(req.ExecutorUid, req.NodeName); err != nil {
		return nil, err
	}
	if err := s.verify(ctx); err != nil {
		return nil, err
	}
	if s.Executor == nil {
		return nil, status.Error(codes.Unavailable, "Node executor is unavailable")
	}
	if err := s.Executor.Execute(ctx, req, operation); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}
func (s *Service) GetCapacity(ctx context.Context, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	if req == nil || req.PoolName == "" || req.PoolUid == "" || !validHex(req.Evidence, 32) || !validHex(req.Nonce, 16) {
		return nil, status.Error(codes.InvalidArgument, "missing capacity authority")
	}
	if err := s.identity(req.ExecutorUid, req.NodeName); err != nil {
		return nil, err
	}
	if err := s.verify(ctx); err != nil {
		return nil, err
	}
	if s.Pools == nil || s.Probe == nil {
		return nil, status.Error(codes.Unavailable, "Node capacity probe is unavailable")
	}
	pool, err := s.Pools.ReadyPoolForIdentity(ctx, req.PoolName, req.PoolUid, s.Identity.NodeName)
	if err != nil {
		return nil, rpcError(err)
	}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil || evidence != req.Evidence {
		return nil, status.Error(codes.FailedPrecondition, "Pool measurement evidence changed")
	}
	stats, err := s.Probe.StatFSForPool(ctx, pool)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.verify(ctx); err != nil {
		return nil, err
	}
	return &protocol.CapacityResponse{ExecutorUid: s.Identity.PodUID, Evidence: evidence, Nonce: req.Nonce, TotalBytes: stats.TotalBytes, AvailableBytes: stats.AvailableBytes, AvailableInodes: stats.AvailableInodes}, nil
}
func (s *Service) identity(uid, node string) error {
	if !s.Identity.Valid() || uid != s.Identity.PodUID || node != s.Identity.NodeName {
		return status.Error(codes.FailedPrecondition, "Node executor identity changed")
	}
	return nil
}
func (s *Service) verify(ctx context.Context) error {
	if s.VerifyIdentity != nil {
		if err := s.VerifyIdentity(ctx); err != nil {
			return rpcError(err)
		}
	}
	return nil
}
func validHex(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == size && hex.EncodeToString(decoded) == value
}
func rpcError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "Node operation cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "Node operation deadline exceeded")
	case apierrors.IsNotFound(err), errors.Is(err, volumeapi.ErrStateConflict), errors.Is(err, cleanupapi.ErrConflict):
		return status.Error(codes.FailedPrecondition, "Node operation authority changed")
	default:
		return status.Error(codes.Unavailable, "Node operation requires reconciliation")
	}
}
