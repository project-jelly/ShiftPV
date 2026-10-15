package controller

import (
	"context"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) resumeInitializingCreate(ctx context.Context, existing volumeapi.State, request createRequest, parameters map[string]string) (volumeapi.State, error) {
	if !existing.Initializing() {
		return volumeapi.State{}, status.Error(codes.FailedPrecondition, "existing volume has no Pool copy identity")
	}
	if existing.InitialPool == nil {
		// Old releases could leave a spec without an admitted Pool. No effect
		// was authorized; re-admit the same request under its exact UID before
		// recording the newly selected Pool reservation.
		return s.placeNewCreate(ctx, request.id, existing.RequestName, existing.InitialNode, existing.CapacityBytes, parameters, existing.UID)
	}
	ref := existing.InitialPool
	pool, err := s.CapacityPools.PoolForIdentity(ctx, ref.Name, ref.UID, existing.InitialNode)
	if err != nil {
		return volumeapi.State{}, poolSelectionError(err)
	}
	if poolGroup(pool) != requestedPoolGroup(parameters) {
		return volumeapi.State{}, status.Errorf(codes.AlreadyExists, "volume %q belongs to Pool group %q", request.name, poolGroup(pool))
	}
	locks := s.PoolLocks
	if locks == nil {
		locks = &s.poolLifecycles
	}
	unlock := s.observeLock("create_pool_lock_wait", request.id, locks, pool.UID)
	defer unlock()
	current, err := s.Volumes.Get(ctx, request.id)
	if err != nil {
		return volumeapi.State{}, kubernetesAPIError("read initializing volume", err)
	}
	if current.UID != existing.UID {
		return volumeapi.State{}, status.Error(codes.Aborted, "initializing volume identity changed")
	}
	// The spec already owns its reservation. Do not charge it twice or
	// require new free space when completing the first status write.
	return s.Volumes.BeginCreateInPool(ctx, request.id, existing.RequestName, existing.InitialNode, existing.CapacityBytes, pool.Name, pool.UID)
}
