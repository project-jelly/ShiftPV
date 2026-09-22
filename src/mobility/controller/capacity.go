package controller

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func (r *Reconciler) ensureCapacity(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.DestinationNode == "" || r.CapacityProbe == nil || r.PoolLocks == nil {
		return fmt.Errorf("destination capacity admission is not configured")
	}
	// Source traversal can be slow; it does not spend destination capacity.
	// Re-read the destination and all reservations only after taking its lock.
	sourceBytes := move.Status.SourceBytes
	if sourceBytes <= 0 {
		measured, err := r.CapacityProbe.VolumeUsage(ctx, move.Spec.SourceNode, move.Spec.VolumeID)
		if err != nil {
			return fmt.Errorf("measure source volume usage: %w", err)
		}
		if measured <= 0 {
			return fmt.Errorf("source volume usage must be positive, got %d", measured)
		}
		sourceBytes = measured
	}
	if move.Status.SourceBytes != sourceBytes {
		previous := move.Status
		move.Status.SourceBytes = sourceBytes
		if err := r.persistMoveStatus(ctx, move, previous); err != nil {
			return err
		}
	}
	unlock := r.PoolLocks.Lock(observed.DestinationNode)
	defer unlock()

	pool, err := r.poolForNode(ctx, observed.DestinationNode, move.Spec.VolumeID)
	if err != nil {
		return err
	}
	if pool.UID == "" {
		return fmt.Errorf("destination Pool identity is missing")
	}
	requested, logicalReserved, physicalPending, limit, err := r.destinationCapacityForPool(ctx, *move, pool)
	if err != nil {
		return err
	}
	stats, err := r.CapacityProbe.StatFS(ctx, observed.DestinationNode)
	if err != nil {
		return fmt.Errorf("inspect destination Pool filesystem: %w", err)
	}

	previous := move.Status
	move.Status.DestinationNode = observed.DestinationNode
	move.Status.DestinationPoolUID = ""
	move.Status.SourceBytes = sourceBytes
	move.Status.CapacityApproved = false
	move.Status.CapacityReason = ""
	if requested > limit-logicalReserved {
		move.Status.CapacityReason = "DestinationReservationLimit"
	} else if physicalPending > stats.AvailableBytes || sourceBytes > stats.AvailableBytes-physicalPending {
		move.Status.CapacityReason = "DestinationFilesystemSpace"
	} else {
		move.Status.CapacityApproved = true
		move.Status.DestinationPoolUID = pool.UID
	}
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) destinationCapacityForPool(ctx context.Context, current volumeapi.Move, pool volumeapi.Pool) (requested, logicalReserved, physicalPending, limit int64, err error) {
	destination := pool.NodeName
	limit, err = poolcapacity.LimitBytes(pool)
	if err != nil {
		if errors.Is(err, poolcapacity.ErrLimitInexact) || errors.Is(err, poolcapacity.ErrLimitNotPositive) {
			return 0, 0, 0, 0, fmt.Errorf("destination Pool capacity limit must be positive bytes")
		}
		return 0, 0, 0, 0, fmt.Errorf("parse destination Pool capacity limit: %w", err)
	}

	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	state, exists := volumes[current.Spec.VolumeID]
	if !exists {
		return 0, 0, 0, 0, fmt.Errorf("volume %q has no capacity state", current.Spec.VolumeID)
	}
	if state.CapacityBytes <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("volume %q has invalid capacity", current.Spec.VolumeID)
	}
	requested = state.CapacityBytes
	logicalReserved, err = poolcapacity.ReservedBytes(volumes, moves, destination)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	for _, move := range moves {
		if move.Name == current.Name || volumeapi.MoveCleanupSettled(move) || !move.Status.CapacityApproved || move.Status.DestinationNode != destination {
			continue
		}
		state, exists := volumes[move.Spec.VolumeID]
		if !exists {
			return 0, 0, 0, 0, fmt.Errorf("approved move %q has no volume state", move.Name)
		}
		if !volumeapi.MoveReservesDestination(move, state, destination) {
			continue
		}
		if state.CapacityBytes <= 0 || move.Status.SourceBytes < 0 {
			return 0, 0, 0, 0, fmt.Errorf("approved move %q has invalid capacity state", move.Name)
		}
		if move.Status.SourceBytes > math.MaxInt64-physicalPending {
			return 0, 0, 0, 0, fmt.Errorf("destination physical pending total overflows int64")
		}
		physicalPending += move.Status.SourceBytes
	}
	return requested, logicalReserved, physicalPending, limit, nil
}

func (r *Reconciler) poolForNode(ctx context.Context, nodeName, volumeID string) (volumeapi.Pool, error) {
	pools, err := r.Repository.ReadyPools(ctx)
	if err != nil {
		return volumeapi.Pool{}, err
	}
	for _, pool := range pools {
		if pool.NodeName == nodeName {
			if volumeapi.PoolHasServingVolume(pool, volumeID) {
				return volumeapi.Pool{}, fmt.Errorf("node %q Pool contains a serving copy for volume %q", nodeName, volumeID)
			}
			return pool, nil
		}
	}
	return volumeapi.Pool{}, fmt.Errorf("node %q has no Ready Pool", nodeName)
}
