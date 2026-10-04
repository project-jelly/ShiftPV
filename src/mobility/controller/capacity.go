package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var errCapacityApprovalPersistence = errors.New("capacity approval persistence failed")

func (r *Reconciler) ensureCapacity(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.DestinationNode == "" || r.CapacityProbe == nil || r.PoolLocks == nil {
		return fmt.Errorf("destination capacity admission is not configured")
	}
	if err := r.measureSourceCopy(ctx, move); err != nil {
		return err
	}
	pools, err := r.destinationPools(ctx, *move, observed.DestinationNode)
	if err != nil {
		return err
	}
	// Approval is a durable hold. A retry must retain its exact incarnation,
	// even if capacity has since been spent or the node has another usable Pool.
	if move.Status.CapacityApproved {
		return nil
	}
	var denial string
	for _, pool := range pools {
		reason, err := r.approveDestinationPool(ctx, move, pool)
		if err != nil {
			return err
		}
		if reason == "" {
			return nil
		}
		if denial == "" {
			denial = reason
		}
	}
	previous := move.Status
	move.Status.DestinationNode = observed.DestinationNode
	move.Status.CapacityReason = denial
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) measureSourceCopy(ctx context.Context, move *volumeapi.Move) error {
	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil {
		return err
	}
	copy := state.CurrentCopy
	if state.Phase != volumeapi.PhaseMoving || state.ActiveMove != move.Name || state.OwnerNode != move.Spec.SourceNode ||
		copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing || copy.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("source volume has no locked serving-copy identity")
	}
	if move.Status.SourceCopy != nil && *move.Status.SourceCopy != *copy {
		return fmt.Errorf("source copy identity changed before capacity admission")
	}
	previous := move.Status
	if move.Status.SourceCopy == nil {
		// The volume CAS may have succeeded before the Move status was persisted.
		identity := *copy
		move.Status.SourceCopy = &identity
	}
	if move.Status.SourceBytes <= 0 {
		measured, err := r.CapacityProbe.VolumeUsageForCopy(ctx, *copy)
		if err != nil {
			return fmt.Errorf("measure source volume usage: %w", err)
		}
		if measured <= 0 {
			return fmt.Errorf("source volume usage must be positive, got %d", measured)
		}
		move.Status.SourceBytes = measured
	}
	return r.persistMoveStatus(ctx, move, previous)
}

func (r *Reconciler) destinationPools(ctx context.Context, move volumeapi.Move, nodeName string) ([]volumeapi.Pool, error) {
	copy := move.Status.SourceCopy
	if copy == nil || copy.Validate() != nil || nodeName == copy.NodeName {
		return nil, fmt.Errorf("automatic move requires an exact source copy and another destination node")
	}
	source, err := r.Repository.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return nil, err
	}
	if move.Status.CapacityApproved && (move.Status.DestinationPoolUID == "" || move.Status.DestinationNode != nodeName) {
		return nil, fmt.Errorf("approved destination Pool identity is missing or changed")
	}
	pools, err := r.Repository.ReadyPools(ctx)
	if err != nil {
		return nil, err
	}
	var eligible []volumeapi.Pool
	for _, pool := range pools {
		if pool.NodeName != nodeName || pool.PoolGroup != source.PoolGroup ||
			(move.Status.DestinationPoolUID != "" && pool.UID != move.Status.DestinationPoolUID) {
			continue
		}
		if pool.UID == "" || volumeapi.PoolHasServingVolume(pool, move.Spec.VolumeID) {
			return nil, fmt.Errorf("destination Pool %q has missing identity or a serving copy", pool.Name)
		}
		eligible = append(eligible, pool)
	}
	if len(eligible) == 0 {
		return nil, fmt.Errorf("node %q has no Ready Pool in source group with the required identity", nodeName)
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].Name < eligible[j].Name })
	return eligible, nil
}

func (r *Reconciler) approveDestinationPool(ctx context.Context, move *volumeapi.Move, pool volumeapi.Pool) (string, error) {
	unlock := r.PoolLocks.Lock(pool.UID)
	defer unlock()
	requested, logicalReserved, physicalPending, limit, err := r.destinationCapacityForPool(ctx, *move, pool)
	if err != nil {
		return "", err
	}
	stats, err := r.CapacityProbe.StatFSForPool(ctx, pool)
	if err != nil {
		return "", fmt.Errorf("inspect destination Pool filesystem: %w", err)
	}
	if requested > limit-logicalReserved {
		return "DestinationReservationLimit", nil
	}
	if physicalPending > stats.AvailableBytes || move.Status.SourceBytes > stats.AvailableBytes-physicalPending {
		return "DestinationFilesystemSpace", nil
	}
	previous := move.Status
	move.Status.DestinationNode = pool.NodeName
	move.Status.DestinationPoolUID = pool.UID
	move.Status.CapacityApproved = true
	move.Status.CapacityReason = ""
	if err := r.persistMoveStatus(ctx, move, previous); err != nil {
		// The write may have been accepted despite a lost response. Leave the
		// durable state for the next observation; diagnostics cannot replay this
		// newly mutated approval after releasing the capacity lock.
		return "", errors.Join(errCapacityApprovalPersistence, err)
	}
	return "", nil
}

func (r *Reconciler) destinationCapacityForPool(ctx context.Context, current volumeapi.Move, pool volumeapi.Pool) (requested, logicalReserved, physicalPending, limit int64, err error) {
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
	logicalReserved, err = poolcapacity.ReservedBytesForPool(volumes, moves, pool.UID)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	for _, move := range moves {
		if move.Name == current.Name || volumeapi.MoveCleanupSettled(move) || !move.Status.CapacityApproved || move.Status.DestinationPoolUID != pool.UID {
			continue
		}
		state, exists := volumes[move.Spec.VolumeID]
		if !exists {
			return 0, 0, 0, 0, fmt.Errorf("approved move %q has no volume state", move.Name)
		}
		if state.CurrentCopy != nil && state.CurrentCopy.PoolUID == pool.UID {
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
