package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

var errCapacityApprovalPersistence = errors.New("capacity approval persistence failed")
var errPoolBusy = errors.New("Pool capacity lock is busy")

func (r *Reconciler) ensureCapacity(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.DestinationNode == "" || r.CapacityProbe == nil || r.PoolLocks == nil {
		return fmt.Errorf("destination capacity admission is not configured")
	}
	if move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1 && move.Status.Preparation != nil {
		// The legacy FSM may ask for capacity while entering this stage. The
		// prepared CAS converts only an already observed WaitingForCapacity.
		if move.Status.Phase != string(fsm.PhaseWaitingForCapacity) {
			return nil
		}
		return r.convertPreparation(ctx, move)
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
	unlock, acquired := r.PoolLocks.TryLock(pool.UID)
	if !acquired {
		return "", errPoolBusy
	}
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
	planned := *move
	planned.Status.DestinationNode = pool.NodeName
	planned.Status.DestinationPoolUID = pool.UID
	if err := prepareMoveCopyIdentitiesForPool(&planned, pool); err != nil {
		return "", err
	}
	planned.Status.CapacityApproved = true
	planned.Status.CapacityReason = ""
	*move = planned
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
	if current.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1 && current.Status.Preparation != nil {
		if err := preparedCapacitySnapshot(current, state, moves, pool); err != nil {
			return 0, 0, 0, 0, err
		}
	}
	logicalReserved, err = poolcapacity.ReservedBytesForPool(volumes, moves, pool.UID)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	physicalPending, err = pendingPhysicalBytes(current, pool, volumes, moves)
	return requested, logicalReserved, physicalPending, limit, err
}

func pendingPhysicalBytes(current volumeapi.Move, pool volumeapi.Pool, volumes map[string]volumeapi.State, moves []volumeapi.Move) (int64, error) {
	var physicalPending int64
	for _, move := range moves {
		if move.Name == current.Name || volumeapi.MoveCleanupSettled(move) || !move.Status.CapacityApproved || move.Status.DestinationPoolUID != pool.UID {
			continue
		}
		state, exists := volumes[move.Spec.VolumeID]
		if !exists {
			return 0, fmt.Errorf("approved move %q has no volume state", move.Name)
		}
		if state.CurrentCopy != nil && state.CurrentCopy.PoolUID == pool.UID {
			continue
		}
		if state.CapacityBytes <= 0 || move.Status.SourceBytes < 0 {
			return 0, fmt.Errorf("approved move %q has invalid capacity state", move.Name)
		}
		if move.Status.SourceBytes > math.MaxInt64-physicalPending {
			return 0, fmt.Errorf("destination physical pending total overflows int64")
		}
		physicalPending += move.Status.SourceBytes
	}
	return physicalPending, nil
}
