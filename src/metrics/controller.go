package metrics

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type Inventory interface {
	ListPools(context.Context) ([]volumeapi.Pool, error)
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
}

type CleanupInventory interface {
	List(context.Context) ([]cleanupapi.Cleanup, error)
}

type PersistentVolumeInventory interface {
	List(context.Context) ([]corev1.PersistentVolume, error)
}

type Controller struct {
	Exporter          *Exporter
	Inventory         Inventory
	Cleanups          CleanupInventory
	PersistentVolumes PersistentVolumeInventory
	Interval          time.Duration
	StaleAfter        time.Duration
}

func (c *Controller) Run(ctx context.Context) {
	if c.Interval <= 0 {
		klog.Error("metrics snapshot interval must be positive")
		return
	}
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		snapshotCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := c.Refresh(snapshotCtx); err != nil {
			klog.V(2).Infof("metrics snapshot failed: %v", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) Refresh(ctx context.Context) (refreshErr error) {
	defer func() {
		if refreshErr != nil {
			c.Exporter.Cache.update("metadata", nil, false)
		}
	}()
	pools, err := c.Inventory.ListPools(ctx)
	if err != nil {
		return err
	}
	volumes, err := c.Inventory.ListVolumes(ctx)
	if err != nil {
		return err
	}
	moves, err := c.Inventory.ListMoves(ctx)
	if err != nil {
		return err
	}
	staleAfter := c.StaleAfter
	if staleAfter <= 0 {
		staleAfter = volumeapi.DefaultPoolReadinessStaleAfter
	}
	values := c.poolSamples(pools, volumes, moves, staleAfter)
	values = append(values, volumePhaseSamples(volumes)...)
	moveValues, err := movePhaseSamples(volumes, moves)
	if err != nil {
		return err
	}
	values = append(values, moveValues...)
	cleanupValues, cleanupContracts, err := c.cleanupPhaseSamples(ctx)
	if err != nil {
		return err
	}
	values = append(values, cleanupValues...)
	values = append(values, copyObservationSamples(pools, volumes, moves, cleanupContracts)...)
	var persistentVolumes []corev1.PersistentVolume
	if c.PersistentVolumes != nil {
		if persistentVolumes, err = c.PersistentVolumes.List(ctx); err != nil {
			return err
		}
	}
	// Emitted even with no inventory: an absent series cannot be told from a
	// Released count of zero, and ShiftPVReleasedVolumes reads absence as calm.
	values = append(values, persistentVolumeSamples(persistentVolumes, volumes)...)
	c.Exporter.Cache.update("metadata", values, true)
	return nil
}

func volumePhaseSamples(volumes map[string]volumeapi.State) []sample {
	counts := make(map[string]int)
	for _, state := range volumes {
		counts[bounded(state.Phase, volumePhases)]++
	}
	var values []sample
	for _, phase := range volumePhases {
		values = append(values, sample{"volumes", float64(counts[phase]), []string{phase}})
	}
	return values
}

// movePhaseSamples counts one Move per volume that names an active Move, plus
// the Completing Moves that the volume no longer points at. A volume naming a
// Move that does not exist or belongs to another volume is a broken link.
func movePhaseSamples(volumes map[string]volumeapi.State, moves []volumeapi.Move) ([]sample, error) {
	active := make(map[string]volumeapi.Move, len(moves))
	for _, move := range moves {
		active[move.Name] = move
	}
	counts := make(map[string]int)
	for id, state := range volumes {
		if state.ActiveMove == "" {
			continue
		}
		move, exists := active[state.ActiveMove]
		if !exists || move.Spec.VolumeID != id {
			return nil, fmt.Errorf("active Move link is incomplete")
		}
		counts[bounded(move.Status.Phase, movePhases)]++
	}
	for _, move := range moves {
		state, exists := volumes[move.Spec.VolumeID]
		if move.Status.Phase == "Completing" && (!exists || state.ActiveMove != move.Name) {
			counts["Completing"]++
		}
	}
	var values []sample
	for _, phase := range movePhases {
		values = append(values, sample{"moves", float64(counts[phase]), []string{phase}})
	}
	return values, nil
}

// cleanupPhaseSamples also returns the listed journals, which the copy
// observation classification needs. A journal with no phase counts as Pending.
func (c *Controller) cleanupPhaseSamples(ctx context.Context) ([]sample, []cleanupapi.Cleanup, error) {
	cleanupCounts := map[string]int{}
	cleanupPhases := []string{cleanupapi.PhasePending, cleanupapi.PhaseRunning, cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence, cleanupapi.PhaseNeedsReview, cleanupapi.PhaseCompleted, "Unknown"}
	var cleanupContracts []cleanupapi.Cleanup
	if c.Cleanups != nil {
		cleanups, err := c.Cleanups.List(ctx)
		if err != nil {
			return nil, nil, err
		}
		cleanupContracts = cleanups
		for _, request := range cleanups {
			phase := request.Status.Phase
			if phase == "" {
				phase = cleanupapi.PhasePending
			}
			cleanupCounts[bounded(phase, cleanupPhases)]++
		}
	}
	var values []sample
	for _, state := range cleanupPhases {
		values = append(values, sample{"cleanup_requests", float64(cleanupCounts[state]), []string{state}})
	}
	return values, cleanupContracts, nil
}

// persistentVolumeSamples counts driver-owned PersistentVolumes by phase and by
// the Pool that holds the volume's current copy, and sums the capacity that
// Released volumes hold for an operator reclaim decision. A volume handle that
// no live ShiftPVVolume claims counts as pool "unknown". PersistentVolume names
// and claim references stay out of the label set.
//
// Pool "unknown" is always present so the family exists even with no
// driver-owned PersistentVolume at all, which keeps a zero count distinct from
// a series that never arrived.
func persistentVolumeSamples(persistentVolumes []corev1.PersistentVolume, volumes map[string]volumeapi.State) []sample {
	counts := map[string]map[string]int{"unknown": {}}
	released := map[string]int64{}
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != volume.DriverName {
			continue
		}
		pool := persistentVolumePool(persistentVolume.Spec.CSI.VolumeHandle, volumes)
		phase := bounded(string(persistentVolume.Status.Phase), persistentVolumePhases)
		if counts[pool] == nil {
			counts[pool] = map[string]int{}
			released[pool] = 0
		}
		counts[pool][phase]++
		if phase == "Released" {
			released[pool] += persistentVolume.Spec.Capacity.Storage().Value()
		}
	}
	pools := make([]string, 0, len(counts))
	for pool := range counts {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	var values []sample
	for _, pool := range pools {
		for _, phase := range persistentVolumePhases {
			values = append(values, sample{"persistent_volumes", float64(counts[pool][phase]), []string{phase, pool}})
		}
		values = append(values, sample{"persistent_volumes_released_bytes", float64(released[pool]), []string{pool}})
	}
	return values
}

func persistentVolumePool(volumeHandle string, volumes map[string]volumeapi.State) string {
	state, exists := volumes[volumeHandle]
	if !exists || state.CurrentCopy == nil || state.CurrentCopy.PoolName == "" {
		return "unknown"
	}
	return state.CurrentCopy.PoolName
}

func (c *Controller) poolSamples(pools []volumeapi.Pool, volumes map[string]volumeapi.State, moves []volumeapi.Move, staleAfter time.Duration) []sample {
	var values []sample
	for _, pool := range pools {
		labels := []string{pool.Name, pool.NodeName}
		ready, _ := pool.ReadyAt(time.Now(), staleAfter)
		values = append(values, sample{"pool_ready", boolValue(ready), labels})
		inventoryValid, inventoryTruncated := false, false
		if pool.Status.Inventory != nil {
			inventoryValid, inventoryTruncated = pool.Status.Inventory.Valid, pool.Status.Inventory.Truncated
		}
		values = append(values,
			sample{"pool_inventory_valid", boolValue(inventoryValid), labels},
			sample{"pool_inventory_truncated", boolValue(inventoryTruncated), labels},
		)
		limit, limitErr := capacity.LimitBytes(pool)
		reserved, accountingErr := capacity.ReservedBytesForPool(volumes, moves, pool.UID)
		valid := limitErr == nil && accountingErr == nil
		values = append(values, sample{"pool_accounting_valid", boolValue(valid), labels})
		if !valid {
			// Keep only this Pool's last good numbers; validity explicitly marks them stale.
			c.Exporter.Cache.mu.RLock()
			for _, previous := range c.Exporter.Cache.groups["metadata"].samples {
				if (previous.name == "pool_capacity_limit_bytes" || previous.name == "pool_reserved_bytes") && previous.labels[0] == pool.Name && previous.labels[1] == pool.NodeName {
					values = append(values, previous)
				}
			}
			c.Exporter.Cache.mu.RUnlock()
			continue
		}
		values = append(values, sample{"pool_capacity_limit_bytes", float64(limit), labels}, sample{"pool_reserved_bytes", float64(reserved), labels})
	}
	return values
}

func copyObservationSamples(pools []volumeapi.Pool, volumes map[string]volumeapi.State, moves []volumeapi.Move, cleanups []cleanupapi.Cleanup) []sample {
	authority := copyAuthority(volumes, moves, cleanups)
	var values []sample
	// One series per Pool: the operator needs to know which Pool holds the
	// reviewed copies. Volume and copy IDs stay out of the label set.
	for _, pool := range pools {
		counts := map[string]int{}
		if pool.Status.Inventory == nil || !pool.Status.Inventory.Valid || pool.Status.Inventory.Truncated {
			counts["NeedsReview"]++
		}
		if pool.Status.Inventory != nil {
			for _, observed := range pool.Status.Inventory.Copies {
				counts[observationState(observed, authority)]++
			}
		}
		for _, state := range copyObservationStates {
			values = append(values, sample{"copy_observations", float64(counts[state]), []string{pool.Name, state}})
		}
	}
	return values
}

var copyObservationStates = []string{"Current", "InFlight", "CleanupTarget", "OrphanPreserved", "Missing", "NeedsReview"}

// copyAuthority maps every copy identity the API still vouches for to the
// reason it is expected on disk. Later claims deliberately win over earlier
// ones: a cleanup target outranks an in-flight copy, which outranks current.
func copyAuthority(volumes map[string]volumeapi.State, moves []volumeapi.Move, cleanups []cleanupapi.Cleanup) map[volume.CopyIdentity]string {
	authority := map[volume.CopyIdentity]string{}
	for _, state := range volumes {
		if state.CurrentCopy != nil {
			authority[*state.CurrentCopy] = "Current"
		}
	}
	for _, move := range moves {
		state, active := volumes[move.Spec.VolumeID]
		if !active || state.ActiveMove != move.Name {
			continue
		}
		for _, identity := range []*volume.CopyIdentity{move.Status.SourceCopy, move.Status.IncomingCopy, move.Status.DestinationCopy} {
			if identity != nil {
				authority[*identity] = "InFlight"
			}
		}
	}
	for _, request := range cleanups {
		if request.Status.Phase != cleanupapi.PhaseCompleted {
			authority[request.Spec.Target] = "CleanupTarget"
		}
	}
	return authority
}

// observationState classifies one scanner observation against live authority.
func observationState(observed volumeapi.CopyObservation, authority map[volume.CopyIdentity]string) string {
	if observed.Identity == nil || observed.Problem != "" {
		return "NeedsReview"
	}
	if !observed.Present {
		return "Missing"
	}
	if state, exists := authority[*observed.Identity]; exists {
		return state
	}
	return "OrphanPreserved"
}
