package metrics

import (
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// retainedVolumeSamples observes owners without a PV reference, not permission
// to delete a copy. Ready can precede PV publication, so alerts need a grace period.
func retainedVolumeSamples(pools []volumeapi.Pool, persistentVolumes []corev1.PersistentVolume, volumes map[string]volumeapi.State) []sample {
	referenced := make(map[string]bool, len(persistentVolumes))
	for _, pv := range persistentVolumes {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == volume.DriverName {
			referenced[pv.Spec.CSI.VolumeHandle] = true
		}
	}
	registered := make(map[string]volumeapi.Pool, len(pools))
	counts, reserved := map[string]int{"unknown": 0}, map[string]float64{}
	for _, pool := range pools {
		registered[pool.UID] = pool
		counts[pool.Name] = 0
	}
	for id, state := range volumes {
		if referenced[id] || !retainedOwner(id, state) {
			continue
		}
		name := "unknown"
		copy := state.CurrentCopy
		if pool, found := registered[copy.PoolUID]; found && pool.Name == copy.PoolName && pool.NodeName == copy.NodeName {
			name = pool.Name
		}
		counts[name]++
		reserved[name] += float64(state.CapacityBytes)
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	var values []sample
	for _, name := range names {
		values = append(values,
			sample{"volumes_without_persistent_volume", float64(counts[name]), []string{name}},
			sample{"volumes_without_persistent_volume_reserved_bytes", reserved[name], []string{name}},
		)
	}
	return values
}

func retainedOwner(id string, state volumeapi.State) bool {
	if (state.Phase != volumeapi.PhaseReady && state.Phase != volumeapi.PhaseBlocked) ||
		state.Deleting || state.HasCleanup || state.DeletionOperationID != "" || state.ActiveMove != "" || state.CapacityBytes <= 0 {
		return false
	}
	copy := state.CurrentCopy
	return copy != nil && copy.Validate() == nil && copy.Role == volume.RoleServing &&
		copy.VolumeID == id && copy.VolumeUID == state.UID && copy.NodeName == state.OwnerNode
}
