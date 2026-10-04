package metrics

import (
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

// ObservePool reuses the existing probe's statfs result, including on a full or read-only filesystem.
func (e *Exporter) ObservePool(pool volumeapi.Pool, result readiness.Result, err error) {
	e.ObservePools([]readiness.Observation{{Pool: pool, Result: result, Err: err}}, err)
}

// ObservePools replaces the filesystem snapshot with all Pools observed on
// this node in one pass, so no Pool's metrics overwrite another's.
func (e *Exporter) ObservePools(observations []readiness.Observation, err error) {
	if err != nil {
		e.Cache.update("filesystem", nil, false)
		return
	}
	if len(observations) == 0 || len(observations) == 1 && observations[0].Pool.Name == "" {
		e.Cache.clear("filesystem")
		return
	}
	values := make([]sample, 0, 3*len(observations))
	for _, observation := range observations {
		if observation.Err != nil || !observation.Result.CapacityReadable.OK {
			e.Cache.update("filesystem", nil, false)
			return
		}
		pool, result := observation.Pool, observation.Result
		labels := []string{pool.Name, pool.NodeName}
		values = append(values,
			sample{"pool_filesystem_size_bytes", float64(result.Filesystem.TotalBytes), labels},
			sample{"pool_filesystem_available_bytes", float64(result.Filesystem.AvailableBytes), labels},
			sample{"pool_filesystem_available_inodes", float64(result.Filesystem.AvailableInodes), labels},
		)
	}
	e.Cache.update("filesystem", values, true)
}
