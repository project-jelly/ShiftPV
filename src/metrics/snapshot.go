package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type sample struct {
	name   string
	value  float64
	labels []string
}

type snapshot struct {
	samples     []sample
	success     bool
	lastSuccess time.Time
}

// Cache contains completed observations only. Collect performs no external I/O.
type Cache struct {
	mu     sync.RWMutex
	groups map[string]snapshot
	desc   map[string]*prometheus.Desc
}

func newCache() *Cache {
	c := &Cache{groups: make(map[string]snapshot), desc: make(map[string]*prometheus.Desc)}
	for _, d := range definitions {
		c.desc[d.name] = prometheus.NewDesc("shiftpv_"+d.name, d.help, d.labels, nil)
	}
	return c
}

var definitions = []struct {
	name, help string
	labels     []string
}{
	{"pool_capacity_limit_bytes", "Configured logical capacity-hold limit.", []string{"pool", "node"}},
	{"pool_reserved_bytes", "Volume owner and active Move capacity holds, not disk usage.", []string{"pool", "node"}},
	{"pool_accounting_valid", "Whether the latest Pool capacity-hold accounting is valid.", []string{"pool", "node"}},
	{"pool_ready", "Pool readiness including generation and probe freshness.", []string{"pool", "node"}},
	{"pool_registration_retired", "Whether the Controller retired this exact Pool registration; not physical absence.", []string{"pool", "node"}},
	{"pool_filesystem_size_bytes", "Total size of the filesystem containing the registered directory.", []string{"pool", "node"}},
	{"pool_filesystem_available_bytes", "Filesystem bytes available to unprivileged users, including external writers.", []string{"pool", "node"}},
	{"pool_filesystem_available_inodes", "Free filesystem inodes reported by statfs.", []string{"pool", "node"}},
	{"pool_inventory_valid", "Whether the latest bounded node storage inventory is trustworthy.", []string{"pool", "node"}},
	{"pool_inventory_truncated", "Whether the latest node storage inventory exceeded its fixed scan budget.", []string{"pool", "node"}},
	{"metrics_snapshot_success", "Whether the latest observation completed successfully.", []string{"source"}},
	{"metrics_snapshot_last_success_timestamp_seconds", "Unix time of the last successful observation, zero before first success.", []string{"source"}},
	{"volumes", "Current Volume objects by phase.", []string{"phase"}},
	{"moves", "Moves referenced by a live Volume activeMove plus unfinished Completing journals, by phase.", []string{"phase"}},
	{"cleanup_requests", "Durable cleanup intents by observed lifecycle state; Unknown means invalid metadata.", []string{"state"}},
	{"copy_observations", "Node-observed copy placements classified against API authority.", []string{"pool", "state"}},
	{"mobility_deferred_volumes", "Volumes deferred during the last completed cordon discovery.", []string{"reason"}},
	{"persistent_volumes", "Driver-owned PersistentVolumes by phase and by the Pool holding the current copy.", []string{"phase", "pool"}},
	{"persistent_volumes_released_bytes", "Requested capacity of Released driver-owned PersistentVolumes awaiting an operator reclaim decision.", []string{"pool"}},
	{"volumes_without_persistent_volume", "Ready or preserved Blocked volume owners without a matching driver PersistentVolume, excluding lifecycle work.", []string{"pool"}},
	{"volumes_without_persistent_volume_reserved_bytes", "Logical owner reservations without a matching driver PersistentVolume, not disk usage or reclaim permission.", []string{"pool"}},
}

func (c *Cache) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.desc {
		ch <- d
	}
}

func (c *Cache) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	groups := make(map[string]snapshot, len(c.groups))
	for k, v := range c.groups {
		groups[k] = v
	}
	c.mu.RUnlock()
	for source, s := range groups {
		success := 0.0
		if s.success {
			success = 1
		}
		timestamp := 0.0
		if !s.lastSuccess.IsZero() {
			timestamp = float64(s.lastSuccess.UnixNano()) / 1e9
		}
		ch <- prometheus.MustNewConstMetric(c.desc["metrics_snapshot_success"], prometheus.GaugeValue, success, source)
		ch <- prometheus.MustNewConstMetric(c.desc["metrics_snapshot_last_success_timestamp_seconds"], prometheus.GaugeValue, timestamp, source)
		for _, v := range s.samples {
			ch <- prometheus.MustNewConstMetric(c.desc[v.name], prometheus.GaugeValue, v.value, v.labels...)
		}
	}
}

func (c *Cache) update(source string, values []sample, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.groups[source]
	previous.success = success
	if success {
		previous.samples = values
		previous.lastSuccess = time.Now()
	}
	c.groups[source] = previous
}

func (c *Cache) clear(source string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.groups[source]
	previous.samples = nil
	previous.success = false
	c.groups[source] = previous
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
