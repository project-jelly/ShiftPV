package controller

import (
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// discoverySource invalidates a scheduling delay when routing changes. It is
// never execution authority; a due attempt still runs the full live preflight.
type discoverySource struct {
	volumeUID, ownerNode, nodeUID string
	copy                          volume.CopyIdentity
}

type deferredDiscovery struct {
	source discoverySource
	until  time.Time
}

// deferDiscovery retains only the next attempt time, not an eligibility result.
// A late worker cannot postpone a replaced or independently rerouted source.
func (d *volumeDispatcher) deferDiscovery(work volumeWork, result discoveryResult) time.Duration {
	if !work.discover || d.retryAfter <= 0 || work.source.volumeUID == "" {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current, exists := d.hints[work.volumeID]
	if !exists || !current.discover || current.source != work.source {
		return 0
	}
	if result.err != nil || result.reason == "" || result.uid != work.source.volumeUID {
		delete(d.deferred, work.volumeID)
		return 0
	}
	d.deferred[work.volumeID] = deferredDiscovery{source: work.source, until: d.now().Add(d.retryAfter)}
	return d.retryAfter
}
