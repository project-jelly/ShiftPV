package controller

import (
	"errors"
	"sync"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type discoveryResult struct {
	uid    string
	reason string
	err    error
}

// The exporter accepts a whole gauge snapshot. Retain one observation per
// current Volume incarnation so concurrent workers cannot overwrite each other.
type discoveryObservations struct {
	mu      sync.Mutex
	members map[string]string
	results map[string]discoveryResult
	scanErr error
	observe func(map[string]int, error)
}

func newDiscoveryObservations(observe func(map[string]int, error)) *discoveryObservations {
	return &discoveryObservations{members: map[string]string{}, results: map[string]discoveryResult{}, observe: observe}
}

func (o *discoveryObservations) membership(volumes map[string]volumeapi.State) {
	o.mu.Lock()
	defer o.mu.Unlock()
	members := make(map[string]string, len(volumes))
	for id, state := range volumes {
		members[id] = state.UID
		if previous, exists := o.results[id]; exists && previous.uid != state.UID {
			delete(o.results, id)
		}
		if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" {
			o.results[id] = discoveryResult{uid: state.UID}
		}
	}
	for id := range o.results {
		if _, exists := members[id]; !exists {
			delete(o.results, id)
		}
	}
	o.members = members
}

func (o *discoveryObservations) record(id string, result discoveryResult) {
	if result.uid == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if uid, exists := o.members[id]; !exists || uid != result.uid {
		return // A late worker cannot resurrect a removed or replaced Volume.
	}
	o.results[id] = result
	o.publish()
}

func (o *discoveryObservations) recordBatch(results map[string]discoveryResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, result := range results {
		if uid, exists := o.members[id]; exists && uid == result.uid {
			o.results[id] = result
		}
	}
	o.publish()
}

func (o *discoveryObservations) scanned(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.scanErr = err
	o.publish()
}

// Called under mu, including the callback, to keep snapshot publication ordered.
func (o *discoveryObservations) publish() {
	if o.observe == nil {
		return
	}
	counts := map[string]int{}
	var failures []error
	failures = append(failures, o.scanErr)
	for id := range o.members {
		result, observed := o.results[id]
		if !observed {
			failures = append(failures, errors.New("Volume discovery not observed yet"))
			continue
		}
		if result.reason != "" {
			counts[result.reason]++
		}
		failures = append(failures, result.err)
	}
	o.observe(counts, errors.Join(failures...))
}
