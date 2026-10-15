package controller

import (
	"errors"
	"maps"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestDiscoveryObservationsAggregateAndRejectLateIncarnations(t *testing.T) {
	var counts map[string]int
	var observedErr error
	o := newDiscoveryObservations(func(current map[string]int, err error) { counts, observedErr = maps.Clone(current), err })
	volumes := map[string]volumeapi.State{
		"a": {UID: "a-uid", Phase: volumeapi.PhaseReady},
		"b": {UID: "b-uid", Phase: volumeapi.PhaseReady},
	}
	o.membership(volumes)
	o.scanned(nil)
	if observedErr == nil {
		t.Fatal("new unobserved membership reported complete discovery")
	}
	o.record("a", discoveryResult{uid: "a-uid", reason: "DisruptionBudgetDenied"})
	o.record("b", discoveryResult{uid: "b-uid", reason: "DisruptionBudgetDenied"})
	if observedErr != nil || counts["DisruptionBudgetDenied"] != 2 {
		t.Fatalf("one worker overwrote another: %v err=%v", counts, observedErr)
	}
	sentinel := errors.New("LIST unavailable")
	o.scanned(sentinel)
	if !errors.Is(observedErr, sentinel) || counts["DisruptionBudgetDenied"] != 2 {
		t.Fatalf("failed scan discarded known observations: %v err=%v", counts, observedErr)
	}
	delete(volumes, "a")
	volumes["b"] = volumeapi.State{UID: "b-replacement", Phase: volumeapi.PhaseReady}
	o.membership(volumes)
	o.scanned(nil)
	o.record("a", discoveryResult{uid: "a-uid", reason: "DisruptionBudgetDenied"})
	o.record("b", discoveryResult{uid: "b-uid", reason: "DisruptionBudgetDenied"})
	if len(counts) != 0 || observedErr == nil {
		t.Fatalf("late result resurrected removed/replaced membership: %v err=%v", counts, observedErr)
	}
	o.record("b", discoveryResult{uid: "b-replacement"})
	if len(counts) != 0 || observedErr != nil {
		t.Fatalf("fresh successful result did not recover metric: %v err=%v", counts, observedErr)
	}
}
