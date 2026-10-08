package nodeexecutor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
)

func TestCreationObservationExcludesNestedAuthority(t *testing.T) {
	var observed = map[string]time.Duration{}
	node := &Node{ObserveStep: func(step string, elapsed time.Duration) { observed[step] += elapsed }}
	now := time.Unix(0, 0)
	o := creationObservation{node: node, volumeID: testID, now: func() time.Time { return now }}
	failure := errors.New("authority changed")
	if err := o.local("prepare", func() error {
		now = now.Add(time.Second)
		for range 3 {
			if err := o.authorize(func() error { now = now.Add(10 * time.Second); return nil }); err != nil {
				return err
			}
		}
		now = now.Add(time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := o.local("receipt", func() error {
		now = now.Add(2 * time.Second)
		return o.authorize(func() error { now = now.Add(5 * time.Second); return failure })
	}); !errors.Is(err, failure) {
		t.Fatalf("authority error changed: %v", err)
	}
	if observed["prepare"] != 2*time.Second || observed["receipt"] != 2*time.Second || observed["node_create_authority"] != 35*time.Second {
		t.Fatalf("nested or previous authority leaked into local durations: %v", observed)
	}
}

func TestEffectObservationReportsCanceledGateWithoutReadingIntent(t *testing.T) {
	_, node, dynamic, _, _ := fixture(t)
	unlock, err := node.gate.Lock(context.Background(), testID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	before := len(dynamic.Actions())
	var steps []string
	node.ObserveStep = func(step string, _ time.Duration) { steps = append(steps, step) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = node.Execute(ctx, &protocol.EffectRequest{ExecutorUid: node.Identity.PodUID, NodeName: node.Identity.NodeName, VolumeName: testID}, protocol.Operation_CREATE)
	if !errors.Is(err, context.Canceled) || len(dynamic.Actions()) != before || len(steps) != 1 || steps[0] != "node_effect_lock_wait" {
		t.Fatalf("canceled gate proceeded: err=%v actions=%d steps=%v", err, len(dynamic.Actions())-before, steps)
	}
}
