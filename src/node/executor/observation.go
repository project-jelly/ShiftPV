package executor

import (
	"context"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/klog/v2"
)

func (n *Node) observeStep(step, volumeID string) func() {
	started := time.Now()
	return func() { n.reportStep(step, volumeID, time.Since(started)) }
}

func (n *Node) reportStep(step, volumeID string, elapsed time.Duration) {
	klog.V(2).InfoS("Node provisioning step completed", "step", step, "volumeID", volumeID, "duration", elapsed)
	if n.ObserveStep != nil {
		n.ObserveStep(step, elapsed)
	}
}

func (n *Node) lockEffect(ctx context.Context, volumeID string) (func(), error) {
	defer n.observeStep("node_effect_lock_wait", volumeID)()
	return n.gate.Lock(ctx, volumeID)
}

func (n *Node) readEffectState(ctx context.Context, volumeID string) (volumeapi.State, error) {
	defer n.observeStep("node_effect_intent_read", volumeID)()
	return n.Volumes.Get(ctx, volumeID)
}

// Ownership invokes authority callbacks synchronously. Each creation keeps its
// own total so local timing excludes API and backing checks inside those calls.
type creationObservation struct {
	node      *Node
	volumeID  string
	now       func() time.Time
	authority time.Duration
}

func (o *creationObservation) authorize(check func() error) error {
	started := o.now()
	err := check()
	elapsed := o.now().Sub(started)
	o.authority += elapsed
	o.node.reportStep("node_create_authority", o.volumeID, elapsed)
	return err
}

func (o *creationObservation) local(step string, action func() error) error {
	before, started := o.authority, o.now()
	err := action()
	elapsed := o.now().Sub(started) - (o.authority - before)
	o.node.reportStep(step, o.volumeID, elapsed)
	return err
}
