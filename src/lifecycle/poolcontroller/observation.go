package poolcontroller

import (
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	"k8s.io/klog/v2"
)

type DeletionObservation struct {
	Stage              string
	Reason             string
	ObservedGeneration int64
	Blockers           []uninstallcheck.Blocker
	Err                error
}

func (r *Reconciler) observeDeletion(pool volumeapi.Pool, observation DeletionObservation) {
	if r.ObserveDelete != nil {
		r.ObserveDelete(pool, observation)
	}
	if !klog.V(2).Enabled() {
		return
	}
	blockers := observation.Blockers
	if len(blockers) > 5 {
		blockers = blockers[:5]
	}
	klog.V(2).InfoS("ShiftPVPool deletion progress", "pool", pool.Name, "poolUID", pool.UID,
		"stage", observation.Stage, "reason", observation.Reason, "generation", pool.Generation,
		"observedGeneration", observation.ObservedGeneration, "blockerCount", len(observation.Blockers),
		"blockers", blockers, "error", observation.Err)
}
