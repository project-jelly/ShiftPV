package main

import (
	"context"
	"time"

	"github.com/project-jelly/ShiftPV/src/cmd/internal/wiring"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/lifecycle/creationcontroller"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"github.com/project-jelly/ShiftPV/src/provisioning/reclaim"
	"k8s.io/client-go/rest"
)

// Retirement settles existing work, including while provisioning is quiesced.
// Its API budget and non-waiting effect adapter are independent of CSI and
// admission, so a lost Node cannot hold either request path's budget or worker.
func newCreationRetirement(cfg config, config *rest.Config, fail wiring.Fail) *creationcontroller.Reconciler {
	background := rest.CopyConfig(config)
	background.QPS, background.Burst = 5, 10
	background.RateLimiter = wiring.NewAPIBudget(background.QPS, background.Burst)
	background.Timeout = 0
	background.UserAgent = "shiftpv-creation-retirement"
	clients := wiring.ForConfig(background, "creation retirement", fail)
	volumes := &volumeapi.Registry{Client: clients.Dynamic, PoolReadinessStaleAfter: cfg.poolReadinessStaleAfter}
	inspector := &helperpod.Inspector{Client: clients.Typed, Namespace: cfg.namespace}
	effects := &nodeexecutor.Client{
		Discovery: nodeexecutor.Discovery{Client: clients.Typed, Namespace: cfg.namespace, DaemonSet: cfg.nodeDaemonSet},
		Volumes:   volumes, Inspector: inspector,
	}
	settler := &reclaim.Settler{Store: &cleanupapi.Store{Client: clients.Dynamic},
		Reclaim: func(ctx context.Context, current cleanupapi.Cleanup, journal cleanupapi.Journal) (cleanupapi.Cleanup, error) {
			next, _, err := effects.AdvanceReclaim(ctx, current, journal)
			return next, err
		},
	}
	return &creationcontroller.Reconciler{
		Volumes: volumes, Claims: creation.RetirementInspector{Reader: creation.KubernetesRetirementReader{Client: clients.Typed}},
		Cancel:   &creation.Canceller{Volumes: volumes, Inspector: inspector, Cleanups: settler},
		Interval: 10 * time.Second, WorkTimeout: 10 * time.Second, Workers: 2,
	}
}
