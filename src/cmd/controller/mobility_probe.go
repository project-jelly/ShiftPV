package main

import (
	"context"
	"errors"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	mobilitycontroller "github.com/project-jelly/ShiftPV/src/mobility/controller"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/measurement"
	"github.com/project-jelly/ShiftPV/src/pool/measurement/remote"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const mobilityProbeTimeout = 4 * time.Second

// Mobility uses resident readers with one budget for discovery, transport and
// final validation. Its private locks and fallback never wait on a helper Pod.
func newMobilityCapacityProbe(pools *volumeapi.Registry, discovery remote.Discovery, rpc remote.RPC, observe func(string, time.Duration)) *mobilityCapacityProbe {
	api := &measurement.Client{Pools: pools, NonBlocking: true}
	var probe mobilitycontroller.CapacityProbe = api
	if rpc != nil {
		probe = &remote.Client{Pools: pools, Discovery: discovery, RPC: rpc, Fallback: api,
			NonBlocking: true, FallbackOnUnavailable: true}
	}
	return &mobilityCapacityProbe{probe: probe, timeout: mobilityProbeTimeout, observe: observe}
}

type mobilityCapacityProbe struct {
	probe   mobilitycontroller.CapacityProbe
	timeout time.Duration
	observe func(string, time.Duration)
}

func (p *mobilityCapacityProbe) StatFSForPool(ctx context.Context, pool volumeapi.Pool) (capacity.Filesystem, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	started := time.Now()
	defer func() {
		if p.observe != nil {
			p.observe("live_capacity_probe", time.Since(started))
		}
	}()
	stats, err := p.probe.StatFSForPool(ctx, pool)
	// A late successful response cannot extend this call's authority.
	if ctx.Err() != nil {
		return capacity.Filesystem{}, errors.Join(mobilitycontroller.ErrCapacityProbePending, ctx.Err(), err)
	}
	if err != nil {
		var retryable interface{ Retryable() bool }
		if errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
			(errors.As(err, &retryable) && retryable.Retryable()) {
			err = errors.Join(mobilitycontroller.ErrCapacityProbePending, err)
		}
		return capacity.Filesystem{}, err
	}
	return stats, nil
}
