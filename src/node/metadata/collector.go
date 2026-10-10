//go:build linux || darwin

// Package metadata collects settled local journals after their API owners have
// disappeared. It never removes volume data or releases capacity holds.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const DefaultRetention = 7 * 24 * time.Hour

type Repository interface {
	InstallationID(context.Context) (string, error)
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
	Get(context.Context, string) (volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
}

type Collector struct {
	NodeName, HostRoot string
	Repository         Repository
	Retention          time.Duration
	Now                func() time.Time
	VerifyPool         func(context.Context, volumeapi.Pool) error
	Collect            func(context.Context, string, ownership.PoolIdentity, time.Time, time.Duration, func(context.Context, volume.CopyIdentity) (bool, error)) (int, error)
}

func (c *Collector) Run(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
			klog.Errorf("collect node metadata: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Collector) validate() error {
	if c == nil || c.Repository == nil || c.NodeName == "" || !filepath.IsAbs(c.HostRoot) || c.Retention < time.Hour {
		return fmt.Errorf("node metadata collector configuration is incomplete")
	}
	return nil
}

func (c *Collector) Reconcile(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	pools, err := c.Repository.ListPoolRegistrations(ctx)
	if err != nil {
		return err
	}
	installation, err := c.Repository.InstallationID(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	collect := c.Collect
	if collect == nil {
		collect = ownership.CollectMetadata
	}
	var result error
	for _, pool := range pools {
		if pool.NodeName != c.NodeName || pool.DeletionTimestamp != nil {
			continue
		}
		if decidePoolCollection(pool, now) != collectEligible {
			continue
		}
		if err := c.verify(ctx, pool); err != nil {
			result = errors.Join(result, err)
			continue
		}
		root := filepath.Join(c.HostRoot, strings.TrimPrefix(filepath.Clean(pool.MountPath), string(filepath.Separator)))
		removed, err := collect(ctx, root, ownership.PoolIdentity{InstallationID: installation, PoolUID: pool.UID}, now, c.Retention, func(ctx context.Context, target volume.CopyIdentity) (bool, error) {
			return c.unreferenced(ctx, pool, target)
		})
		if removed > 0 {
			klog.InfoS("Collected settled node metadata", "node", c.NodeName, "pool", pool.Name, "records", removed)
		}
		result = errors.Join(result, err)
	}
	return result
}

func (c *Collector) verify(ctx context.Context, expected volumeapi.Pool) error {
	pools, err := c.Repository.ListPoolRegistrations(ctx)
	if err != nil {
		return err
	}
	for _, pool := range pools {
		if pool.UID != expected.UID {
			continue
		}
		if pool.Name != expected.Name || pool.NodeName != c.NodeName || pool.MountPath != expected.MountPath || pool.Generation != expected.Generation || pool.DeletionTimestamp != nil {
			return ownership.ErrIdentity
		}
		now := time.Now()
		if c.Now != nil {
			now = c.Now()
		}
		if decidePoolCollection(pool, now) != collectEligible {
			return volumeapi.ErrPoolNotReady
		}
		if c.VerifyPool != nil {
			return c.VerifyPool(ctx, pool)
		}
		return readiness.VerifyHostPool(c.HostRoot, pool)
	}
	return ownership.ErrIdentity
}

func (c *Collector) unreferenced(ctx context.Context, pool volumeapi.Pool, target volume.CopyIdentity) (bool, error) {
	if target.NodeName != c.NodeName {
		return false, ownership.ErrIdentity
	}
	// Re-read authority for each candidate; no stale snapshot can permit GC.
	if err := c.verify(ctx, pool); err != nil {
		return false, err
	}
	_, err := c.Repository.Get(ctx, target.VolumeID)
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, err
	}
	if target.PoolUID != pool.UID {
		registrations, err := c.Repository.ListPoolRegistrations(ctx)
		if err != nil {
			return false, err
		}
		for _, registered := range registrations {
			if registered.UID == target.PoolUID {
				return false, nil
			}
		}
	}
	moves, err := c.Repository.ListMoves(ctx)
	if err != nil {
		return false, err
	}
	for _, move := range moves {
		if move.Spec.VolumeID == target.VolumeID {
			return false, nil
		}
	}
	return true, c.verify(ctx, pool)
}
