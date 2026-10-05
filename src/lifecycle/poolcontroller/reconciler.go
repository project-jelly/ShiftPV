package poolcontroller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

type Repository interface {
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
	EnsurePoolFinalizer(context.Context, string, string) error
	RemovePoolFinalizer(context.Context, string, string) error
	ApprovePoolIdentityRelease(context.Context, string, string) error
}

type SafetyChecker interface {
	CheckPoolDeleteAfter(context.Context, string, types.UID, time.Time) (uninstallcheck.Report, error)
}

type QuiesceState interface {
	Quiescing(context.Context) (string, bool, error)
}

type Reconciler struct {
	Pools     Repository
	Safety    SafetyChecker
	Quiesce   QuiesceState
	PoolLocks *poolcapacity.Locker
	Interval  time.Duration
}

func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	r.reconcileAndLog(ctx)
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.reconcileAndLog(ctx)
		}
	}
}

func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	pools, err := r.Pools.ListPoolRegistrations(ctx)
	if err != nil {
		return err
	}
	_, quiescing, err := r.Quiesce.Quiescing(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, pool := range pools {
		if pool.DeletionTimestamp == nil {
			if !quiescing && !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) {
				result = errors.Join(result, r.Pools.EnsurePoolFinalizer(ctx, pool.Name, pool.UID))
			}
			continue
		}
		if !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) {
			continue
		}
		unlock := r.PoolLocks.Lock(pool.UID)
		report, checkErr := r.Safety.CheckPoolDeleteAfter(ctx, pool.Name, types.UID(pool.UID), pool.DeletionTimestamp.Time)
		if checkErr != nil {
			unlock()
			result = errors.Join(result, fmt.Errorf("check deleting Pool %q: %w", pool.Name, checkErr))
			continue
		}
		if !report.Safe() {
			unlock()
			continue
		}
		if pool.IdentityReleaseApproval != pool.UID {
			result = errors.Join(result, r.Pools.ApprovePoolIdentityRelease(ctx, pool.Name, pool.UID))
			unlock()
			continue
		}
		released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased)
		if released == nil || released.Status != metav1.ConditionTrue || released.ObservedGeneration != pool.Generation {
			unlock()
			continue
		}
		result = errors.Join(result, r.Pools.RemovePoolFinalizer(ctx, pool.Name, pool.UID))
		unlock()
	}
	return result
}

func (r *Reconciler) reconcileAndLog(ctx context.Context) {
	if err := r.ReconcileAll(ctx); err != nil && !errors.Is(err, context.Canceled) {
		klog.Errorf("reconcile ShiftPVPool lifecycle: %v", err)
	}
}

func (r *Reconciler) validate() error {
	if r == nil || r.Pools == nil || r.Safety == nil || r.Quiesce == nil || r.PoolLocks == nil || r.Interval <= 0 {
		return fmt.Errorf("Pool lifecycle reconciler configuration is incomplete")
	}
	return nil
}
