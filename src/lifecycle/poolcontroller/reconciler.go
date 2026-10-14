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
	Pools         Repository
	Safety        SafetyChecker
	Quiesce       QuiesceState
	PoolLocks     *poolcapacity.Locker
	Interval      time.Duration
	ObserveDelete func(volumeapi.Pool, DeletionObservation)
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
		observation := r.reconcileDeletion(ctx, pool)
		result = errors.Join(result, observation.Err)
		r.observeDeletion(pool, observation)
	}
	return result
}

// reconcileDeletion owns the same UID fence as create/move admission. Its
// observation is delivered only after releasing that fence.
func (r *Reconciler) reconcileDeletion(ctx context.Context, pool volumeapi.Pool) DeletionObservation {
	unlock := r.PoolLocks.Lock(pool.UID)
	defer unlock()
	report, err := r.Safety.CheckPoolDeleteAfter(ctx, pool.Name, types.UID(pool.UID), pool.DeletionTimestamp.Time)
	if err != nil {
		return DeletionObservation{Stage: "SafetyCheckFailed", Err: fmt.Errorf("check deleting Pool %q: %w", pool.Name, err)}
	}
	if !report.Safe() {
		return DeletionObservation{Stage: "SafetyBlocked", Blockers: report.Blockers}
	}
	if pool.IdentityReleaseApproval != pool.UID {
		return DeletionObservation{Stage: "ReleaseApproval", Err: r.Pools.ApprovePoolIdentityRelease(ctx, pool.Name, pool.UID)}
	}
	released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if released == nil || released.Status != metav1.ConditionTrue || released.ObservedGeneration != pool.Generation {
		observation := DeletionObservation{Stage: "NodeReleasePending", Reason: "IdentityReleaseObservationMissing"}
		if released != nil {
			observation.Reason = released.Reason
			observation.ObservedGeneration = released.ObservedGeneration
			if released.ObservedGeneration != pool.Generation {
				observation.Reason = "IdentityReleaseObservationStale"
			}
		}
		return observation
	}
	return DeletionObservation{Stage: "RemoveProtection", Err: r.Pools.RemovePoolFinalizer(ctx, pool.Name, pool.UID)}
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
