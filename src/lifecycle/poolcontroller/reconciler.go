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
	ApprovePoolRetirement(context.Context, string, string) error
}

type SafetyChecker interface {
	CheckPoolDeleteAfter(context.Context, string, types.UID, time.Time) (uninstallcheck.Report, error)
	CheckPoolRetirement(context.Context, string, types.UID) (uninstallcheck.Report, error)
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
	Observe   func(volumeapi.Pool, Observation)
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
		if pool.RetirementRequested() && !pool.RegistrationRetired() && pool.RegistrationApproved() && !quiescing && slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) {
			observation := r.reconcileRetirement(ctx, pool)
			result = errors.Join(result, observation.Err)
			r.observe(pool, observation)
			continue
		}
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
		r.observe(pool, observation)
	}
	return result
}

func (r *Reconciler) reconcileRetirement(ctx context.Context, pool volumeapi.Pool) Observation {
	unlock := r.PoolLocks.Lock(pool.UID)
	defer unlock()
	report, err := r.Safety.CheckPoolRetirement(ctx, pool.Name, types.UID(pool.UID))
	if err != nil {
		return Observation{Stage: "RetirementCheckFailed", Err: err}
	}
	if !report.Safe() {
		return Observation{Stage: "RetirementBlocked", Blockers: report.Blockers}
	}
	return Observation{Stage: "RetirementApproval", Err: r.Pools.ApprovePoolRetirement(ctx, pool.Name, pool.UID)}
}

// reconcileDeletion owns the same UID fence as create/move admission. Its
// observation is delivered only after releasing that fence.
func (r *Reconciler) reconcileDeletion(ctx context.Context, pool volumeapi.Pool) Observation {
	unlock := r.PoolLocks.Lock(pool.UID)
	defer unlock()
	report, err := r.Safety.CheckPoolDeleteAfter(ctx, pool.Name, types.UID(pool.UID), pool.DeletionTimestamp.Time)
	if err != nil {
		return Observation{Stage: "SafetyCheckFailed", Err: fmt.Errorf("check deleting Pool %q: %w", pool.Name, err)}
	}
	if !report.Safe() {
		return Observation{Stage: "SafetyBlocked", Blockers: report.Blockers}
	}
	if pool.IdentityReleaseApproval != pool.UID {
		return Observation{Stage: "ReleaseApproval", Err: r.Pools.ApprovePoolIdentityRelease(ctx, pool.Name, pool.UID)}
	}
	released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if released == nil || released.Status != metav1.ConditionTrue || released.ObservedGeneration != pool.Generation {
		observation := Observation{Stage: "NodeReleasePending", Reason: "IdentityReleaseObservationMissing"}
		if released != nil {
			observation.Reason = released.Reason
			observation.ObservedGeneration = released.ObservedGeneration
			if released.ObservedGeneration != pool.Generation {
				observation.Reason = "IdentityReleaseObservationStale"
			}
		}
		return observation
	}
	return Observation{Stage: "RemoveProtection", Err: r.Pools.RemovePoolFinalizer(ctx, pool.Name, pool.UID)}
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
