package readiness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type Repository interface {
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
	SetPoolStatus(context.Context, string, string, string, volumeapi.PoolStatus) error
}

type Reconciler struct {
	NodeName   string
	Pools      Repository
	Inspector  Inspector
	Interval   time.Duration
	Wake       <-chan struct{}
	Now        func() time.Time
	Observe    func(volumeapi.Pool, Result, error)
	ObserveAll func([]Observation, error)
	Inventory  func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory
	Release    func(context.Context, volumeapi.Pool) error
}

// Observation is one Pool's result from a node reconciliation pass.
type Observation struct {
	Pool   volumeapi.Pool
	Result Result
	Err    error
}

func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := r.reconcileAndLog(ctx); err != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.Wake:
		}
		if err := r.reconcileAndLog(ctx); err != nil && errors.Is(err, context.Canceled) {
			return nil
		}
	}
}

func (r *Reconciler) Reconcile(ctx context.Context) (reconcileErr error) {
	if err := r.validate(); err != nil {
		return err
	}
	var observations []Observation
	if r.ObserveAll != nil {
		defer func() { r.ObserveAll(observations, reconcileErr) }()
	}
	pools, err := r.Pools.ListPoolRegistrations(ctx)
	if err != nil {
		if r.Observe != nil {
			r.Observe(volumeapi.Pool{}, Result{}, err)
		}
		return err
	}
	isolation := r.capacityIsolation(pools)
	observed := false
	for _, pool := range pools {
		if pool.NodeName != r.NodeName {
			continue
		}
		observed = true
		result, err := r.reconcilePool(ctx, pool, isolation[pool.UID])
		observations = append(observations, Observation{Pool: pool, Result: result, Err: err})
		reconcileErr = errors.Join(reconcileErr, err)
	}
	if !observed && r.Observe != nil {
		r.Observe(volumeapi.Pool{}, Result{}, nil)
	}
	return reconcileErr
}

func (r *Reconciler) reconcilePool(ctx context.Context, pool volumeapi.Pool, allocation allocationObservation) (result Result, reconcileErr error) {
	if r.Observe != nil {
		defer func() { r.Observe(pool, result, reconcileErr) }()
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	if allocation.check.Reason != "" && !allocation.check.OK {
		result = Result{Independent: allocation.check, Accessible: skipped(), Writable: skipped(), CapacityReadable: skipped()}
	} else {
		result = r.Inspector.Inspect(pool)
		if allocation.unit != nil && !volumeapi.SameCapacityUnit(allocation.unit, result.CapacityUnit) {
			result.Independent = capacityFailure("CapacityIdentityChanged", fmt.Errorf("capacity allocation changed during node observation"))
		}
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && !result.Mounted.Known && result.Mounted.Reason == "" {
		result.Mounted = skipped()
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && result.Mounted.OK && result.MountIdentity == nil {
		result.Mounted = Check{Known: true, Reason: "MountIdentityMissing", Message: "mount probe did not return an identity"}
	}
	status, cleanupReady := r.observedStatus(ctx, pool, result, now)
	releaseErr := r.recordIdentityRelease(ctx, pool, &status, cleanupReady, now)
	return result, errors.Join(r.Pools.SetPoolStatus(ctx, pool.Name, pool.UID, r.NodeName, status), releaseErr)
}

// observedStatus records one node observation. A terminating Pool may remain
// cleanup-ready even though it is no longer eligible for new placement.
func (r *Reconciler) observedStatus(ctx context.Context, pool volumeapi.Pool, result Result, now time.Time) (volumeapi.PoolStatus, bool) {
	status := pool.Status
	status.ObservedGeneration = pool.Generation
	status.LastProbeTime = metav1.NewTime(now)
	for _, condition := range conditions(result, pool.Generation, now) {
		meta.SetStatusCondition(&status.Conditions, condition)
	}
	ready := meta.FindStatusCondition(status.Conditions, volumeapi.PoolConditionReady)
	cleanupReady := ready != nil && ready.Status == metav1.ConditionTrue
	if pool.DeletionTimestamp != nil && cleanupReady {
		meta.SetStatusCondition(&status.Conditions, condition(volumeapi.PoolConditionReady, Check{
			Known: true, Reason: "PoolDeregistering", Message: "Pool rejects new placement while deregistration converges",
		}, pool.Generation, now))
	}
	status.Inventory = r.observedInventory(ctx, pool, result, status, now)
	if cleanupReady && completeInventory(status.Inventory) {
		if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && status.MountIdentity == nil {
			status.MountIdentity = result.MountIdentity
		}
		if pool.CapacityPolicy == volumeapi.PoolCapacityPolicyFixedBlock && status.CapacityUnit == nil && result.Independent.OK {
			status.CapacityUnit = result.CapacityUnit
		}
	}
	meta.RemoveStatusCondition(&status.Conditions, volumeapi.PoolConditionIdentityReleased)
	return status, cleanupReady
}

func (r *Reconciler) observedInventory(ctx context.Context, pool volumeapi.Pool, result Result, status volumeapi.PoolStatus, now time.Time) *volumeapi.PoolInventory {
	if result.Independent.Reason != "" && !result.Independent.OK {
		return &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Message: "CapacityUnavailable: " + result.Independent.Reason}
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && !result.Mounted.OK {
		return &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Message: "MountUnavailable: " + result.Mounted.Reason}
	}
	if r.Inventory == nil {
		return status.Inventory
	}
	candidate := pool
	if candidate.Status.MountIdentity == nil {
		candidate.Status.MountIdentity = result.MountIdentity
	}
	if candidate.Status.CapacityUnit == nil {
		candidate.Status.CapacityUnit = result.CapacityUnit
	}
	candidate.Status.Conditions = status.Conditions
	inventory := r.Inventory(ctx, candidate, now)
	return &inventory
}

func completeInventory(inventory *volumeapi.PoolInventory) bool {
	return inventory != nil && inventory.Valid && !inventory.Truncated && inventory.Message == ""
}

// recordIdentityRelease performs only the exact empty Pool release approved
// by the controller, then records the node's release evidence in status.
func (r *Reconciler) recordIdentityRelease(ctx context.Context, pool volumeapi.Pool, status *volumeapi.PoolStatus, cleanupReady bool, now time.Time) error {
	if pool.DeletionTimestamp == nil {
		return nil
	}
	release := Check{Known: true, Reason: "PoolIdentityRetained", Message: "Pool identity remains until inventory is empty and complete"}
	var releaseErr error
	if pool.IdentityReleaseApproval != pool.UID {
		release.Reason = "PoolIdentityReleasePending"
		release.Message = "Pool identity remains until the controller approves exact release"
	} else if cleanupReady && emptyInventory(status.Inventory) {
		if r.Release == nil {
			release.Reason = "PoolIdentityReleaseUnavailable"
			release.Message = "node Pool identity release is not configured"
		} else if err := r.Release(ctx, pool); err != nil {
			releaseErr = err
			release.Reason = "PoolIdentityReleaseFailed"
			release.Message = err.Error()
		} else {
			release.OK = true
			release.Reason = "PoolIdentityReleased"
			release.Message = "exact empty Pool identity was released"
		}
	}
	meta.SetStatusCondition(&status.Conditions, condition(volumeapi.PoolConditionIdentityReleased, release, pool.Generation, now))
	return releaseErr
}

func emptyInventory(inventory *volumeapi.PoolInventory) bool {
	return inventory != nil && inventory.Valid && !inventory.Truncated && inventory.Message == "" && len(inventory.Copies) == 0
}

func (r *Reconciler) reconcileAndLog(ctx context.Context) error {
	err := r.Reconcile(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		klog.Errorf("reconcile ShiftPVPool readiness on node %s: %v", r.NodeName, err)
	}
	return err
}

func (r *Reconciler) validate() error {
	if r.NodeName == "" || r.Pools == nil || r.Inspector == nil || r.Interval <= 0 {
		return fmt.Errorf("Pool readiness reconciler configuration is incomplete")
	}
	return nil
}

func conditions(result Result, generation int64, now time.Time) []metav1.Condition {
	accessible := condition(volumeapi.PoolConditionAccessible, result.Accessible, generation, now)
	writable := condition(volumeapi.PoolConditionWritable, result.Writable, generation, now)
	capacity := condition(volumeapi.PoolConditionCapacityReadable, result.CapacityReadable, generation, now)
	readyCheck := Check{OK: true, Known: true, Reason: "PoolReady", Message: "Pool directory is accessible, writable, and capacity-readable"}
	checks := []Check{}
	if result.Independent.Reason != "" {
		checks = append(checks, result.Independent)
	}
	checks = append(checks, result.Accessible)
	conditions := []metav1.Condition{accessible}
	if result.Independent.Reason != "" {
		conditions = append(conditions, condition(volumeapi.PoolConditionCapacityIndependent, result.Independent, generation, now))
	}
	if result.Mounted.Reason != "" {
		checks = append(checks, result.Mounted)
		conditions = append(conditions, condition(volumeapi.PoolConditionMounted, result.Mounted, generation, now))
	}
	checks = append(checks, result.Writable, result.CapacityReadable)
	conditions = append(conditions, writable, capacity)
	for _, candidate := range checks {
		if !candidate.Known || !candidate.OK {
			readyCheck.OK = false
			readyCheck.Reason = candidate.Reason
			readyCheck.Message = candidate.Message
			break
		}
	}
	return append(conditions, condition(volumeapi.PoolConditionReady, readyCheck, generation, now))
}

func condition(conditionType string, check Check, generation int64, now time.Time) metav1.Condition {
	status := metav1.ConditionUnknown
	if check.Known {
		status = metav1.ConditionFalse
		if check.OK {
			status = metav1.ConditionTrue
		}
	}
	return metav1.Condition{
		Type: conditionType, Status: status, ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(now), Reason: check.Reason, Message: check.Message,
	}
}
