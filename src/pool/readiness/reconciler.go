package readiness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type Repository interface {
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
	SetPoolStatus(context.Context, string, string, string, volumeapi.PoolStatus) error
}

type Reconciler struct {
	reconcileMu sync.Mutex
	// Metrics snapshots retain peers across a targeted retry. Never used as
	// storage authority; every reconciliation still reads live registrations.
	lastObservations map[string]Observation
	NodeName         string
	Pools            Repository
	Inspector        Inspector
	Interval         time.Duration
	Clock            clock.WithTicker
	// PoolTimeout bounds context-aware inventory, release and API work for
	// each Pool. It cannot interrupt synchronous host filesystem syscalls.
	PoolTimeout time.Duration
	Wake        <-chan struct{}
	Now         func() time.Time
	Observe     func(volumeapi.Pool, Result, error)
	ObserveAll  func([]Observation, error)
	Inventory   func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory
	Release     func(context.Context, volumeapi.Pool) error
}

const defaultPoolTimeout = 5 * time.Second

func NewReconciler(nodeName string, pools Repository, inspector Inspector, interval time.Duration) (*Reconciler, error) {
	r := &Reconciler{NodeName: nodeName, Pools: pools, Inspector: inspector, Interval: interval}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r, nil
}

// Observation is one Pool's result from a node reconciliation pass.
type Observation struct {
	Pool   volumeapi.Pool
	Result Result
	Err    error
}

func (r *Reconciler) Reconcile(ctx context.Context) error {
	_, err := r.reconcile(ctx, nil)
	return err
}

// only selects scheduled retries by Pool UID; nil performs a complete pass.
func (r *Reconciler) reconcile(ctx context.Context, only map[string]bool) (retry map[string]bool, reconcileErr error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
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
		return nil, err
	}
	retry = make(map[string]bool)
	isolation := r.capacityIsolation(pools)
	observed := false
	for _, pool := range pools {
		if ctx.Err() != nil {
			return retry, errors.Join(reconcileErr, ctx.Err())
		}
		if pool.NodeName != r.NodeName || (only != nil && !only[pool.UID]) {
			continue
		}
		observed = true
		timeout := r.PoolTimeout
		if timeout == 0 {
			timeout = defaultPoolTimeout
		}
		poolCtx, cancel := context.WithTimeout(ctx, timeout)
		result, err := r.reconcilePool(poolCtx, pool, isolation[pool.UID])
		cancel()
		observations = append(observations, Observation{Pool: pool, Result: result, Err: err})
		retry[pool.UID] = result.RetryInventory
		reconcileErr = errors.Join(reconcileErr, err)
	}
	if !observed && r.Observe != nil {
		r.Observe(volumeapi.Pool{}, Result{}, nil)
	}
	if r.ObserveAll != nil {
		observations = r.observationSnapshot(pools, observations)
	}
	return retry, reconcileErr
}

func (r *Reconciler) observationSnapshot(pools []volumeapi.Pool, changed []Observation) []Observation {
	if r.lastObservations == nil {
		r.lastObservations = map[string]Observation{}
	}
	for _, observation := range changed {
		r.lastObservations[observation.Pool.UID] = observation
	}
	live := make(map[string]Observation)
	snapshot := make([]Observation, 0, len(pools))
	for _, pool := range pools {
		if pool.NodeName != r.NodeName {
			continue
		}
		observation, ok := r.lastObservations[pool.UID]
		if !ok || observation.Pool.Generation != pool.Generation {
			observation = Observation{Pool: pool}
		}
		live[pool.UID] = observation
		snapshot = append(snapshot, observation)
	}
	r.lastObservations = live
	return snapshot
}

func (r *Reconciler) reconcilePool(ctx context.Context, pool volumeapi.Pool, allocation allocationObservation) (result Result, reconcileErr error) {
	if r.Observe != nil {
		defer func() { r.Observe(pool, result, reconcileErr) }()
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	if skipRegistrationProbe(pool, allocation) {
		result = Result{Independent: allocation.check, Accessible: skipped(), Writable: skipped(), CapacityReadable: skipped()}
	} else {
		candidate := pool
		if candidate.Status.CapacityUnit == nil {
			candidate.Status.CapacityUnit = allocation.unit
		}
		result = r.Inspector.Inspect(candidate)
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
	if !pool.RegistrationApproved() && status.RegistrationApproved {
		if err := r.recheckRegistration(ctx, pool, &result); err != nil {
			return result, err
		}
		if !result.Independent.OK && result.Independent.Reason != "" {
			status, cleanupReady = r.observedStatus(ctx, pool, result, now)
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.RetryInventory = needsInventoryRetry(status.Inventory)
	releaseErr := r.recordIdentityRelease(ctx, pool, &status, cleanupReady, now)
	if err := ctx.Err(); err != nil {
		return result, errors.Join(err, releaseErr)
	}
	return result, errors.Join(r.Pools.SetPoolStatus(ctx, pool.Name, pool.UID, r.NodeName, status), releaseErr)
}

// observedStatus records one node observation. A terminating Pool may remain
// cleanup-ready even though it is no longer eligible for new placement.
func (r *Reconciler) observedStatus(ctx context.Context, pool volumeapi.Pool, result Result, now time.Time) (volumeapi.PoolStatus, bool) {
	status := pool.Status
	status.Conditions = append([]metav1.Condition(nil), pool.Status.Conditions...)
	status.RegistrationApproved = pool.RegistrationApproved()
	status.CapacityProbeSupported = true
	status.ObservedGeneration = pool.Generation
	status.LastProbeTime = metav1.NewTime(now)
	status.FilesystemTotalBytes = 0
	if result.CapacityReadable.OK {
		status.FilesystemTotalBytes = result.Filesystem.TotalBytes
	}
	for _, condition := range conditions(result, pool.Generation, now) {
		meta.SetStatusCondition(&status.Conditions, condition)
	}
	ready := meta.FindStatusCondition(status.Conditions, volumeapi.PoolConditionReady)
	cleanupReady := ready != nil && ready.Status == metav1.ConditionTrue
	configuration := pool.ConfigurationCheck()
	if !configuration.OK {
		meta.SetStatusCondition(&status.Conditions, condition(volumeapi.PoolConditionReady, configuration, pool.Generation, now))
	} else if pool.DeletionTimestamp != nil && cleanupReady {
		meta.SetStatusCondition(&status.Conditions, condition(volumeapi.PoolConditionReady, Check{
			Known: true, Reason: "PoolDeregistering", Message: "Pool rejects new placement while deregistration converges",
		}, pool.Generation, now))
	}
	status.Inventory = r.observedInventory(ctx, pool, result, status, now)
	if cleanupReady && completeInventory(status.Inventory) {
		if pool.DeletionTimestamp == nil && configuration.OK {
			status.RegistrationApproved = true
		}
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
	if pool.DeletingUnapprovedFixedBlock() && r.Inventory != nil {
		inventory := r.Inventory(ctx, pool, now)
		return &inventory
	}
	if invalidRegistrationCandidate(pool) {
		return &volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Message: "ConfigurationUnavailable: " + pool.ConfigurationCheck().Reason}
	}
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
	candidate.Status.RegistrationApproved = status.RegistrationApproved
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
	} else if (cleanupReady || pool.DeletingUnapprovedFixedBlock()) && emptyInventory(status.Inventory) {
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
			if pool.DeletingUnapprovedFixedBlock() {
				release.Reason = "PoolRegistrationCanceled"
				release.Message = "unapproved registration has no storage metadata, managed data, or published target"
			}
		}
	}
	meta.SetStatusCondition(&status.Conditions, condition(volumeapi.PoolConditionIdentityReleased, release, pool.Generation, now))
	return releaseErr
}

func emptyInventory(inventory *volumeapi.PoolInventory) bool {
	return inventory != nil && inventory.Valid && !inventory.Truncated && inventory.Message == "" && len(inventory.Copies) == 0
}

func (r *Reconciler) validate() error {
	if r == nil || r.NodeName == "" || r.Pools == nil || r.Inspector == nil || r.Interval <= 0 || r.PoolTimeout < 0 {
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
