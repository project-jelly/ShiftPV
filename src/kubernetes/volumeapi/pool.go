package volumeapi

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/volume"
)

type Pool struct {
	Name                    string
	UID                     string
	NodeName                string
	MountPath               string
	CapacityLimit           string
	Generation              int64
	DeletionTimestamp       *metav1.Time
	Finalizers              []string
	IdentityReleaseApproval string
	Status                  PoolStatus
}

type PoolStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	LastProbeTime      metav1.Time        `json:"lastProbeTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Inventory          *PoolInventory     `json:"inventory,omitempty"`
}

type PoolInventory struct {
	ObservedAt metav1.Time       `json:"observedAt"`
	Valid      bool              `json:"valid"`
	Truncated  bool              `json:"truncated,omitempty"`
	Message    string            `json:"message,omitempty"`
	Copies     []CopyObservation `json:"copies,omitempty"`
}

type CopyObservation struct {
	Marker    string               `json:"marker"`
	Identity  *volume.CopyIdentity `json:"identity,omitempty"`
	Present   bool                 `json:"present"`
	Published bool                 `json:"published,omitempty"`
	Problem   string               `json:"problem,omitempty"`
}

func (r *Registry) Pools(ctx context.Context) ([]Pool, error) {
	pools, err := r.ListPools(ctx)
	if err == nil && len(pools) == 0 {
		return nil, fmt.Errorf("%w: no ShiftPVPool nodes are registered", ErrPoolConfiguration)
	}
	return pools, err
}

// ListPools permits an empty registry for read-only inventory.
func (r *Registry) ListPools(ctx context.Context) ([]Pool, error) {
	pools, err := r.ListPoolRegistrations(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		if pool.NodeName == "" || !filepath.IsAbs(pool.MountPath) || pool.MountPath == "/" {
			return nil, fmt.Errorf("%w: ShiftPVPool %q has invalid nodeName or mountPath", ErrPoolConfiguration, pool.Name)
		}
		if _, duplicate := nodes[pool.NodeName]; duplicate {
			return nil, duplicatePoolError(pool.NodeName)
		}
		nodes[pool.NodeName] = struct{}{}
	}
	sort.Slice(pools, func(left, right int) bool { return pools[left].NodeName < pools[right].NodeName })
	return pools, nil
}

// ListPoolRegistrations returns each Pool incarnation independently. Lifecycle
// protection uses this view so one invalid or duplicate registration cannot
// prevent finalizers from being installed on every object.
func (r *Registry) ListPoolRegistrations(ctx context.Context) ([]Pool, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	list, err := r.Client.Resource(PoolResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ShiftPVPool: %w", err)
	}
	result := make([]Pool, 0, len(list.Items))
	for index := range list.Items {
		pool, poolErr := poolFrom(&list.Items[index])
		if poolErr != nil {
			return nil, poolErr
		}
		result = append(result, pool)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result, nil
}

// PoolSnapshot keeps registration and placement readiness on the same API read.
type PoolSnapshot struct {
	Registered []Pool
	Ready      []Pool
}

func (r *Registry) ObservePools(ctx context.Context) (PoolSnapshot, error) {
	pools, err := r.Pools(ctx)
	if err != nil {
		return PoolSnapshot{}, err
	}
	now, staleAfter := r.readiness()
	snapshot := PoolSnapshot{Registered: pools}
	for _, pool := range pools {
		if placeable, _ := poolReady(pool, now, staleAfter); placeable {
			snapshot.Ready = append(snapshot.Ready, pool)
		}
	}
	return snapshot, nil
}

func (r *Registry) ReadyPools(ctx context.Context) ([]Pool, error) {
	snapshot, err := r.ObservePools(ctx)
	return snapshot.Ready, err
}

func (r *Registry) PoolNodes(ctx context.Context) ([]string, error) {
	// Accessible topology is the durable mobility universe encoded into the PV.
	// Keep every registered Pool here even if one is temporarily not Ready;
	// current readiness is enforced when selecting a provisioning or move target.
	pools, err := r.Pools(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		nodes[pool.NodeName] = struct{}{}
	}
	result := make([]string, 0, len(nodes))
	for node := range nodes {
		result = append(result, node)
	}
	sort.Strings(result)
	return result, nil
}

func (r *Registry) PoolForNode(ctx context.Context, nodeName string) (Pool, error) {
	if nodeName == "" {
		return Pool{}, fmt.Errorf("%w: node name is required", ErrPoolConfiguration)
	}
	pools, err := r.Pools(ctx)
	if err != nil {
		return Pool{}, err
	}
	var result Pool
	for _, pool := range pools {
		if pool.NodeName != nodeName {
			continue
		}
		if result.NodeName != "" {
			return Pool{}, duplicatePoolError(nodeName)
		}
		result = pool
	}
	if result.NodeName == "" {
		return Pool{}, fmt.Errorf("%w: no ShiftPVPool is registered for node %q", ErrPoolNotFound, nodeName)
	}
	return result, nil
}

// PoolForNodeLifecycle selects one terminating registration ahead of active
// duplicates. This lets the node prove an empty duplicate Pool and release its
// exact identity while ordinary placement continues to reject duplicates.
func (r *Registry) PoolForNodeLifecycle(ctx context.Context, nodeName string) (Pool, error) {
	if nodeName == "" {
		return Pool{}, fmt.Errorf("%w: node name is required", ErrPoolConfiguration)
	}
	pools, err := r.ListPoolRegistrations(ctx)
	if err != nil {
		return Pool{}, err
	}
	candidates := make([]Pool, 0, 2)
	deleting := make([]Pool, 0, 1)
	for _, pool := range pools {
		if pool.NodeName != nodeName {
			continue
		}
		candidates = append(candidates, pool)
		if pool.DeletionTimestamp != nil {
			deleting = append(deleting, pool)
		}
	}
	if len(deleting) > 0 {
		sort.Slice(deleting, func(left, right int) bool {
			leftTime, rightTime := deleting[left].DeletionTimestamp.Time, deleting[right].DeletionTimestamp.Time
			if leftTime.Equal(rightTime) {
				return deleting[left].Name < deleting[right].Name
			}
			return leftTime.Before(rightTime)
		})
		return deleting[0], nil
	}
	if len(candidates) == 0 {
		return Pool{}, fmt.Errorf("%w: no ShiftPVPool is registered for node %q", ErrPoolNotFound, nodeName)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return Pool{}, duplicatePoolError(nodeName)
}

// PoolForIdentity resolves one exact Pool incarnation without applying the
// node-wide duplicate-registration gate. Cleanup uses this path to settle data
// owned by a terminating Pool while ordinary placement remains fail-closed.
func (r *Registry) PoolForIdentity(ctx context.Context, name, uid, nodeName string) (Pool, error) {
	if err := r.validate(); err != nil {
		return Pool{}, err
	}
	if name == "" || uid == "" || nodeName == "" {
		return Pool{}, fmt.Errorf("%w: Pool name, UID, and node name are required", ErrPoolConfiguration)
	}
	object, err := r.Client.Resource(PoolResource).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Pool{}, fmt.Errorf("%w: ShiftPVPool %q", ErrPoolNotFound, name)
	}
	if err != nil {
		return Pool{}, fmt.Errorf("get ShiftPVPool %q: %w", name, err)
	}
	pool, err := poolFrom(object)
	if err != nil {
		return Pool{}, err
	}
	if pool.UID != uid || pool.NodeName != nodeName {
		return Pool{}, fmt.Errorf("%w: ShiftPVPool %q identity changed", ErrStateConflict, name)
	}
	if !filepath.IsAbs(pool.MountPath) || pool.MountPath == "/" {
		return Pool{}, fmt.Errorf("%w: ShiftPVPool %q has invalid mountPath", ErrPoolConfiguration, name)
	}
	return pool, nil
}

func (r *Registry) ReadyPoolForNode(ctx context.Context, nodeName string) (Pool, error) {
	pool, err := r.PoolForNode(ctx, nodeName)
	if err != nil {
		return Pool{}, err
	}
	now, staleAfter := r.readiness()
	if placeable, reason := poolReady(pool, now, staleAfter); !placeable {
		return Pool{}, fmt.Errorf("%w: ShiftPVPool %q on node %q: %s", ErrPoolNotReady, pool.Name, nodeName, reason)
	}
	return pool, nil
}

// readiness resolves the observation instant and the probe staleness budget a
// readiness decision is made against.
func (r *Registry) readiness() (time.Time, time.Duration) {
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	staleAfter := r.PoolReadinessStaleAfter
	if staleAfter <= 0 {
		staleAfter = DefaultPoolReadinessStaleAfter
	}
	return now, staleAfter
}

// poolReady is the single gate that admits a Pool for new placement: a fresh
// Ready probe, a complete inventory, and installed deregistration protection.
func poolReady(pool Pool, now time.Time, staleAfter time.Duration) (bool, string) {
	if ready, reason := pool.ReadyAt(now, staleAfter); !ready {
		return false, reason
	}
	if ready, reason := poolInventoryReadyAt(pool, now, staleAfter); !ready {
		return false, reason
	}
	if !slices.Contains(pool.Finalizers, PoolProtectionFinalizer) {
		return false, "PoolProtectionMissing"
	}
	return true, ""
}

func duplicatePoolError(nodeName string) error {
	return fmt.Errorf("%w: multiple ShiftPVPools are registered for node %q", ErrPoolConfiguration, nodeName)
}

func poolInventoryReadyAt(pool Pool, now time.Time, staleAfter time.Duration) (bool, string) {
	inventory := pool.Status.Inventory
	if inventory == nil {
		return false, "InventoryMissing"
	}
	if !inventory.Valid {
		return false, "InventoryInvalid"
	}
	if inventory.Truncated {
		return false, "InventoryTruncated"
	}
	if inventory.ObservedAt.IsZero() || now.Before(inventory.ObservedAt.Time) || now.Sub(inventory.ObservedAt.Time) > staleAfter {
		return false, "InventoryStale"
	}
	return true, ""
}

// PoolHasServingVolume reports whether the Pool already contains the physical
// serving path reserved for volumeID, regardless of that copy's incarnation.
func PoolHasServingVolume(pool Pool, volumeID string) bool {
	return PoolHasConflictingServingVolume(pool, volumeID, nil)
}

// PoolHasConflictingServingVolume permits only the exact serving copy already
// journaled by the current transaction.
func PoolHasConflictingServingVolume(pool Pool, volumeID string, allowed *volume.CopyIdentity) bool {
	if pool.Status.Inventory == nil {
		return false
	}
	for _, observed := range pool.Status.Inventory.Copies {
		if observed.Present && observed.Identity != nil && observed.Identity.Role == volume.RoleServing && observed.Identity.VolumeID == volumeID {
			if allowed != nil && *observed.Identity == *allowed {
				continue
			}
			return true
		}
	}
	return false
}

// PoolHasPublishedCopy reports scanner proof that the exact serving copy is
// both present and mounted from the Pool that owns that copy.
func PoolHasPublishedCopy(pool Pool, target *volume.CopyIdentity) bool {
	if target == nil || target.Validate() != nil || target.Role != volume.RoleServing ||
		target.PoolName != pool.Name || target.PoolUID != pool.UID || target.NodeName != pool.NodeName || pool.Status.Inventory == nil {
		return false
	}
	for _, observed := range pool.Status.Inventory.Copies {
		if observed.Identity != nil && *observed.Identity == *target {
			return observed.Present && observed.Published && observed.Problem == ""
		}
	}
	return false
}

func (r *Registry) SetPoolStatus(ctx context.Context, name, uid, nodeName string, status PoolStatus) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" || nodeName == "" {
		return fmt.Errorf("ShiftPVPool name, UID, and node name are required")
	}
	return r.mutateObject(ctx, objectMutation{
		resource:   PoolResource,
		kind:       "ShiftPVPool",
		name:       name,
		uid:        uid,
		backoff:    retry.DefaultRetry,
		readError:  "read ShiftPVPool status",
		writeError: "update ShiftPVPool status",
		status:     true,
		apply: func(object *unstructured.Unstructured) error {
			registeredNode, _, _ := unstructured.NestedString(object.Object, "spec", "nodeName")
			if registeredNode != nodeName {
				return fmt.Errorf("%w: ShiftPVPool %q belongs to node %q, not %q", ErrStateConflict, name, registeredNode, nodeName)
			}
			data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
			if err != nil {
				return fmt.Errorf("encode ShiftPVPool status: %w", err)
			}
			if err := unstructured.SetNestedMap(object.Object, data, "status"); err != nil {
				return fmt.Errorf("set ShiftPVPool status: %w", err)
			}
			return nil
		},
	})
}

func (r *Registry) EnsurePoolFinalizer(ctx context.Context, name, uid string) error {
	return r.updatePoolFinalizer(ctx, name, uid, true)
}

func (r *Registry) RemovePoolFinalizer(ctx context.Context, name, uid string) error {
	return r.updatePoolFinalizer(ctx, name, uid, false)
}

func (r *Registry) ApprovePoolIdentityRelease(ctx context.Context, name, uid string) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" {
		return fmt.Errorf("ShiftPVPool name and UID are required")
	}
	return r.mutateObject(ctx, objectMutation{
		resource:   PoolResource,
		kind:       "ShiftPVPool",
		name:       name,
		uid:        uid,
		backoff:    retry.DefaultRetry,
		readError:  "read ShiftPVPool identity release approval",
		writeError: "approve ShiftPVPool identity release",
		apply: func(object *unstructured.Unstructured) error {
			if object.GetDeletionTimestamp() == nil {
				return fmt.Errorf("%w: ShiftPVPool %q is not deleting", ErrStateConflict, name)
			}
			annotations := object.GetAnnotations()
			if annotations == nil {
				annotations = map[string]string{}
			}
			if annotations[PoolIdentityReleaseAnnotation] == uid {
				return errObjectUnchanged
			}
			annotations[PoolIdentityReleaseAnnotation] = uid
			object.SetAnnotations(annotations)
			return nil
		},
	})
}

func (r *Registry) updatePoolFinalizer(ctx context.Context, name, uid string, present bool) error {
	return r.updateObjectFinalizer(ctx, PoolResource, name, uid, PoolProtectionFinalizer, present)
}

func (p Pool) ReadyAt(now time.Time, staleAfter time.Duration) (bool, string) {
	if p.DeletionTimestamp != nil {
		return false, "PoolDeregistering"
	}
	condition := meta.FindStatusCondition(p.Status.Conditions, PoolConditionReady)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		if condition != nil && condition.Reason != "" {
			return false, condition.Reason
		}
		return false, "ProbePending"
	}
	if p.Status.ObservedGeneration != p.Generation || condition.ObservedGeneration != p.Generation {
		return false, "ProbeOutdated"
	}
	if p.Status.LastProbeTime.IsZero() || staleAfter <= 0 || now.Sub(p.Status.LastProbeTime.Time) > staleAfter || now.Before(p.Status.LastProbeTime.Time) {
		return false, "ProbeStale"
	}
	return true, condition.Reason
}

// CleanupReadyAt keeps an exact, already-approved cleanup executable while a
// Pool is terminating. New placement continues to use ReadyAt and remains
// closed for the same Pool.
func (p Pool) CleanupReadyAt(now time.Time, staleAfter time.Duration) (bool, string) {
	if p.DeletionTimestamp == nil {
		return p.ReadyAt(now, staleAfter)
	}
	if p.Status.ObservedGeneration != p.Generation {
		return false, "ProbeOutdated"
	}
	for _, conditionType := range []string{PoolConditionAccessible, PoolConditionWritable, PoolConditionCapacityReadable} {
		condition := meta.FindStatusCondition(p.Status.Conditions, conditionType)
		if condition == nil {
			return false, "ProbePending"
		}
		if condition.ObservedGeneration != p.Generation {
			return false, "ProbeOutdated"
		}
		if condition.Status != metav1.ConditionTrue {
			if condition.Reason != "" {
				return false, condition.Reason
			}
			return false, "ProbeFailed"
		}
	}
	if p.Status.LastProbeTime.IsZero() || staleAfter <= 0 || now.Sub(p.Status.LastProbeTime.Time) > staleAfter || now.Before(p.Status.LastProbeTime.Time) {
		return false, "ProbeStale"
	}
	return true, "PoolCleanupReady"
}

func poolFrom(object *unstructured.Unstructured) (Pool, error) {
	status := PoolStatus{}
	if data, found, err := unstructured.NestedMap(object.Object, "status"); err != nil {
		return Pool{}, fmt.Errorf("decode ShiftPVPool %q status: %w", object.GetName(), err)
	} else if found {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(data, &status); err != nil {
			return Pool{}, fmt.Errorf("decode ShiftPVPool %q status: %w", object.GetName(), err)
		}
	}
	nodeName, _, _ := unstructured.NestedString(object.Object, "spec", "nodeName")
	mountPath, _, _ := unstructured.NestedString(object.Object, "spec", "mountPath")
	capacityLimit, _, _ := unstructured.NestedString(object.Object, "spec", "capacity", "limit")
	return Pool{
		Name: object.GetName(), UID: string(object.GetUID()), NodeName: nodeName, MountPath: filepath.Clean(mountPath),
		CapacityLimit: capacityLimit, Generation: object.GetGeneration(), DeletionTimestamp: object.GetDeletionTimestamp(),
		Finalizers: append([]string(nil), object.GetFinalizers()...), IdentityReleaseApproval: object.GetAnnotations()[PoolIdentityReleaseAnnotation], Status: status,
	}, nil
}

// PoolInventoryFrom decodes status.inventory from a ShiftPVPool object.
func PoolInventoryFrom(pool *unstructured.Unstructured) (PoolInventory, bool, error) {
	value, found, err := unstructured.NestedMap(pool.Object, "status", "inventory")
	if err != nil || !found {
		return PoolInventory{}, found, err
	}
	var inventory PoolInventory
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(value, &inventory); err != nil {
		return PoolInventory{}, true, fmt.Errorf("decode Pool inventory: %w", err)
	}
	return inventory, true, nil
}
