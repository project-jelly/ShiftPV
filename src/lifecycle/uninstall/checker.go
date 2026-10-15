package uninstall

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	DriverName                    = volume.DriverName
	PoolProtectionFinalizer       = volumeapi.PoolProtectionFinalizer
	PoolIdentityReleaseAnnotation = volumeapi.PoolIdentityReleaseAnnotation
)

type VolumeRepository interface {
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
	ListPools(context.Context) ([]volumeapi.Pool, error)
	ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error)
	RemovePoolFinalizer(context.Context, string, string) error
}

type CleanupRepository interface {
	List(context.Context) ([]cleanupapi.Cleanup, error)
}

type Checker struct {
	Client            kubernetes.Interface
	Volumes           VolumeRepository
	StorageClassNames []string
	Namespace         string
	Cleanups          CleanupRepository
	Now               func() time.Time
	InventoryMaxAge   time.Duration
}

type Blocker struct {
	Kind      string
	Namespace string
	Name      string
	Reason    string
}

type Report struct {
	Blockers []Blocker
}

const PoolInventoryBlockerKind = "ShiftPVPoolInventory"
const topologyKey = volume.TopologyKey

func (r Report) Safe() bool {
	return len(r.Blockers) == 0
}

func (c *Checker) Check(ctx context.Context) (Report, error) {
	return c.CheckAfter(ctx, time.Time{})
}

func (c *Checker) ReleasePoolProtection(ctx context.Context) error {
	if c == nil || c.Volumes == nil {
		return fmt.Errorf("uninstall checker is not configured")
	}
	pools, err := c.Volumes.ListPoolRegistrations(ctx)
	if err != nil {
		return fmt.Errorf("list ShiftPVPools: %w", err)
	}
	var result error
	for _, pool := range pools {
		if !slices.Contains(pool.Finalizers, PoolProtectionFinalizer) {
			continue
		}
		if err := c.Volumes.RemovePoolFinalizer(ctx, pool.Name, pool.UID); err != nil {
			result = errors.Join(result, fmt.Errorf("release ShiftPVPool %q protection: %w", pool.Name, err))
		}
	}
	return result
}

// CheckAfter determines whether the whole driver can be removed. Every ShiftPV
// dependency is a blocker regardless of which Pool owns it.
func (c *Checker) CheckAfter(ctx context.Context, inventoryAfter time.Time) (Report, error) {
	if c == nil || c.Client == nil || c.Volumes == nil || c.Cleanups == nil {
		return Report{}, fmt.Errorf("uninstall checker is not configured")
	}
	if len(c.StorageClassNames) == 0 {
		return Report{}, fmt.Errorf("at least one ShiftPV StorageClass name is required")
	}
	for _, storageClassName := range c.StorageClassNames {
		if strings.TrimSpace(storageClassName) == "" {
			return Report{}, fmt.Errorf("ShiftPV StorageClass names must not be empty")
		}
	}

	report := Report{}
	persistentVolumes, err := c.listDriverPersistentVolumes(ctx)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, persistentVolumeBlockers(persistentVolumes)...)

	claimed, err := c.claimBlockers(ctx)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, claimed...)

	volumes, err := c.Volumes.ListVolumes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("list ShiftPVVolumes: %w", err)
	}
	report.Blockers = append(report.Blockers, volumeBlockers(volumes)...)

	moved, err := c.moveBlockers(ctx, nil)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, moved...)

	cleaned, err := c.cleanupBlockers(ctx, nil)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, cleaned...)

	pooled, err := c.poolBlockers(ctx, inventoryAfter)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, pooled...)

	sortBlockers(report.Blockers)
	return report, nil
}

// CheckPoolDeleteAfter determines whether one exact Pool registration can be
// removed without losing authority over a volume, move, embedded cleanup
// journal, PersistentVolume, or physical copy. Other Pools may remain in use.
func (c *Checker) CheckPoolDeleteAfter(ctx context.Context, poolName string, poolUID types.UID, inventoryAfter time.Time) (Report, error) {
	if c == nil || c.Client == nil || c.Volumes == nil || c.Cleanups == nil {
		return Report{}, fmt.Errorf("Pool deletion checker is not configured")
	}
	target, err := c.exactPool(ctx, poolName, poolUID)
	if err != nil {
		return Report{}, err
	}

	report := Report{}
	report.Blockers = append(report.Blockers, poolInventoryBlockers(target, c.now(), c.inventoryMaxAge(), inventoryAfter.UTC())...)

	volumes, err := c.Volumes.ListVolumes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("list ShiftPVVolumes: %w", err)
	}

	persistentVolumes, err := c.listDriverPersistentVolumes(ctx)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, poolPersistentVolumeBlockers(persistentVolumes, volumes, target)...)
	report.Blockers = append(report.Blockers, poolVolumeBlockers(volumes, target)...)

	moved, err := c.moveBlockers(ctx, &target)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, moved...)

	cleaned, err := c.cleanupBlockers(ctx, &target)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, cleaned...)

	sortBlockers(report.Blockers)
	return report, nil
}

// exactPool resolves the registration that still carries the requested identity.
func (c *Checker) exactPool(ctx context.Context, poolName string, poolUID types.UID) (volumeapi.Pool, error) {
	if strings.TrimSpace(poolName) == "" || poolUID == "" {
		return volumeapi.Pool{}, fmt.Errorf("exact Pool identity is required")
	}
	pools, err := c.Volumes.ListPoolRegistrations(ctx)
	if err != nil {
		return volumeapi.Pool{}, fmt.Errorf("list ShiftPVPools: %w", err)
	}
	for _, pool := range pools {
		if pool.Name != poolName {
			continue
		}
		if pool.UID != string(poolUID) {
			return volumeapi.Pool{}, fmt.Errorf("Pool %q identity changed", poolName)
		}
		return pool, nil
	}
	return volumeapi.Pool{}, fmt.Errorf("Pool %q was not found", poolName)
}

func (c *Checker) listDriverPersistentVolumes(ctx context.Context) ([]corev1.PersistentVolume, error) {
	list, err := c.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	driverOwned := []corev1.PersistentVolume{}
	for _, persistentVolume := range list.Items {
		if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != DriverName {
			continue
		}
		driverOwned = append(driverOwned, persistentVolume)
	}
	return driverOwned, nil
}

// persistentVolumeBlockers reports every driver-owned PersistentVolume. Driver
// removal loses the mount path of all of them.
func persistentVolumeBlockers(persistentVolumes []corev1.PersistentVolume) []Blocker {
	blockers := []Blocker{}
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI == nil {
			continue
		}
		reason := fmt.Sprintf("driver=%s volumeHandle=%s", DriverName, persistentVolume.Spec.CSI.VolumeHandle)
		if persistentVolume.Spec.ClaimRef != nil {
			reason += fmt.Sprintf(" claim=%s/%s", persistentVolume.Spec.ClaimRef.Namespace, persistentVolume.Spec.ClaimRef.Name)
		}
		blockers = append(blockers, Blocker{Kind: "PersistentVolume", Name: persistentVolume.Name, Reason: reason})
	}
	return blockers
}

// poolPersistentVolumeBlockers reports only the PersistentVolumes whose data
// may live on the target Pool. An exact current copy decides on its own; an
// unknown copy falls back to node affinity and fails closed when the placement
// cannot be read.
func poolPersistentVolumeBlockers(persistentVolumes []corev1.PersistentVolume, volumes map[string]volumeapi.State, pool volumeapi.Pool) []Blocker {
	blockers := []Blocker{}
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI == nil {
			continue
		}
		if state, exists := volumes[persistentVolume.Spec.CSI.VolumeHandle]; exists {
			if usesPool, known := currentCopyUsesPool(persistentVolume.Spec.CSI.VolumeHandle, state, pool); known {
				if usesPool {
					blockers = append(blockers, Blocker{Kind: "PersistentVolume", Name: persistentVolume.Name, Reason: "pool=" + pool.Name + " poolUID=" + pool.UID})
				}
				continue
			}
		}
		matches, known := persistentVolumeTargetsNode(persistentVolume, pool.NodeName)
		if !known || matches {
			reason := "placement=unknown"
			if matches {
				reason = "node=" + pool.NodeName
			}
			blockers = append(blockers, Blocker{Kind: "PersistentVolume", Name: persistentVolume.Name, Reason: reason})
		}
	}
	return blockers
}

func (c *Checker) claimBlockers(ctx context.Context) ([]Blocker, error) {
	claims, err := c.Client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumeClaims: %w", err)
	}
	blockers := []Blocker{}
	for _, claim := range claims.Items {
		if claim.Spec.StorageClassName == nil || !slices.Contains(c.StorageClassNames, *claim.Spec.StorageClassName) {
			continue
		}
		reason := "references the ShiftPV StorageClass"
		if claim.Spec.VolumeName != "" {
			reason += " volume=" + claim.Spec.VolumeName
		}
		blockers = append(blockers, Blocker{Kind: "PersistentVolumeClaim", Namespace: claim.Namespace, Name: claim.Name, Reason: reason})
	}
	return blockers, nil
}

// volumeBlockers reports every ShiftPVVolume. Driver removal loses the
// lifecycle of all of them.
func volumeBlockers(volumes map[string]volumeapi.State) []Blocker {
	blockers := []Blocker{}
	for volumeID, state := range volumes {
		reasonParts := []string{"phase=" + state.Phase, "owner=" + state.OwnerNode}
		if state.ActiveMove != "" {
			reasonParts = append(reasonParts, "activeMove="+state.ActiveMove)
		}
		if len(state.PublishedNodes) > 0 {
			reasonParts = append(reasonParts, "publishedNodes="+strings.Join(state.PublishedNodes, ","))
		}
		blockers = append(blockers, Blocker{Kind: "ShiftPVVolume", Name: volumeID, Reason: strings.Join(reasonParts, " ")})
	}
	return blockers
}

// poolVolumeBlockers reports only the ShiftPVVolumes that may still hold data
// on the target Pool, and fails closed on an unknown current copy that either
// is owned by or is published on the Pool node.
func poolVolumeBlockers(volumes map[string]volumeapi.State, pool volumeapi.Pool) []Blocker {
	blockers := []Blocker{}
	for volumeID, state := range volumes {
		usesPool, known := currentCopyUsesPool(volumeID, state, pool)
		if known && !usesPool {
			continue
		}
		if !known && !slices.Contains(state.PublishedNodes, pool.NodeName) && state.OwnerNode != pool.NodeName &&
			!(state.Phase == "" && state.InitialNode == pool.NodeName) {
			continue
		}
		reason := "owner=" + state.OwnerNode
		if state.CurrentCopy == nil {
			reason += " currentCopy=missing"
		} else {
			reason += " pool=" + state.CurrentCopy.PoolName + " poolUID=" + state.CurrentCopy.PoolUID
		}
		blockers = append(blockers, Blocker{Kind: "ShiftPVVolume", Name: volumeID, Reason: reason})
	}
	return blockers
}

// moveBlockers reports unsettled ShiftPVMoves. A nil pool scopes the scan to
// the whole driver; a non-nil pool keeps only the moves that still reference
// that exact Pool identity or node.
func (c *Checker) moveBlockers(ctx context.Context, pool *volumeapi.Pool) ([]Blocker, error) {
	moves, err := c.Volumes.ListMoves(ctx)
	if err != nil {
		return nil, fmt.Errorf("list ShiftPVMoves: %w", err)
	}
	blockers := []Blocker{}
	for _, move := range moves {
		if volumeapi.MoveCleanupSettled(move) {
			continue
		}
		if pool != nil && !moveUsesPool(move, *pool) {
			continue
		}
		blockers = append(blockers, Blocker{
			Kind:   "ShiftPVMove",
			Name:   move.Name,
			Reason: fmt.Sprintf("phase=%s volume=%s", move.Status.Phase, move.Spec.VolumeID),
		})
	}
	return blockers, nil
}

// cleanupBlockers reports unfinished cleanup journals. A nil pool scopes the
// scan to the whole driver; a non-nil pool keeps only the journals targeting
// that exact Pool identity.
func (c *Checker) cleanupBlockers(ctx context.Context, pool *volumeapi.Pool) ([]Blocker, error) {
	cleanups, err := c.Cleanups.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list cleanup journals: %w", err)
	}
	blockers := []Blocker{}
	for _, cleanup := range cleanups {
		if cleanup.Status.Phase == cleanupapi.PhaseCompleted {
			continue
		}
		if pool != nil && (cleanup.Spec.Target.PoolName != pool.Name || cleanup.Spec.Target.PoolUID != pool.UID) {
			continue
		}
		blocker, err := cleanupJournalBlocker(cleanup)
		if err != nil {
			return nil, err
		}
		blockers = append(blockers, blocker)
	}
	return blockers, nil
}

// poolBlockers reports the inventory and retained identity of every Pool.
func (c *Checker) poolBlockers(ctx context.Context, inventoryAfter time.Time) ([]Blocker, error) {
	pools, err := c.Volumes.ListPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("list ShiftPVPools: %w", err)
	}
	now := c.now()
	maxAge := c.inventoryMaxAge()
	observedAfter := inventoryAfter.UTC()
	blockers := []Blocker{}
	for _, pool := range pools {
		blockers = append(blockers, poolInventoryBlockers(pool, now, maxAge, observedAfter)...)
		if pool.DeletionTimestamp == nil {
			continue
		}
		released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased)
		if released == nil || released.Status != metav1.ConditionTrue || released.ObservedGeneration != pool.Generation {
			reason := "identity=retained"
			if released != nil && released.Reason != "" {
				reason = released.Reason
			}
			blockers = append(blockers, Blocker{Kind: "ShiftPVPoolIdentity", Name: pool.Name, Reason: reason})
		}
	}
	return blockers, nil
}

func (c *Checker) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *Checker) inventoryMaxAge() time.Duration {
	if c.InventoryMaxAge > 0 {
		return c.InventoryMaxAge
	}
	return volumeapi.DefaultPoolReadinessStaleAfter
}

func sortBlockers(blockers []Blocker) {
	sort.Slice(blockers, func(left, right int) bool {
		return blockerKey(blockers[left]) < blockerKey(blockers[right])
	})
}

func copyUsesPool(copy *cleanupapi.CopyIdentity, pool volumeapi.Pool) bool {
	return copy != nil && copy.PoolName == pool.Name && copy.PoolUID == pool.UID
}

func moveUsesPool(move volumeapi.Move, pool volumeapi.Pool) bool {
	preparationUses, destinationKnown := preparationUsesPool(move.Status.Preparation, pool)
	if preparationUses {
		return true
	}
	sourceKnown := exactMoveCopy(move.Status.SourceCopy, move.Spec.VolumeID, move.Spec.SourceNode, volume.RoleServing)
	if sourceKnown {
		if copyUsesPool(move.Status.SourceCopy, pool) {
			return true
		}
	} else if move.Spec.SourceNode == pool.NodeName {
		return true
	}

	for _, candidate := range []struct {
		identity *volume.CopyIdentity
		role     string
	}{{move.Status.IncomingCopy, volume.RoleIncoming}, {move.Status.DestinationCopy, volume.RoleServing}} {
		if !exactMoveCopy(candidate.identity, move.Spec.VolumeID, move.Status.DestinationNode, candidate.role) {
			continue
		}
		destinationKnown = true
		if copyUsesPool(candidate.identity, pool) {
			return true
		}
	}
	if move.Status.DestinationNode != "" && move.Status.DestinationPoolUID != "" {
		destinationKnown = true
		if move.Status.DestinationPoolUID == pool.UID {
			return true
		}
	}
	return !destinationKnown && (move.Status.DestinationNode == pool.NodeName || slices.Contains(move.Status.CandidateNodes, pool.NodeName))
}

func preparationUsesPool(preparation *volumeapi.MovePreparation, pool volumeapi.Pool) (uses, known bool) {
	if preparation == nil {
		return false, false
	}
	// A contradictory name or node must not erase a retained Pool UID.
	if preparation.Pool.UID == pool.UID {
		return true, true
	}
	if volume.ValidObjectName(preparation.Pool.Name) && volume.ValidIdentityToken(preparation.Pool.UID) &&
		volume.ValidObjectName(preparation.NodeName) {
		return false, true
	}
	// Without an exact target, retain protection wherever it might belong.
	return preparation.Pool.Name == pool.Name || preparation.NodeName == "" || preparation.NodeName == pool.NodeName, false
}

func exactMoveCopy(copy *volume.CopyIdentity, volumeID, nodeName, role string) bool {
	return copy != nil && copy.Validate() == nil && copy.VolumeID == volumeID && copy.NodeName == nodeName && copy.Role == role
}

func currentCopyUsesPool(volumeID string, state volumeapi.State, pool volumeapi.Pool) (bool, bool) {
	if state.Initializing() && state.InitialPool != nil {
		return state.InitialPool.Name == pool.Name && state.InitialPool.UID == pool.UID, true
	}
	copy := state.CurrentCopy
	if copy == nil || copy.Validate() != nil || copy.VolumeID != volumeID || copy.VolumeUID != state.UID || copy.NodeName != state.OwnerNode || copy.Role != volume.RoleServing {
		return false, false
	}
	return copy.PoolName == pool.Name && copy.PoolUID == pool.UID, true
}

func persistentVolumeTargetsNode(persistentVolume corev1.PersistentVolume, nodeName string) (bool, bool) {
	affinity := persistentVolume.Spec.NodeAffinity
	if affinity == nil || affinity.Required == nil || len(affinity.Required.NodeSelectorTerms) == 0 {
		return false, false
	}
	for _, term := range affinity.Required.NodeSelectorTerms {
		known := false
		matches := false
		for _, expression := range term.MatchExpressions {
			if expression.Key != topologyKey {
				continue
			}
			switch expression.Operator {
			case corev1.NodeSelectorOpIn:
				known = true
				matches = slices.Contains(expression.Values, nodeName)
			case corev1.NodeSelectorOpNotIn:
				known = true
				matches = !slices.Contains(expression.Values, nodeName)
			}
		}
		if !known {
			return false, false
		}
		if matches {
			return true, true
		}
	}
	return false, true
}

func cleanupJournalBlocker(cleanup cleanupapi.Cleanup) (Blocker, error) {
	kind := cleanup.Spec.Authority.Kind
	name := cleanup.Spec.Authority.Name
	if (kind != "ShiftPVVolume" && kind != "ShiftPVMove") || strings.TrimSpace(name) == "" {
		return Blocker{}, fmt.Errorf("cleanup journal %q has invalid parent authority %q/%q", cleanup.Name, kind, name)
	}
	phase := cleanup.Status.Phase
	if phase == "" {
		phase = cleanupapi.PhasePending
	}
	reason := fmt.Sprintf("cleanupPhase=%s operation=%s volume=%s", phase, cleanup.Spec.OperationID, cleanup.Spec.Target.VolumeID)
	return Blocker{Kind: kind, Name: name, Reason: reason}, nil
}

func (r Report) WaitingForInventory() bool {
	if len(r.Blockers) == 0 {
		return false
	}
	for _, blocker := range r.Blockers {
		if blocker.Kind != PoolInventoryBlockerKind && blocker.Kind != "ShiftPVPoolIdentity" {
			return false
		}
	}
	return true
}

func poolInventoryBlockers(pool volumeapi.Pool, now time.Time, maxAge time.Duration, observedAfter time.Time) []Blocker {
	inventory := pool.Status.Inventory
	if inventory == nil {
		return []Blocker{{Kind: PoolInventoryBlockerKind, Name: pool.Name, Reason: "inventory=missing"}}
	}
	result := []Blocker{}
	reasons := []string{}
	if pool.Status.ObservedGeneration != pool.Generation {
		reasons = append(reasons, fmt.Sprintf("generation=%d observedGeneration=%d", pool.Generation, pool.Status.ObservedGeneration))
	}
	if !inventory.Valid {
		reasons = append(reasons, "valid=false")
	}
	if inventory.Truncated {
		reasons = append(reasons, "truncated=true")
	}
	if inventory.Message != "" {
		reasons = append(reasons, "message="+inventory.Message)
	}
	observedAt := inventory.ObservedAt.Time
	switch {
	case inventory.ObservedAt.IsZero():
		reasons = append(reasons, "observedAt=missing")
	case now.Before(observedAt):
		reasons = append(reasons, "observedAt=future")
	case now.Sub(observedAt) > maxAge:
		reasons = append(reasons, "observedAt=stale")
	case !observedAfter.IsZero() && !observedAt.After(observedAfter):
		reasons = append(reasons, "observedAt=before-quiesce")
	}
	if len(reasons) > 0 {
		result = append(result, Blocker{Kind: PoolInventoryBlockerKind, Name: pool.Name, Reason: strings.Join(reasons, " ")})
		// Copy observations are authoritative only when the whole inventory is
		// generation-current, valid, complete, fresh, and newer than the
		// uninstall quiesce barrier. Until then, wait for a trustworthy snapshot
		// instead of turning stale copy evidence into a terminal blocker.
		return result
	}
	for _, observed := range inventory.Copies {
		if !observed.Present && !observed.Published {
			continue
		}
		parts := []string{"marker=" + observed.Marker, fmt.Sprintf("present=%t", observed.Present)}
		if observed.Identity != nil {
			parts = append(parts, "role="+string(observed.Identity.Role), "volume="+observed.Identity.VolumeID, "copy="+observed.Identity.CopyID)
		}
		if observed.Published {
			parts = append(parts, "published=true")
		}
		if observed.Problem != "" {
			parts = append(parts, "problem="+observed.Problem)
		}
		result = append(result, Blocker{Kind: "ShiftPVPoolCopy", Name: pool.Name, Reason: strings.Join(parts, " ")})
	}
	return result
}

func blockerKey(blocker Blocker) string {
	return blocker.Kind + "\x00" + blocker.Namespace + "\x00" + blocker.Name + "\x00" + blocker.Reason
}
