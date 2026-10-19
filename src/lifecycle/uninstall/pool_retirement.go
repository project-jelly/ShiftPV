package uninstall

import (
	"context"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// CheckPoolRetirement checks authority references, not physical absence. The
// caller holds the Pool UID admission fence; the registration request already
// excludes new admission. Unbound legacy requests and destination hints carry
// no Pool hold and must not deadlock retirement of an unavailable peer.
func (c *Checker) CheckPoolRetirement(ctx context.Context, name string, uid types.UID) (Report, error) {
	if c == nil || c.Client == nil || c.Volumes == nil || c.Cleanups == nil || c.Namespace == "" {
		return Report{}, fmt.Errorf("Pool retirement checker is not configured")
	}
	pool, err := c.exactPool(ctx, name, uid)
	if err != nil {
		return Report{}, err
	}
	if !pool.RetirementRequested() || !pool.RegistrationApproved() || !filepath.IsAbs(pool.MountPath) || pool.MountPath == "/" {
		return Report{}, fmt.Errorf("Pool retirement requires an exact approved registration request")
	}
	return c.retirementReferences(ctx, pool)
}

func (c *Checker) retirementReferences(ctx context.Context, pool volumeapi.Pool) (Report, error) {
	volumes, err := c.Volumes.ListVolumes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("list ShiftPVVolumes: %w", err)
	}
	pvs, err := c.listDriverPersistentVolumes(ctx)
	if err != nil {
		return Report{}, err
	}
	report := Report{Blockers: retirementVolumeBlockers(volumes, pool)}
	report.Blockers = append(report.Blockers, poolPersistentVolumeBlockers(pvs, volumes, pool)...)
	moves, err := c.Volumes.ListMoves(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("list ShiftPVMoves: %w", err)
	}
	for _, move := range moves {
		if !volumeapi.MoveCleanupSettled(move) && retirementMoveUsesPool(move, pool, volumes) {
			report.Blockers = append(report.Blockers, Blocker{Kind: "ShiftPVMove", Name: move.Name, Reason: "unsettled Pool reference"})
		}
	}
	cleanups, err := c.retirementCleanupBlockers(ctx, pool)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, cleanups...)
	actors, err := c.poolRetirementActors(ctx, pool)
	if err != nil {
		return Report{}, err
	}
	report.Blockers = append(report.Blockers, actors...)
	sortBlockers(report.Blockers)
	return report, nil
}

func (c *Checker) retirementCleanupBlockers(ctx context.Context, pool volumeapi.Pool) ([]Blocker, error) {
	cleanups, err := c.Cleanups.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list cleanup journals: %w", err)
	}
	var blockers []Blocker
	for _, cleanup := range cleanups {
		if cleanup.Status.Phase == "Completed" {
			continue
		}
		copy := cleanup.Spec.Target
		if copy.PoolUID == pool.UID || (copy.Validate() != nil && (copy.NodeName == "" || copy.NodeName == pool.NodeName)) {
			blocker, err := cleanupJournalBlocker(cleanup)
			if err != nil {
				return nil, err
			}
			blockers = append(blockers, blocker)
		}
	}
	return blockers, nil
}

func retirementMoveUsesPool(move volumeapi.Move, pool volumeapi.Pool, volumes map[string]volumeapi.State) bool {
	for _, copy := range []*volume.CopyIdentity{move.Status.SourceCopy, move.Status.IncomingCopy, move.Status.DestinationCopy} {
		if copy != nil && copy.PoolUID == pool.UID {
			return true
		}
		if copy != nil && copy.Validate() != nil && (copy.NodeName == "" || copy.NodeName == pool.NodeName) {
			return true
		}
	}
	if move.Status.DestinationPoolUID == pool.UID || move.Status.Preparation != nil && move.Status.Preparation.Pool.UID == pool.UID || !volume.ValidObjectName(move.Spec.SourceNode) {
		return true
	}
	move = retirementMoveView(move, volumes)
	return moveUsesPool(move, pool)
}

func retirementMoveView(move volumeapi.Move, volumes map[string]volumeapi.State) volumeapi.Move {
	if state, ok := volumes[move.Spec.VolumeID]; ok && move.Status.SourceCopy == nil && state.CurrentCopy != nil &&
		state.CurrentCopy.VolumeUID == state.UID && state.OwnerNode == move.Spec.SourceNode && exactMoveCopy(state.CurrentCopy, move.Spec.VolumeID, move.Spec.SourceNode, volume.RoleServing) {
		move.Status.SourceCopy = state.CurrentCopy
	}
	move.Status.CandidateNodes = nil
	if !move.Status.CapacityApproved && move.Status.Preparation == nil && move.Status.IncomingCopy == nil && move.Status.DestinationCopy == nil && move.Status.DestinationPoolUID == "" {
		move.Status.DestinationNode = ""
	}
	return move
}

func retirementVolumeBlockers(volumes map[string]volumeapi.State, pool volumeapi.Pool) []Blocker {
	var blockers []Blocker
	references := make(map[string]volumeapi.State, len(volumes))
	for id, state := range volumes {
		if state.InitialPool != nil && state.InitialPool.UID == pool.UID || state.CurrentCopy != nil && state.CurrentCopy.PoolUID == pool.UID {
			blockers = append(blockers, Blocker{Kind: "ShiftPVVolume", Name: id, Reason: "Pool UID reference remains"})
			continue
		}
		if state.InitialPool == nil && state.UnstartedCreation() {
			continue
		}
		references[id] = state
	}
	return append(blockers, poolVolumeBlockers(references, pool)...)
}

// Production helpers mount the exact registered Pool root. Resident Nodes
// mount the host root and remain running after all of their intents settle.
func helperMountsPool(spec corev1.PodSpec, pool volumeapi.Pool) bool {
	if spec.NodeName != "" && spec.NodeName != pool.NodeName {
		return false
	}
	for _, mount := range spec.Volumes {
		if mount.HostPath != nil && filepath.Clean(mount.HostPath.Path) == pool.MountPath {
			return true
		}
	}
	return false
}

func (c *Checker) poolRetirementActors(ctx context.Context, pool volumeapi.Pool) ([]Blocker, error) {
	pods, err := c.Client.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Pool helper Pods: %w", err)
	}
	var blockers []Blocker
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed && helperMountsPool(pod.Spec, pool) {
			blockers = append(blockers, Blocker{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, Reason: "active Pool helper"})
		}
	}
	jobs, err := c.Client.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Pool helper Jobs: %w", err)
	}
	for _, job := range jobs.Items {
		terminal := false
		for _, condition := range job.Status.Conditions {
			terminal = terminal || condition.Status == corev1.ConditionTrue && (condition.Type == "Complete" || condition.Type == "Failed")
		}
		if !terminal && helperMountsPool(job.Spec.Template.Spec, pool) {
			blockers = append(blockers, Blocker{Kind: "Job", Namespace: job.Namespace, Name: job.Name, Reason: "active Pool helper"})
		}
	}
	return blockers, nil
}
