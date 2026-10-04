package helperauth

import (
	"context"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// MoveOptions carries the exact move helper identity the authority predicates recheck.
type MoveOptions struct {
	MoveName, MoveUID, OperationID, Namespace, PodName string
}

// SourceAuthority rechecks that this Pod may still serve the exact source copy.
func SourceAuthority(client kubernetes.Interface, registry *volumeapi.Registry, options MoveOptions, identity volume.CopyIdentity) func(context.Context) error {
	return func(ctx context.Context) error {
		move, err := registry.GetMove(ctx, options.MoveName)
		if err != nil {
			return fmt.Errorf("read source service Move: %w", err)
		}
		if !sourceServiceAuthorizes(move, options, identity) {
			return fmt.Errorf("source service authority changed: %w", volumeapi.ErrStateConflict)
		}
		installationID, err := registry.InstallationID(ctx)
		if err != nil {
			return fmt.Errorf("read source installation identity: %w", err)
		}
		if installationID != identity.InstallationID {
			return fmt.Errorf("source installation authority changed: %w", volumeapi.ErrStateConflict)
		}
		pool, err := registry.PoolForIdentity(ctx, identity.PoolName, identity.PoolUID, identity.NodeName)
		if err != nil {
			return fmt.Errorf("read source Pool: %w", err)
		}
		if !poolMatchesIdentity(pool, identity) {
			return fmt.Errorf("source Pool authority changed: %w", volumeapi.ErrStateConflict)
		}
		state, err := registry.Get(ctx, identity.VolumeID)
		if err != nil {
			return fmt.Errorf("read source volume state: %w", err)
		}
		if !sourceVolumeAuthorizes(state, move, identity) {
			return fmt.Errorf("source volume authority changed: %w", volumeapi.ErrStateConflict)
		}
		pod, err := client.CoreV1().Pods(options.Namespace).Get(ctx, options.PodName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read source executor Pod: %w", err)
		}
		if !sourceExecutorAuthorizes(pod, move, identity) {
			return fmt.Errorf("source executor authority changed: %w", volumeapi.ErrStateConflict)
		}
		if !podControlledByMove(pod, move) {
			return fmt.Errorf("source executor is not owned by the current Move: %w", volumeapi.ErrStateConflict)
		}
		return nil
	}
}

// sourceServiceAuthorizes reports whether the Move still authorizes serving the
// exact source copy under the exact copy operation this helper was started for.
func sourceServiceAuthorizes(move volumeapi.Move, options MoveOptions, identity volume.CopyIdentity) bool {
	return move.UID == options.MoveUID && move.Status.SourceCopy != nil && *move.Status.SourceCopy == identity &&
		move.Status.CopyOperationID == options.OperationID &&
		(move.Status.Phase == "WaitingForCapacity" || move.Status.Phase == "Copying")
}

// sourceVolumeAuthorizes reports whether the volume is still moving under this
// exact Move, still carries the exact source copy, and is not published there.
func sourceVolumeAuthorizes(state volumeapi.State, move volumeapi.Move, identity volume.CopyIdentity) bool {
	return state.UID == identity.VolumeUID && state.Phase == volumeapi.PhaseMoving && state.ActiveMove == move.Name &&
		state.OwnerNode == identity.NodeName && state.CurrentCopy != nil && *state.CurrentCopy == identity &&
		!slices.Contains(state.PublishedNodes, identity.NodeName)
}

// sourceExecutorAuthorizes reports whether this Pod is a live, identified helper
// scheduled on the copy's node and labelled for the exact Move.
func sourceExecutorAuthorizes(pod *corev1.Pod, move volumeapi.Move, identity volume.CopyIdentity) bool {
	return pod.UID != "" && pod.DeletionTimestamp == nil && pod.Spec.NodeName == identity.NodeName &&
		pod.Labels["shiftpv.io/move-uid"] == move.UID
}

// podControlledByMove reports whether the Pod's controller owner is the exact
// ShiftPVMove incarnation.
func podControlledByMove(pod *corev1.Pod, move volumeapi.Move) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.APIVersion == "shiftpv.io/v1alpha1" && owner.Kind == "ShiftPVMove" && owner.Name == move.Name && string(owner.UID) == move.UID {
			return true
		}
	}
	return false
}

// poolMatchesIdentity reports whether the node's Pool is still the exact
// incarnation the copy identity names.
func poolMatchesIdentity(pool volumeapi.Pool, identity volume.CopyIdentity) bool {
	return pool.Name == identity.PoolName && pool.UID == identity.PoolUID
}

// MoveAuthority rechecks that the exact copy or promote operation is still authorized.
func MoveAuthority(client kubernetes.Interface, registry *volumeapi.Registry, options MoveOptions, action string, target volume.CopyIdentity) func(context.Context) error {
	return func(ctx context.Context) error {
		move, err := registry.GetMove(ctx, options.MoveName)
		if err != nil {
			return fmt.Errorf("read Move: %w", err)
		}
		if !moveTransactionIntact(move, options) {
			return fmt.Errorf("move authority changed: %w", volumeapi.ErrStateConflict)
		}
		if !moveActionPhaseAllowed(move, action) || !moveActionOperationMatches(move, options, action, target) {
			return fmt.Errorf("move operation is no longer authorized")
		}
		if err := verifyMoveExecutor(ctx, client, options, move, action, target.NodeName); err != nil {
			return err
		}
		installationID, err := registry.InstallationID(ctx)
		if err != nil {
			return fmt.Errorf("read installation identity: %w", err)
		}
		if installationID != target.InstallationID {
			return fmt.Errorf("installation authority changed: %w", volumeapi.ErrStateConflict)
		}
		pool, err := registry.PoolForIdentity(ctx, target.PoolName, target.PoolUID, target.NodeName)
		if err != nil {
			return fmt.Errorf("read target Pool: %w", err)
		}
		if !poolMatchesIdentity(pool, target) {
			return fmt.Errorf("Pool authority changed: %w", volumeapi.ErrStateConflict)
		}
		state, err := registry.Get(ctx, move.Spec.VolumeID)
		if err != nil {
			return fmt.Errorf("read source volume state: %w", err)
		}
		if !moveSourceVolumeAuthorizes(state, move, target) {
			return fmt.Errorf("source volume authority changed: %w", volumeapi.ErrStateConflict)
		}
		return nil
	}
}

// moveTransactionIntact reports whether the Move is still the exact object this
// helper was started for and still carries both transaction copies.
func moveTransactionIntact(move volumeapi.Move, options MoveOptions) bool {
	return move.UID == options.MoveUID && move.Status.SourceCopy != nil && move.Status.IncomingCopy != nil
}

// moveActionPhaseAllowed reports whether the Move's phase still admits action.
func moveActionPhaseAllowed(move volumeapi.Move, action string) bool {
	return (action == "copy" && (move.Status.Phase == "WaitingForCapacity" || move.Status.Phase == "Copying")) ||
		(action == "promote" && (move.Status.Phase == "Copying" || move.Status.Phase == "Promoting"))
}

// moveActionOperationMatches reports whether the Move's durable Job, operation
// ID, and transaction copies still name this exact action against target. It
// requires both transaction copies, so callers gate it on moveTransactionIntact.
func moveActionOperationMatches(move volumeapi.Move, options MoveOptions, action string, target volume.CopyIdentity) bool {
	return action == "copy" && move.Status.CopyJobName != "" && move.Status.CopyOperationID == options.OperationID && *move.Status.IncomingCopy == target ||
		action == "promote" && move.Status.PromotionJobName != "" && move.Status.PromotionOperationID == options.OperationID && *move.Status.IncomingCopy == target && move.Status.DestinationCopy != nil
}

// moveSourceVolumeAuthorizes reports whether the source volume is still moving
// under this exact Move, still carries the Move's exact source copy, and has not
// republished it on the source node.
func moveSourceVolumeAuthorizes(state volumeapi.State, move volumeapi.Move, target volume.CopyIdentity) bool {
	return state.UID == target.VolumeUID && state.Phase == volumeapi.PhaseMoving && state.ActiveMove == move.Name &&
		state.OwnerNode == move.Spec.SourceNode && state.CurrentCopy != nil && *state.CurrentCopy == *move.Status.SourceCopy &&
		!slices.Contains(state.PublishedNodes, move.Spec.SourceNode)
}

// RecoveryAuthority rechecks that this Pod may still verify the blocked recovery owner.
func RecoveryAuthority(client kubernetes.Interface, registry *volumeapi.Registry, options MoveOptions, identity volume.CopyIdentity) func(context.Context) error {
	return func(checkCtx context.Context) error {
		currentMove, err := registry.GetMove(checkCtx, options.MoveName)
		if err != nil {
			return fmt.Errorf("read recovery Move: %w", err)
		}
		if !recoveryOwnerAuthorizes(currentMove, options, identity) {
			return fmt.Errorf("recovery authority changed: %w", volumeapi.ErrStateConflict)
		}
		current, err := registry.Get(checkCtx, currentMove.Spec.VolumeID)
		if err != nil {
			return fmt.Errorf("read recovery volume state: %w", err)
		}
		if !recoveryVolumeAuthorizes(current, currentMove, identity) {
			return fmt.Errorf("recovery volume authority changed: %w", volumeapi.ErrStateConflict)
		}
		for _, node := range current.PublishedNodes {
			if node != current.OwnerNode {
				return fmt.Errorf("foreign publication remains")
			}
		}
		if err := verifyOwnedMoveJob(checkCtx, client, options, currentMove, identity.NodeName); err != nil {
			return err
		}
		installationID, err := registry.InstallationID(checkCtx)
		if err != nil {
			return fmt.Errorf("read recovery installation identity: %w", err)
		}
		if installationID != identity.InstallationID {
			return fmt.Errorf("recovery installation authority changed: %w", volumeapi.ErrStateConflict)
		}
		pool, err := registry.PoolForIdentity(checkCtx, identity.PoolName, identity.PoolUID, identity.NodeName)
		if err != nil {
			return fmt.Errorf("read recovery Pool: %w", err)
		}
		if !poolMatchesIdentity(pool, identity) {
			return fmt.Errorf("recovery Pool authority changed: %w", volumeapi.ErrStateConflict)
		}
		return nil
	}
}

// recoveryOwnerAuthorizes reports whether the Move is still the exact blocked
// object whose verification this node owns.
func recoveryOwnerAuthorizes(move volumeapi.Move, options MoveOptions, identity volume.CopyIdentity) bool {
	return move.UID == options.MoveUID && move.Status.Phase == "Blocked" &&
		move.Status.RecoveryPhase == "Verifying" && move.Status.RecoveryOwner == identity.NodeName
}

// recoveryVolumeAuthorizes reports whether the volume is still blocked under this
// exact Move and still carries the exact copy being verified.
func recoveryVolumeAuthorizes(state volumeapi.State, move volumeapi.Move, identity volume.CopyIdentity) bool {
	return state.UID == identity.VolumeUID && state.Phase == volumeapi.PhaseBlocked && state.ActiveMove == move.Name &&
		state.OwnerNode == identity.NodeName && state.CurrentCopy != nil && *state.CurrentCopy == identity
}

func verifyMoveExecutor(ctx context.Context, client kubernetes.Interface, options MoveOptions, move volumeapi.Move, action, nodeName string) error {
	job, err := ownedMoveJob(ctx, client, options, move, nodeName)
	if err != nil {
		return err
	}
	wantedJob := move.Status.CopyJobName
	if action == "promote" {
		wantedJob = move.Status.PromotionJobName
	}
	if job.Name != wantedJob {
		return fmt.Errorf("move executor Job differs from the durable operation")
	}
	return nil
}

func verifyOwnedMoveJob(ctx context.Context, client kubernetes.Interface, options MoveOptions, move volumeapi.Move, nodeName string) error {
	_, err := ownedMoveJob(ctx, client, options, move, nodeName)
	return err
}

func ownedMoveJob(ctx context.Context, client kubernetes.Interface, options MoveOptions, move volumeapi.Move, nodeName string) (*batchv1.Job, error) {
	pod, err := client.CoreV1().Pods(options.Namespace).Get(ctx, options.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read move executor Pod: %w", err)
	}
	if pod.DeletionTimestamp != nil {
		return nil, fmt.Errorf("move executor Pod is terminating: %w", volumeapi.ErrStateConflict)
	}
	if pod.Spec.NodeName != nodeName {
		return nil, fmt.Errorf("move executor Pod runs on a different node: %w", volumeapi.ErrStateConflict)
	}
	jobName, jobUID := "", ""
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "Job" {
			jobName, jobUID = owner.Name, string(owner.UID)
			break
		}
	}
	job, err := client.BatchV1().Jobs(options.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read move executor Job: %w", err)
	}
	if jobUID == "" || string(job.UID) != jobUID || job.DeletionTimestamp != nil || job.Labels["shiftpv.io/move-uid"] != move.UID {
		return nil, fmt.Errorf("move executor is not authorized: %w", volumeapi.ErrStateConflict)
	}
	for _, owner := range job.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "ShiftPVMove" && owner.Name == move.Name && string(owner.UID) == move.UID {
			return job, nil
		}
	}
	return nil, fmt.Errorf("move Job is not owned by the exact ShiftPVMove")
}
