// Move progress, helper Job outcomes, publication proof, and cleanup observations.
package controller

import (
	"context"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// observeMoveState projects the persisted Move status and the volume lock onto
// the FSM flags. It reads no API and adds no diagnosis beyond the capacity
// reason the capacity reconciler already recorded.
func observeMoveState(move volumeapi.Move, result *observation) {
	state := result.Volume
	result.FSM.VolumeLocked = state.Phase == volumeapi.PhaseMoving && state.ActiveMove == move.Name && state.OwnerNode == move.Spec.SourceNode
	result.FSM.ConsumerExists = result.Consumer != nil
	result.FSM.EvictionRequested = move.Status.EvictionRequested
	result.FSM.PublishedOnSource = slices.Contains(state.PublishedNodes, move.Spec.SourceNode)
	result.FSM.CapacityApproved = move.Status.CapacityApproved
	result.FSM.CapacityBlocked = move.Status.CapacityReason != "" && !move.Status.CapacityApproved
	if result.FSM.CapacityBlocked {
		result.FSM.UnsafeReason = move.Status.CapacityReason
	}
	result.FSM.ReplacementExists = result.Replacement != nil
	result.FSM.ReplacementHeld = result.Replacement != nil && hasPlacementHold(result.Replacement)
}

func (r *Reconciler) observeJobs(ctx context.Context, move volumeapi.Move, repaired bool, result *observation) error {
	var err error
	result.FSM.CopyComplete, result.FSM.CopyFailed, err = r.jobState(ctx, result.Names.CopyJob)
	if err != nil {
		return err
	}
	result.FSM.PromotionComplete, result.FSM.PromotionFailed, err = r.jobState(ctx, result.Names.PromotionJob)
	if err != nil {
		return err
	}
	// A helper may repair its own exact unrecorded path, but a completed Job
	// must still wait for the next ordinary valid inventory before authority can
	// advance to promotion or owner commit.
	if repaired && (move.Status.Phase == string(fsm.PhaseCopying) && result.FSM.CopyComplete ||
		move.Status.Phase == string(fsm.PhasePromoting) && result.FSM.PromotionComplete) {
		result.FSM.DestinationUnavailable = true
	}
	return nil
}

func observeDestinationPublish(move volumeapi.Move, pools poolIndex, result *observation) {
	destinationPool, destinationReady := indexedCopyPool(pools.ready, move.Status.DestinationCopy)
	result.FSM.PublishedOnDestination = result.DestinationNode != "" &&
		slices.Contains(result.Volume.PublishedNodes, result.DestinationNode) &&
		destinationReady &&
		volumeapi.PoolHasPublishedCopy(destinationPool, move.Status.DestinationCopy)
}

func (r *Reconciler) observeCleanup(ctx context.Context, move volumeapi.Move, result *observation) error {
	if move.Status.Phase != string(fsm.PhaseWaitingForDestinationPublish) && move.Status.Phase != string(fsm.PhaseCleaningSource) {
		return nil
	}
	complete, failed, err := r.cleanupState(ctx, move)
	if err != nil {
		return err
	}
	result.FSM.CleanupComplete, result.FSM.CleanupFailed = complete, failed
	return nil
}

func (r *Reconciler) jobState(ctx context.Context, name string) (complete, failed bool, err error) {
	job, err := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("read Job %q: %w", name, err)
	}
	for _, condition := range job.Status.Conditions {
		switch condition.Type {
		case batchv1.JobComplete:
			complete = condition.Status == corev1.ConditionTrue
		case batchv1.JobFailed:
			failed = condition.Status == corev1.ConditionTrue
		}
	}
	return complete, failed, nil
}
