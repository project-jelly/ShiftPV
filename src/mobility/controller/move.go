package controller

import (
	"context"
	"errors"

	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func (r *Reconciler) reconcileMove(ctx context.Context, move volumeapi.Move) error {
	previous := move.Status
	if move.Status.Phase == "" {
		move.Status.Phase = string(fsm.PhasePending)
		move.Status.Message = mobilityMessage(fsm.PhasePending, "")
		return r.persistMoveStatus(ctx, &move, previous)
	}
	observed, err := r.observe(ctx, move)
	if err != nil {
		return r.recordMoveError(ctx, &move, previous, "ObservationFailed", "failed to observe Kubernetes state", err)
	}
	if observed.PendingObsolete {
		klog.Infof("deleting unstarted ShiftPVMove %s because source node %s is schedulable", move.Name, move.Spec.SourceNode)
		if err := r.Repository.RemoveMoveFinalizer(ctx, move.Name, move.UID); err != nil {
			return err
		}
		return r.Repository.DeleteMove(ctx, move.Name, move.UID)
	}
	decision, err := fsm.Decide(fsm.Phase(move.Status.Phase), observed.FSM)
	if err != nil {
		return r.recordMoveError(ctx, &move, previous, "ActionFailed", "failed to decide the next mobility action", err)
	}
	if err := r.execute(ctx, &move, observed, decision); err != nil {
		if errors.Is(err, errCapacityApprovalPersistence) {
			return err
		}
		return r.recordMoveError(ctx, &move, previous, "ActionFailed", "failed to execute the current mobility action", err)
	}
	move.Status.Phase = string(decision.Next)
	move.Status.Reason = decision.Reason
	move.Status.Message = mobilityMessage(decision.Next, decision.Reason)
	return r.persistMoveStatus(ctx, &move, previous)
}
