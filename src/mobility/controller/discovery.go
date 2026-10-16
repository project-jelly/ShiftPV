package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// discoverMoves is a synchronous discovery-only pass used by contract callers.
// The running controller invokes discoverVolume exclusively inside its Volume lane.
func (r *Reconciler) discoverMoves(ctx context.Context) error {
	observations := newDiscoveryObservations(r.ObserveDiscovery)
	hints, scanErr := r.scanVolumeWork(ctx, nil, observations)
	failures := []error{scanErr}
	for id, work := range hints {
		if !work.discover {
			continue
		}
		_, result, err := r.discoverVolume(ctx, id)
		observations.record(id, result)
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (r *Reconciler) discoverVolume(ctx context.Context, volumeID string) (volumeapi.Move, discoveryResult, error) {
	state, err := r.Repository.Get(ctx, volumeID)
	if apierrors.IsNotFound(err) {
		return volumeapi.Move{}, discoveryResult{}, nil
	}
	result := discoveryResult{uid: state.UID, err: err}
	if err != nil || state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" {
		return volumeapi.Move{}, result, err
	}
	deferred := map[string]int{}
	move, err := r.discoverVolumeMove(ctx, volumeID, state, deferred)
	result.err = err
	for reason := range deferred {
		result.reason = reason
	}
	return move, result, err
}

// activeMoveVolumes names the volumes a live transaction still speaks for, so
// discovery never opens a second one across a crash boundary.
func activeMoveVolumes(moves []volumeapi.Move, volumes map[string]volumeapi.State) map[string]bool {
	activeByVolume := make(map[string]bool)
	for _, move := range moves {
		state := volumes[move.Spec.VolumeID]
		if (!legacyMoveProtocol(move) && !supportedMoveProtocol(move)) || !volumeapi.MoveCleanupSettled(move) || state.ActiveMove == move.Name {
			activeByVolume[move.Spec.VolumeID] = true
		}
	}
	return activeByVolume
}

// Discovery observations are advisory until this lane rechecks the source and
// the complete Move list. A failed or response-lost creation is re-observed on
// the next pass, never followed by effects using an unconfirmed new journal.
func (r *Reconciler) discoverVolumeMove(ctx context.Context, volumeID string, state volumeapi.State, deferred map[string]int) (volumeapi.Move, error) {
	node, err := r.Client.CoreV1().Nodes().Get(ctx, state.OwnerNode, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return volumeapi.Move{}, fmt.Errorf("read owner Node %q: %w", state.OwnerNode, err)
	}
	if node == nil || !node.Spec.Unschedulable || !admission.NodeReady(node) {
		return volumeapi.Move{}, nil
	}
	eligible, reason, err := r.preflightVolume(ctx, volumeID, state.OwnerNode)
	if err != nil {
		return volumeapi.Move{}, fmt.Errorf("preflight volume %s: %w", volumeID, err)
	}
	if !eligible {
		deferred[reason]++
		klog.V(2).Infof("deferred ShiftPV mobility for volume %s: %s", volumeID, reason)
		return volumeapi.Move{}, nil
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return volumeapi.Move{}, err
	}
	if err := r.validateSettledMoves(ctx, movesForVolume(moves, volumeID)); err != nil {
		return volumeapi.Move{}, err
	}
	fresh, err := r.Repository.Get(ctx, volumeID)
	if err != nil {
		return volumeapi.Move{}, err
	}
	if fresh.UID != state.UID || fresh.OwnerNode != state.OwnerNode || fresh.Phase != volumeapi.PhaseReady || fresh.ActiveMove != "" ||
		fresh.Deleting || fresh.HasCleanup || activeMoveVolumes(moves, map[string]volumeapi.State{volumeID: fresh})[volumeID] {
		return volumeapi.Move{}, nil
	}
	move, err := r.Repository.CreateMove(ctx, moveGenerateName(volumeID), volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: fresh.OwnerNode, ProtocolVersion: volumeapi.MoveProtocolPreparedV1})
	if err != nil {
		return volumeapi.Move{}, err
	}
	previous := move.Status
	move.Status.Phase = string(fsm.PhasePending)
	move.Status.Message = mobilityMessage(fsm.PhasePending, "")
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		return volumeapi.Move{}, err
	}
	klog.Infof("created ShiftPVMove %s for cordoned node %s volume %s", move.Name, fresh.OwnerNode, volumeID)
	return move, nil
}

func moveGenerateName(volumeID string) string {
	return "move-" + strings.TrimPrefix(volumeID, "shiftpv-") + "-"
}
