package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

func (r *Reconciler) discoverMoves(ctx context.Context) (discoveryErr error) {
	deferred := make(map[string]int)
	if r.ObserveDiscovery != nil {
		defer func() { r.ObserveDiscovery(deferred, discoveryErr) }()
	}
	volumes, err := r.Repository.ListVolumes(ctx)
	if err != nil {
		return err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return err
	}
	activeByVolume := activeMoveVolumes(moves, volumes)
	ownerNodes := make(map[string]*corev1.Node)
	for volumeID, state := range volumes {
		if state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" || activeByVolume[volumeID] {
			continue
		}
		created, err := r.discoverVolumeMove(ctx, volumeID, state, deferred, ownerNodes)
		if err != nil {
			return err
		}
		if created {
			activeByVolume[volumeID] = true
		}
	}
	return nil
}

// activeMoveVolumes names the volumes a live transaction still speaks for, so
// discovery never opens a second one across a crash boundary.
func activeMoveVolumes(moves []volumeapi.Move, volumes map[string]volumeapi.State) map[string]bool {
	activeByVolume := make(map[string]bool)
	for _, move := range moves {
		if move.Status.RecoveryPhase == recoveryRecovered {
			continue
		}
		if move.Spec.Recovery == "ResumeOwner" && move.Status.Phase == string(fsm.PhaseBlocked) {
			// The final Volume CAS can precede recovery status persistence. Do not
			// discover a new transaction across that crash boundary on either owner.
			activeByVolume[move.Spec.VolumeID] = true
			continue
		}
		state, exists := volumes[move.Spec.VolumeID]
		if move.Status.Phase != string(fsm.PhaseSucceeded) &&
			(move.Status.Phase != string(fsm.PhaseBlocked) || (exists && state.OwnerNode == move.Spec.SourceNode)) {
			activeByVolume[move.Spec.VolumeID] = true
		}
	}
	return activeByVolume
}

// discoverVolumeMove opens one transaction for a Ready volume whose owner is
// cordoned. It reports whether a Move was created; an ineligible or
// still-schedulable owner is counted as deferred, never as an error.
func (r *Reconciler) discoverVolumeMove(ctx context.Context, volumeID string, state volumeapi.State, deferred map[string]int, ownerNodes map[string]*corev1.Node) (bool, error) {
	node, found := ownerNodes[state.OwnerNode]
	if !found {
		var err error
		node, err = r.Client.CoreV1().Nodes().Get(ctx, state.OwnerNode, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("read owner Node %q: %w", state.OwnerNode, err)
		}
		// Cache absence as well, only for this discovery pass. Preflight still
		// observes the live source before a transaction can be created.
		ownerNodes[state.OwnerNode] = node
	}
	if node == nil || !node.Spec.Unschedulable || !admission.NodeReady(node) {
		return false, nil
	}
	eligible, reason, err := r.preflightVolume(ctx, volumeID, state.OwnerNode)
	if err != nil {
		return false, fmt.Errorf("preflight volume %s: %w", volumeID, err)
	}
	if !eligible {
		deferred[reason]++
		klog.V(2).Infof("deferred ShiftPV mobility for volume %s: %s", volumeID, reason)
		return false, nil
	}
	move, err := r.Repository.CreateMove(ctx, moveGenerateName(volumeID), volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: state.OwnerNode})
	if err != nil {
		return false, err
	}
	previous := move.Status
	move.Status.Phase = string(fsm.PhasePending)
	move.Status.Message = mobilityMessage(fsm.PhasePending, "")
	if err := r.persistMoveStatus(ctx, &move, previous); err != nil {
		return false, err
	}
	klog.Infof("created ShiftPVMove %s for cordoned node %s volume %s", move.Name, state.OwnerNode, volumeID)
	return true, nil
}

func moveGenerateName(volumeID string) string {
	return "move-" + strings.TrimPrefix(volumeID, "shiftpv-") + "-"
}
