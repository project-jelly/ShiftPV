package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

// Only bulk observations and routing hints belong on the coordinator. In
// particular, a slow Volume's preflight or binding GET cannot hold up the queue.
func (r *Reconciler) scanVolumeWork(ctx context.Context, publish func(volumeWork), observations *discoveryObservations) (map[string]volumeWork, error) {
	hints := map[string]volumeWork{}
	emit := func(work volumeWork) {
		hints[work.volumeID] = mergeVolumeWork(hints[work.volumeID], work)
		if publish != nil {
			publish(work)
		}
	}
	volumes, volumeErr := r.Repository.ListVolumes(ctx)
	if volumeErr == nil {
		observations.membership(volumes)
	}
	// A failed discovery LIST must not suppress independently observed journals.
	moves, moveErr := r.Repository.ListMoves(ctx)
	if moveErr == nil {
		for _, move := range moves {
			if r.moveNeedsWork(move, volumes, volumeErr == nil) {
				emit(volumeWork{volumeID: move.Spec.VolumeID, moves: []moveReference{{move.Name, move.UID}}})
			}
		}
	}
	var discoveryErr error
	if volumeErr == nil && moveErr == nil {
		discoveryErr = r.scanDiscovery(ctx, volumes, moves, emit, observations)
	}
	placementErr := r.scanOwnerPlacements(ctx, emit)
	usageErr := r.scanSourceUsage(ctx, moves, emit)
	err := errors.Join(volumeErr, moveErr, discoveryErr, placementErr, usageErr)
	observations.scanned(err)
	return hints, err
}

func (r *Reconciler) scanDiscovery(ctx context.Context, volumes map[string]volumeapi.State, moves []volumeapi.Move, emit func(volumeWork), observations *discoveryObservations) error {
	active := activeMoveVolumes(moves, volumes)
	candidates := map[string]volumeapi.State{}
	idle := map[string]discoveryResult{}
	for id, state := range volumes {
		if state.Phase == volumeapi.PhaseReady && state.ActiveMove == "" && !active[id] {
			candidates[id] = state
		} else {
			idle[id] = discoveryResult{uid: state.UID}
		}
	}
	if len(candidates) == 0 {
		observations.recordBatch(idle)
		return nil
	}
	nodes, err := r.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		observations.recordBatch(idle)
		return fmt.Errorf("list discovery Nodes: %w", err)
	}
	cordoned := map[string]bool{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		cordoned[node.Name] = node.Spec.Unschedulable && admission.NodeReady(node)
	}
	for id, state := range candidates {
		if cordoned[state.OwnerNode] {
			emit(volumeWork{volumeID: id, volumeUID: state.UID, discover: true})
		} else {
			idle[id] = discoveryResult{uid: state.UID}
		}
	}
	observations.recordBatch(idle)
	return nil
}

func (r *Reconciler) scanOwnerPlacements(ctx context.Context, emit func(volumeWork)) error {
	pods, err := r.Client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: admission.ManagedLabel + "=true"})
	if err != nil {
		return fmt.Errorf("list held workload Pods: %w", err)
	}
	var held []*corev1.Pod
	for i := range pods.Items {
		if ownerPlacementCandidate(&pods.Items[i]) {
			held = append(held, &pods.Items[i])
		}
	}
	if len(held) == 0 {
		return nil
	}
	pvs, err := r.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list owner-placement PV hints: %w", err)
	}
	for _, work := range ownerPlacementHints(held, pvs.Items) {
		emit(work)
	}
	return nil
}

func ownerPlacementHints(pods []*corev1.Pod, pvs []corev1.PersistentVolume) []volumeWork {
	claims := map[string][]string{}
	for _, pv := range pvs {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == admission.DriverName && pv.Spec.CSI.VolumeHandle != "" && pv.Spec.ClaimRef != nil {
			key := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
			claims[key] = append(claims[key], pv.Spec.CSI.VolumeHandle)
		}
	}
	var hints []volumeWork
	for _, pod := range pods {
		for _, mounted := range pod.Spec.Volumes {
			if mounted.PersistentVolumeClaim == nil {
				continue
			}
			for _, id := range claims[pod.Namespace+"/"+mounted.PersistentVolumeClaim.ClaimName] {
				hints = append(hints, volumeWork{volumeID: id, ownerPods: []podReference{{pod.Namespace, pod.Name, pod.UID}}})
			}
		}
	}
	return hints
}

// This only skips future, fully settled history. It never filters an authority
// LIST used by discovery, capacity accounting or an owner-placement writer.
func (r *Reconciler) moveNeedsWork(move volumeapi.Move, volumes map[string]volumeapi.State, volumesKnown bool) bool {
	if !volumesKnown || !supportedMoveProtocol(move) || !volumeapi.MoveCleanupSettled(move) || moveCleanupPending(move) ||
		len(move.Finalizers) != 0 || volumes[move.Spec.VolumeID].ActiveMove == move.Name {
		return true
	}
	transitioned, err := time.Parse(time.RFC3339Nano, move.Status.LastTransitionTime)
	if err != nil || r.now().Before(transitioned) {
		return true
	}
	retention := r.MoveJournalRetention
	if retention <= 0 {
		retention = DefaultMoveJournalRetention
	}
	return !r.now().Before(transitioned.Add(retention))
}

// Read-only helpers can outlive a terminal transition or controller restart.
// Mutable labels cannot hide a leftover. Namespace-local children route through
// the exact Move owner UID; only the worker may validate and retire its helper.
func (r *Reconciler) scanSourceUsage(ctx context.Context, moves []volumeapi.Move, emit func(volumeWork)) error {
	pods, err := r.Client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list Move-owned Pod hints: %w", err)
	}
	parents := make(map[moveReference]string, len(moves))
	for _, move := range moves {
		parents[moveReference{move.Name, move.UID}] = move.Spec.VolumeID
	}
	for i := range pods.Items {
		owner := metav1.GetControllerOf(&pods.Items[i])
		if owner == nil || owner.APIVersion != "shiftpv.io/v1alpha1" || owner.Kind != "ShiftPVMove" || owner.Name == "" || owner.UID == "" {
			continue
		}
		ref := moveReference{owner.Name, string(owner.UID)}
		if id := parents[ref]; id != "" {
			emit(volumeWork{volumeID: id, moves: []moveReference{ref}})
		}
	}
	return nil
}
