package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// A webhook can hold a Pod before any Move exists. Revisit those Pods in the
// same serial pass as discovery and Move execution, including after a restart.
func (r *Reconciler) reconcileOwnerPlacements(ctx context.Context) error {
	pods, err := r.Client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: admission.ManagedLabel + "=true"})
	if err != nil {
		return fmt.Errorf("list held workload Pods: %w", err)
	}
	var failures []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !ownerPlacementCandidate(pod) {
			continue
		}
		if err := r.reconcileOwnerPlacement(ctx, pod); err != nil {
			failures = append(failures, fmt.Errorf("restore Pod %s/%s owner placement: %w", pod.Namespace, pod.Name, err))
		}
	}
	return errors.Join(failures...)
}

func ownerPlacementCandidate(pod *corev1.Pod) bool {
	return pod.UID != "" && pod.Labels[admission.ManagedLabel] == "true" && pod.DeletionTimestamp == nil &&
		pod.Spec.NodeName == "" && (pod.Status.Phase == "" || pod.Status.Phase == corev1.PodPending) && hasPlacementHold(pod)
}

// ownerPlacementIdentity pins the identities observed during this attempt.
// Legacy gates have no admission-time binding identity; they are recovered from
// the current exact reciprocal binding, never from a claim name alone.
type ownerPlacementIdentity struct {
	claimName string
	claimUID  types.UID
	pvName    string
	pvUID     types.UID
	nodeUID   types.UID
	copy      volume.CopyIdentity
}

func (r *Reconciler) reconcileOwnerPlacement(ctx context.Context, observed *corev1.Pod) error {
	var expected *ownerPlacementIdentity
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := r.Client.CoreV1().Pods(observed.Namespace).Get(ctx, observed.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if pod.UID != observed.UID {
			return fmt.Errorf("Pod identity changed before owner placement")
		}
		if !ownerPlacementCandidate(pod) {
			return nil
		}
		identity, err := r.observeOwnerPlacement(ctx, pod)
		if err != nil || identity == nil {
			return err
		}
		if expected == nil {
			expected = identity
		} else if *expected != *identity {
			return fmt.Errorf("binding or serving-copy identity changed during owner placement")
		}
		moves, err := r.Repository.ListMoves(ctx)
		if err != nil {
			return fmt.Errorf("observe Moves before owner placement: %w", err)
		}
		for _, move := range moves {
			journalUnsettled := move.Status.CleanupPhase != "" && move.Status.CleanupPhase != cleanupapi.PhaseCompleted
			if move.Spec.VolumeID == identity.copy.VolumeID && (journalUnsettled || !volumeapi.MoveCleanupSettled(move)) {
				return nil
			}
		}
		// A Move LIST is not a transaction with the Volume or binding. Re-read
		// authority after it and on every Pod conflict; NodePublish remains the
		// final phase/owner fence if an external actor changes it after this read.
		fresh, err := r.observeOwnerPlacement(ctx, pod)
		if err != nil || fresh == nil {
			return err
		}
		if *fresh != *expected {
			return fmt.Errorf("binding or serving-copy identity changed before owner placement")
		}
		return r.releaseOwnerPlacement(ctx, pod, fresh.copy.NodeName)
	})
}

// Pin and release together: a successful Update never exposes an unpinned Pod.
func (r *Reconciler) releaseOwnerPlacement(ctx context.Context, pod *corev1.Pod, owner string) error {
	if selected, exists := pod.Spec.NodeSelector[corev1.LabelHostname]; exists && selected != owner {
		return fmt.Errorf("Pod requires node %q, not current owner %q", selected, owner)
	}
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[corev1.LabelHostname] = owner
	pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
		return gate.Name == placementHoldName
	})
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[placementAnnotationKey] = "owner"
	_, err := r.Client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	return err
}

func (r *Reconciler) observeOwnerPlacement(ctx context.Context, pod *corev1.Pod) (*ownerPlacementIdentity, error) {
	pv, claim, err := r.ownerPlacementBinding(ctx, pod)
	if err != nil || pv == nil {
		return nil, err
	}
	volumeID := pv.Spec.CSI.VolumeHandle
	state, err := r.Repository.Get(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("read ShiftPVVolume: %w", err)
	}
	if state.Deleting || state.HasCleanup || state.Phase != volumeapi.PhaseReady || state.ActiveMove != "" {
		return nil, nil
	}
	copy := state.CurrentCopy
	if copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing || copy.VolumeID != volumeID ||
		copy.VolumeUID != state.UID || copy.NodeName != state.OwnerNode {
		return nil, fmt.Errorf("Volume has no exact serving-copy identity")
	}
	node, err := r.Client.CoreV1().Nodes().Get(ctx, state.OwnerNode, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read owner Node: %w", err)
	}
	if node.DeletionTimestamp != nil || node.Spec.Unschedulable || !admission.NodeReady(node) {
		return nil, nil
	}
	return &ownerPlacementIdentity{claimName: claim.Name, claimUID: claim.UID, pvName: pv.Name, pvUID: pv.UID, nodeUID: node.UID, copy: *copy}, nil
}

func (r *Reconciler) ownerPlacementBinding(ctx context.Context, pod *corev1.Pod) (*corev1.PersistentVolume, *corev1.PersistentVolumeClaim, error) {
	var boundPV *corev1.PersistentVolume
	var boundClaim *corev1.PersistentVolumeClaim
	seenClaims := map[string]bool{}
	for _, podVolume := range pod.Spec.Volumes {
		if podVolume.PersistentVolumeClaim == nil || seenClaims[podVolume.PersistentVolumeClaim.ClaimName] {
			continue
		}
		claimName := podVolume.PersistentVolumeClaim.ClaimName
		seenClaims[claimName] = true
		claim, err := r.Client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("read PVC: %w", err)
		}
		if claim.Spec.VolumeName == "" {
			continue
		}
		pv, err := r.Client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("read PV: %w", err)
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != admission.DriverName {
			continue
		}
		if boundPV != nil {
			return nil, nil, fmt.Errorf("multiple bound ShiftPV volumes are unsupported")
		}
		volumeID := pv.Spec.CSI.VolumeHandle
		if pv.UID == "" || !validBinding(pv, claim, volumeID) {
			return nil, nil, fmt.Errorf("PV/PVC binding identity is invalid")
		}
		boundPV, boundClaim = pv, claim
	}
	return boundPV, boundClaim, nil
}
