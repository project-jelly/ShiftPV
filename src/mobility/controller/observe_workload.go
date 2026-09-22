// PV/PVC binding, consumers, replacements, and placement reservation observations.
package controller

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

// observeBinding resolves the PV/PVC pair this volume is still bound to and the
// namespace opt-in that makes its workload movable at all.
func (r *Reconciler) observeBinding(ctx context.Context, move volumeapi.Move, result *observation) (bool, error) {
	pv, err := r.boundPersistentVolume(ctx, move)
	if err != nil {
		return false, err
	}
	result.PV = pv
	if result.PV == nil || result.PV.Spec.ClaimRef == nil {
		result.FSM.UnsafeReason = "VolumeBindingMissing"
		result.FSM.SourceAuthorityInvalid = true
		return true, nil
	}
	claimRef := result.PV.Spec.ClaimRef
	claim, err := r.Client.CoreV1().PersistentVolumeClaims(claimRef.Namespace).Get(ctx, claimRef.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			result.FSM.UnsafeReason = "VolumeBindingMissing"
			result.FSM.SourceAuthorityInvalid = true
			return true, nil
		}
		return false, fmt.Errorf("read PVC: %w", err)
	}
	result.Claim = claim
	// Names can be reused after PVC/namespace deletion while Retain PVs and
	// ShiftPVVolumes survive. Never associate that old volume with the new Pod.
	if !validBinding(result.PV, claim, move.Spec.VolumeID) {
		result.FSM.UnsafeReason = "VolumeBindingMismatch"
		result.FSM.SourceAuthorityInvalid = true
		return true, nil
	}
	if result.Volume.OwnerNode != move.Spec.SourceNode && !result.FSM.OwnerCommitted {
		result.FSM.UnsafeReason = "OwnerMismatch"
		result.FSM.SourceAuthorityInvalid = true
		return true, nil
	}
	namespace, err := r.Client.CoreV1().Namespaces().Get(ctx, claim.Namespace, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("read workload Namespace: %w", err)
	}
	if namespace.Labels[admissionNamespaceLabel] != "enabled" {
		result.FSM.UnsafeReason = "AdmissionNotEnabled"
		result.FSM.PreflightDeferred = preEviction(move)
		return true, nil
	}
	return false, nil
}

// A started Move has already persisted its PV name. Read that exact object
// and retain validBinding's driver, handle, deletion and claim UID checks.
func (r *Reconciler) boundPersistentVolume(ctx context.Context, move volumeapi.Move) (*corev1.PersistentVolume, error) {
	if move.Status.PersistentVolumeName != "" {
		pv, err := r.Client.CoreV1().PersistentVolumes().Get(ctx, move.Status.PersistentVolumeName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read PersistentVolume: %w", err)
		}
		return pv, nil
	}
	list, err := r.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == admission.DriverName && pv.Spec.CSI.VolumeHandle == move.Spec.VolumeID {
			return pv.DeepCopy(), nil
		}
	}
	return nil, nil
}

// observeConsumers separates the Pod this transaction is moving from any other
// live Pod on the claim, which after eviction is the replacement workload.
func (r *Reconciler) observeConsumers(ctx context.Context, move volumeapi.Move, result *observation) (bool, error) {
	claim := result.Claim
	pods, err := r.Client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list consumer Pods: %w", err)
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if !podUsesClaim(pod, claim.Name) || terminalPod(pod) {
			continue
		}
		if move.Status.ConsumerName != "" && pod.Name == move.Status.ConsumerName &&
			(move.Status.ConsumerUID == "" || string(pod.UID) == move.Status.ConsumerUID) {
			result.Consumer = pod.DeepCopy()
			continue
		}
		if move.Status.ConsumerName == "" && pod.Spec.NodeName == move.Spec.SourceNode {
			if result.Consumer != nil {
				result.FSM.UnsafeReason = "MultipleConsumers"
				result.FSM.PreflightDeferred = preEviction(move)
				return true, nil
			}
			result.Consumer = pod.DeepCopy()
			continue
		}
		if result.Replacement == nil {
			result.Replacement = pod.DeepCopy()
		}
	}
	return false, nil
}

// observeReplacement rejects a replacement Pod that the scheduler or a user has
// pinned somewhere this transaction never approved. Before eviction there is no
// replacement to judge, only the original consumer.
func observeReplacement(move volumeapi.Move, result *observation) {
	if result.Replacement == nil || preEviction(move) {
		return
	}
	if selected := result.Replacement.Spec.NodeSelector["kubernetes.io/hostname"]; selected != "" && !slices.Contains(result.CandidateNodes, selected) {
		result.FSM.DestinationBlocked = true
		result.FSM.UnsafeReason = "UnsupportedSchedulingConstraint"
	}
	if result.Replacement.Spec.NodeName != "" && result.DestinationNode != "" && result.Replacement.Spec.NodeName != result.DestinationNode {
		result.FSM.DestinationBlocked = true
		result.FSM.UnsafeReason = "InvalidDestination"
	}
}

func (r *Reconciler) observePlacement(ctx context.Context, move volumeapi.Move, result *observation) error {
	var placementErr error
	// A Move without a name or a phase owns no reservation: its create response
	// may have been lost before any status was persisted, and the placement Pod
	// name is derived from a Move name that no object carries yet.
	if move.Name != "" && move.Status.Phase != "" {
		placement, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, result.Names.PlacementPod, metav1.GetOptions{})
		if err == nil {
			result.Placement = placement
		} else {
			placementErr = err
		}
	}
	if result.Placement == nil {
		if placementErr != nil && !apierrors.IsNotFound(placementErr) {
			return fmt.Errorf("read placement reservation Pod: %w", placementErr)
		}
		return nil
	}
	placement := result.Placement
	// Keep the workload held until the reservation is actually NotFound. A
	// deletion timestamp starts termination but does not prove that scheduler
	// capacity has been released or that the exact object has disappeared.
	result.FSM.PlacementExists = true
	if identityErr := validatePlacementIdentity(placement, move, result.Names); identityErr != nil {
		result.FSM.DestinationBlocked = true
		result.FSM.UnsafeReason = "PlacementReservationConflict"
	} else if placement.DeletionTimestamp != nil {
		// Wait for API disappearance before recreating the reservation or
		// releasing the held workload.
	} else if placement.Status.Phase == corev1.PodFailed {
		result.FSM.DestinationBlocked = true
		result.FSM.UnsafeReason = "PlacementReservationFailed"
	} else if placement.Spec.NodeName != "" {
		if move.Status.DestinationNode != "" && placement.Spec.NodeName != move.Status.DestinationNode {
			result.FSM.DestinationBlocked = true
			result.FSM.UnsafeReason = "InvalidDestination"
		} else if slices.Contains(result.CandidateNodes, placement.Spec.NodeName) {
			result.DestinationNode = placement.Spec.NodeName
			result.FSM.DestinationScheduled = true
		} else {
			result.FSM.DestinationBlocked = true
			result.FSM.UnsafeReason = "InvalidDestination"
		}
	}
	return nil
}

// validBinding is the single PV/PVC binding rule. Names can be reused after
// PVC/namespace deletion while Retain PVs and ShiftPVVolumes survive, so the
// pair must still be alive, still be this driver's volume, and still reference
// each other by exact UID. Each caller keeps its own diagnosis for a failure.
func validBinding(pv *corev1.PersistentVolume, claim *corev1.PersistentVolumeClaim, volumeID string) bool {
	if pv == nil || claim == nil {
		return false
	}
	ref := pv.Spec.ClaimRef
	return pv.DeletionTimestamp == nil && claim.DeletionTimestamp == nil &&
		pv.Spec.CSI != nil && pv.Spec.CSI.Driver == admission.DriverName && pv.Spec.CSI.VolumeHandle == volumeID &&
		ref != nil && ref.Namespace == claim.Namespace && ref.Name == claim.Name &&
		ref.UID != "" && ref.UID == claim.UID && claim.Spec.VolumeName == pv.Name
}

func podUsesClaim(pod *corev1.Pod, claimName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claimName {
			return true
		}
	}
	return false
}

func terminalPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func hasPlacementHold(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == placementHoldName {
			return true
		}
	}
	return false
}
