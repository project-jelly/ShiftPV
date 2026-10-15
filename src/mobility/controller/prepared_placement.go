package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func (r *Reconciler) ensurePreparedPlacement(ctx context.Context, move volumeapi.Move, observed observation, candidates []string) (*corev1.Pod, string, error) {
	if r.PlacementPriorityClassName == "" {
		return nil, "", fmt.Errorf("preparation requires a non-preempting placement PriorityClass")
	}
	template, reason, err := r.consumerTemplate(ctx, observed.Consumer)
	if err != nil || reason != "" {
		return nil, reason, err
	}
	live := placementProbePod(observed.Consumer, template, move.Spec.SourceNode)
	desiredMove := move
	desiredMove.Status.CandidateNodes = candidates
	desired := r.placementPod(desiredMove, live, namesFor(move.Name))
	never := corev1.PreemptNever
	zero := int32(0)
	// Priority admission derives both fields from the named PriorityClass.
	// Explicit expectations fail closed if that class is misconfigured.
	desired.Spec.PriorityClassName = r.PlacementPriorityClassName
	desired.Spec.Priority = &zero
	desired.Spec.PreemptionPolicy = &never
	placement, err := r.ensurePlacementPod(ctx, move, desired)
	if err != nil || placement == nil || placement.DeletionTimestamp != nil {
		return nil, "", err
	}
	if !nonPreemptingPlacement(placement) {
		return nil, "PreparedPlacementPriorityChanged", nil
	}
	if reason := preparedPlacementCoverage(placement, live, template); reason != "" {
		return nil, reason, nil
	}
	// No hold has been granted, so a stale candidate set or rejected selected
	// node may be discarded. The next pass observes NotFound before creation.
	stale := placement.Spec.NodeName != "" && !slices.Contains(candidates, placement.Spec.NodeName)
	if placement.Spec.NodeName == "" {
		stale = !reflect.DeepEqual(placement.Spec.Affinity, desired.Spec.Affinity) ||
			!maps.Equal(placement.Spec.NodeSelector, desired.Spec.NodeSelector)
	}
	if stale {
		return nil, "", r.deletePlacement(ctx, move)
	}
	return placement, "", nil
}

func (r *Reconciler) validatePreparedPlacement(ctx context.Context, move volumeapi.Move, observed observation) (string, error) {
	if err := volumeapi.ValidateMovePreparation(move); err != nil {
		return "", err
	}
	if move.Status.Preparation == nil {
		return "PreparationMissing", nil
	}
	names := namesFor(move.Name)
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "PreparedPlacementMissing", nil
	}
	if err != nil {
		return "", err
	}
	if validatePlacementIdentity(pod, move, names) != nil || !nonPreemptingPlacement(pod) || pod.DeletionTimestamp != nil ||
		pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded ||
		pod.Spec.NodeName != move.Status.Preparation.NodeName {
		return "PreparedPlacementChanged", nil
	}
	if !slices.Contains(observed.CandidateNodes, pod.Spec.NodeName) {
		return "PreparedDestinationUnavailable", nil
	}
	preparation := move.Status.Preparation
	pool, err := r.Repository.ReadyPoolForIdentity(ctx, preparation.Pool.Name, preparation.Pool.UID, preparation.NodeName)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, volumeapi.ErrPoolNotReady) || errors.Is(err, volumeapi.ErrPoolNotFound) {
			return "PreparedDestinationUnavailable", nil
		}
		return "", err
	}
	if valid, err := r.preparationPoolGroup(ctx, move, pool); err != nil || !valid {
		return "PreparedDestinationChanged", err
	}
	consumer := observed.Consumer
	if consumer == nil {
		consumer = observed.Replacement
	}
	if consumer == nil {
		// StartingDisruption may have evicted the original before a crash.
		// The replacement is checked again before execution approval.
		return "", nil
	}
	return r.preparedWorkloadFits(ctx, move, observed.PV, pod, consumer)
}

func nonPreemptingPlacement(pod *corev1.Pod) bool {
	return pod.Spec.PriorityClassName != "" && pod.Spec.Priority != nil && *pod.Spec.Priority == 0 &&
		pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == corev1.PreemptNever
}

func (r *Reconciler) preparedWorkloadFits(ctx context.Context, move volumeapi.Move, pv *corev1.PersistentVolume, placement, consumer *corev1.Pod) (string, error) {
	template, reason, err := r.consumerTemplate(ctx, consumer)
	if err != nil || reason != "" {
		return reason, err
	}
	live := placementProbePod(consumer, template, move.Spec.SourceNode)
	// A replacement's ShiftPV hold is intentional; other scheduling gates
	// remain unsupported by the ordinary preflight contract.
	live.Spec.SchedulingGates = slices.DeleteFunc(live.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
		return gate.Name == placementHoldName
	})
	if reason := unsupportedConsumerPlacement(live.Spec, template.Spec); reason != "" {
		return reason, nil
	}
	affinity, reason := requiredPVAffinity(pv)
	if reason != "" {
		return reason, nil
	}
	nodes, reason, err := r.compatibleDestinations(ctx, []string{placement.Spec.NodeName}, live, template, affinity)
	if err != nil || reason != "" {
		return reason, err
	}
	if len(nodes) == 0 {
		return "PreparedWorkloadPlacementChanged", nil
	}
	return preparedPlacementCoverage(placement, live, template), nil
}

// Compare the resources actually reserved with requirements, not whole Pod
// specs. Admission/LimitRange/defaulting may enlarge the live Pod relative to
// its raw template; that remains safe when the reservation covers both.
func preparedPlacementCoverage(placement, consumer *corev1.Pod, template *corev1.PodTemplateSpec) string {
	reserved := resourcehelper.PodRequests(placement, resourcehelper.PodResourcesOptions{})
	for _, requestedPod := range []*corev1.Pod{consumer, preparedTemplatePod(template)} {
		requests := resourcehelper.PodRequests(requestedPod, resourcehelper.PodResourcesOptions{})
		for name, requested := range requests {
			available := reserved[name]
			if available.Cmp(requested) < 0 {
				return "PreparedResourceRequestsChanged"
			}
		}
		if !preparedPortsCover(placement, requestedPod) {
			return "PreparedHostPortsChanged"
		}
	}
	if !preparedRuntimeMatches(placement.Spec, consumer.Spec, false) ||
		!preparedRuntimeMatches(placement.Spec, template.Spec, true) {
		return "PreparedRuntimeChanged"
	}
	return ""
}

// API defaulting uses a container limit as its request when that request was
// omitted. Account for that known default without predicting future admission.
func preparedTemplatePod(template *corev1.PodTemplateSpec) *corev1.Pod {
	pod := &corev1.Pod{Spec: *template.Spec.DeepCopy()}
	defaultRequests := func(resources *corev1.ResourceRequirements) {
		if resources.Requests == nil {
			resources.Requests = corev1.ResourceList{}
		}
		for name, limit := range resources.Limits {
			if _, exists := resources.Requests[name]; !exists {
				resources.Requests[name] = limit.DeepCopy()
			}
		}
	}
	for i := range pod.Spec.Containers {
		defaultRequests(&pod.Spec.Containers[i].Resources)
	}
	for i := range pod.Spec.InitContainers {
		defaultRequests(&pod.Spec.InitContainers[i].Resources)
	}
	if pod.Spec.Resources != nil {
		defaultRequests(pod.Spec.Resources)
	}
	return pod
}

func preparedRuntimeMatches(reserved, requested corev1.PodSpec, template bool) bool {
	if reserved.HostNetwork != requested.HostNetwork {
		return false
	}
	// An omitted raw template field may be supplied by admission. Explicit
	// template requirements and the actual live Pod must match the reservation.
	if requested.RuntimeClassName != nil {
		if reserved.RuntimeClassName == nil || *requested.RuntimeClassName != *reserved.RuntimeClassName {
			return false
		}
	} else if !template && reserved.RuntimeClassName != nil {
		return false
	}
	return requested.OS == nil || (reserved.OS != nil && requested.OS.Name == reserved.OS.Name)
}

func preparedPortsCover(placement, requested *corev1.Pod) bool {
	type port struct {
		ip       string
		protocol corev1.Protocol
		number   int32
	}
	key := func(value corev1.ContainerPort) port {
		protocol := value.Protocol
		if protocol == "" {
			protocol = corev1.ProtocolTCP
		}
		return port{value.HostIP, protocol, value.HostPort}
	}
	reserved := make(map[port]bool)
	for _, value := range placementPorts(placement) {
		reserved[key(value)] = true
	}
	for _, value := range placementPorts(requested) {
		if !reserved[key(value)] {
			return false
		}
	}
	return true
}
