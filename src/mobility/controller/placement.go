package controller

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	resourcehelper "k8s.io/component-helpers/resource"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

const placementRole = "placement"

func (r *Reconciler) ensurePlacement(ctx context.Context, move *volumeapi.Move, observed observation) error {
	if observed.Replacement == nil || !hasPlacementHold(observed.Replacement) {
		return fmt.Errorf("held replacement Pod is not observed")
	}
	if len(move.Status.CandidateNodes) == 0 {
		return fmt.Errorf("move has no recorded destination candidates")
	}
	names := namesFor(move.Name)
	desired := r.placementPod(*move, observed.Replacement, names)
	_, err := r.ensurePlacementPod(ctx, *move, desired)
	return err
}

func (r *Reconciler) ensurePlacementPod(ctx context.Context, move volumeapi.Move, desired *corev1.Pod) (*corev1.Pod, error) {
	names := namesFor(move.Name)
	existing, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existing, err = r.Client.CoreV1().Pods(r.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		if err == nil {
			return existing, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create placement reservation Pod: %w", err)
		}
		existing, err = r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read concurrent placement reservation Pod: %w", err)
		}
		return existing, validatePlacementIdentity(existing, move, names)
	}
	if err != nil {
		return nil, fmt.Errorf("read placement reservation Pod: %w", err)
	}
	return existing, validatePlacementIdentity(existing, move, names)
}

func (r *Reconciler) placementPod(move volumeapi.Move, replacement *corev1.Pod, names resourceNames) *corev1.Pod {
	candidates := append([]string(nil), move.Status.CandidateNodes...)
	if move.Status.DestinationNode != "" {
		candidates = []string{move.Status.DestinationNode}
	}
	sort.Strings(candidates)
	affinity := placementAffinity(replacement.Spec.Affinity, candidates)
	zero := int64(0)
	controller := true
	blockOwnerDeletion := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.PlacementPod,
			Namespace: r.Namespace,
			Labels:    placementLabels(names, move),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name,
				UID: types.UID(move.UID), Controller: &controller, BlockOwnerDeletion: &blockOwnerDeletion,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: &zero,
			AutomountServiceAccountToken:  boolPointer(false),
			NodeSelector:                  copyStringMap(replacement.Spec.NodeSelector),
			Affinity:                      affinity,
			Tolerations:                   append([]corev1.Toleration(nil), replacement.Spec.Tolerations...),
			SchedulerName:                 replacement.Spec.SchedulerName,
			PriorityClassName:             replacement.Spec.PriorityClassName,
			PreemptionPolicy:              replacement.Spec.PreemptionPolicy,
			RuntimeClassName:              replacement.Spec.RuntimeClassName,
			HostNetwork:                   replacement.Spec.HostNetwork,
			OS:                            replacement.Spec.OS,
			Containers: []corev1.Container{{
				Name:    "reservation",
				Image:   r.HelperImage,
				Command: []string{"/bin/sh", "-c", "exec sleep 2147483647"},
				Ports:   placementPorts(replacement),
				Resources: corev1.ResourceRequirements{
					Requests: resourcehelper.PodRequests(replacement, resourcehelper.PodResourcesOptions{ExcludeOverhead: true}),
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPointer(false),
					RunAsNonRoot:             boolPointer(true),
					RunAsUser:                int64Pointer(65532),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: boolPointer(true), RunAsUser: int64Pointer(65532), RunAsGroup: int64Pointer(65532),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
		},
	}
}

func (r *Reconciler) requireScheduledPlacement(ctx context.Context, move volumeapi.Move) error {
	names := namesFor(move.Name)
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read placement reservation before owner commit: %w", err)
	}
	if err := validatePlacementIdentity(pod, move, names); err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" || pod.Spec.NodeName != move.Status.DestinationNode {
		return fmt.Errorf("placement reservation is not active on destination %q", move.Status.DestinationNode)
	}
	return nil
}

func placementAffinity(original *corev1.Affinity, candidates []string) *corev1.Affinity {
	affinity := &corev1.Affinity{}
	if original != nil && original.NodeAffinity != nil {
		affinity.NodeAffinity = original.NodeAffinity.DeepCopy()
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	requirement := corev1.NodeSelectorRequirement{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: candidates}
	required := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{requirement}}},
		}
		return affinity
	}
	for index := range required.NodeSelectorTerms {
		required.NodeSelectorTerms[index].MatchExpressions = append(required.NodeSelectorTerms[index].MatchExpressions, requirement)
	}
	return affinity
}

func placementPorts(pod *corev1.Pod) []corev1.ContainerPort {
	seen := map[string]bool{}
	result := []corev1.ContainerPort{}
	appendPorts := func(containers []corev1.Container) {
		for _, container := range containers {
			for _, port := range container.Ports {
				if port.HostPort == 0 {
					continue
				}
				key := fmt.Sprintf("%s/%s/%d", port.HostIP, port.Protocol, port.HostPort)
				if seen[key] {
					continue
				}
				seen[key] = true
				port.Name = ""
				result = append(result, port)
			}
		}
	}
	appendPorts(pod.Spec.InitContainers)
	appendPorts(pod.Spec.Containers)
	return result
}

func (r *Reconciler) pinReplacement(ctx context.Context, move volumeapi.Move) error {
	if move.Status.ReplacementName == "" || move.Status.ReplacementUID == "" || move.Status.ClaimNamespace == "" || move.Status.DestinationNode == "" {
		return fmt.Errorf("move has no durable replacement identity or destination")
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := r.Client.CoreV1().Pods(move.Status.ClaimNamespace).Get(ctx, move.Status.ReplacementName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(pod.UID) != move.Status.ReplacementUID {
			return fmt.Errorf("replacement Pod UID changed")
		}
		if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || !hasPlacementHold(pod) {
			return fmt.Errorf("replacement Pod is not safely held before scheduling")
		}
		if selected := pod.Spec.NodeSelector[corev1.LabelHostname]; selected != "" && selected != move.Status.DestinationNode {
			return fmt.Errorf("replacement Pod requires node %q, not selected destination %q", selected, move.Status.DestinationNode)
		}
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}
		pod.Spec.NodeSelector[corev1.LabelHostname] = move.Status.DestinationNode
		_, err = r.Client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		return err
	})
}

func (r *Reconciler) deletePlacement(ctx context.Context, move volumeapi.Move) error {
	names := namesFor(move.Name)
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.PlacementPod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read placement reservation Pod: %w", err)
	}
	if err := validatePlacementIdentity(pod, move, names); err != nil {
		return err
	}
	uid := pod.UID
	if err := r.Client.CoreV1().Pods(r.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete placement reservation Pod: %w", err)
	}
	return nil
}

func validatePlacementIdentity(pod *corev1.Pod, move volumeapi.Move, names resourceNames) error {
	if move.Status.PlacementPodUID != "" && string(pod.UID) != move.Status.PlacementPodUID {
		return fmt.Errorf("placement reservation Pod UID changed")
	}
	if pod.Labels["shiftpv.io/move"] != names.Base || pod.Labels["shiftpv.io/role"] != placementRole {
		return fmt.Errorf("placement reservation Pod %q labels do not match move", pod.Name)
	}
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "shiftpv.io/v1alpha1" && owner.Kind == "ShiftPVMove" && owner.Name == move.Name && string(owner.UID) == move.UID && move.UID != "" {
			return nil
		}
	}
	return fmt.Errorf("placement reservation Pod %q is not owned by move %q", pod.Name, move.Name)
}

func placementLabels(names resourceNames, moves ...volumeapi.Move) map[string]string {
	labels := transferLabels(names, moves...)
	labels["shiftpv.io/role"] = placementRole
	return labels
}

func copyStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
