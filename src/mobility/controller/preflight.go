package controller

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// preflight only rejects known constraints. It neither reserves scheduler resources
// nor predicts mutations that future admission/workload reconciliation may apply.
func (r *Reconciler) preflight(ctx context.Context, observed *observation) (string, error) {
	pod := observed.Consumer
	if pod == nil || pod.DeletionTimestamp != nil {
		return "ConsumerUnavailable", nil
	}
	if observed.Replacement != nil {
		return "MultipleConsumers", nil
	}
	template, reason, err := r.consumerTemplate(ctx, pod)
	if err != nil || reason != "" {
		return reason, err
	}
	if reason := unsupportedConsumerPlacement(pod.Spec, template.Spec); reason != "" {
		return reason, nil
	}
	live := placementProbePod(pod, template, observed.Volume.OwnerNode)
	pvAffinity, reason := requiredPVAffinity(observed.PV)
	if reason != "" {
		return reason, nil
	}
	candidates, reason, err := r.compatibleDestinations(ctx, observed.CandidateNodes, live, template, pvAffinity)
	if err != nil || reason != "" {
		return reason, err
	}
	if len(candidates) == 0 {
		return "NoCompatibleDestination", nil
	}
	sort.Strings(candidates)
	observed.CandidateNodes = candidates
	return r.preflightPDB(ctx, pod)
}

// unsupportedConsumerPlacement reports the first placement constraint that
// ShiftPV cannot reproduce on a destination node, checking the live Pod before
// the controller template.
func unsupportedConsumerPlacement(pod, template corev1.PodSpec) string {
	if len(pvcNames(pod)) != 1 || len(pvcNames(template)) != 1 {
		return "MultiplePVCsUnsupported"
	}
	if reason := unsupportedPlacement(pod); reason != "" {
		return reason
	}
	if reason := unsupportedPlacement(template); reason != "" {
		return reason
	}
	if template.NodeName != "" {
		return "ExplicitNodeNameUnsupported"
	}
	return ""
}

// placementProbePod copies the live consumer for scheduling checks. A
// controller's template is the source of the next Pod, not the scheduler's
// assigned nodeName on the current Pod. Preserve all other live constraints.
func placementProbePod(pod *corev1.Pod, template *corev1.PodTemplateSpec, ownerNode string) *corev1.Pod {
	live := pod.DeepCopy()
	if live.Annotations[placementAnnotationKey] == "owner" &&
		template.Spec.NodeSelector[corev1.LabelHostname] == "" &&
		live.Spec.NodeSelector[corev1.LabelHostname] == ownerNode {
		delete(live.Spec.NodeSelector, corev1.LabelHostname)
	}
	return live
}

// requiredPVAffinity compiles the PersistentVolume's required node affinity, or
// reports the rejection reason for one the scheduler could not parse either.
func requiredPVAffinity(pv *corev1.PersistentVolume) (*nodeaffinity.NodeSelector, string) {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, ""
	}
	selector, err := nodeaffinity.NewNodeSelector(pv.Spec.NodeAffinity.Required)
	if err != nil {
		return nil, "InvalidPVNodeAffinity"
	}
	return selector, ""
}

// compatibleDestinations reads each candidate Node in order and keeps the ones
// that satisfy every live and template placement constraint.
func (r *Reconciler) compatibleDestinations(ctx context.Context, candidateNodes []string, live *corev1.Pod,
	template *corev1.PodTemplateSpec, pvAffinity *nodeaffinity.NodeSelector) ([]string, string, error) {
	liveAffinity := nodeaffinity.GetRequiredNodeAffinity(live)
	templateAffinity := nodeaffinity.GetRequiredNodeAffinity(&corev1.Pod{Spec: template.Spec})
	var candidates []string
	for _, nodeName := range candidateNodes {
		node, err := r.Client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("preflight destination Node: %w", err)
		}
		liveMatch, liveErr := liveAffinity.Match(node)
		templateMatch, templateErr := templateAffinity.Match(node)
		if liveErr != nil || templateErr != nil {
			return nil, "InvalidNodeAffinity", nil
		}
		if schedulableDestination(node, live.Spec, template.Spec, liveMatch, templateMatch, pvAffinity) {
			candidates = append(candidates, nodeName)
		}
	}
	return candidates, "", nil
}

func schedulableDestination(node *corev1.Node, live, template corev1.PodSpec, liveMatch, templateMatch bool,
	pvAffinity *nodeaffinity.NodeSelector) bool {
	return admission.NodeReady(node) && !node.Spec.Unschedulable && liveMatch && templateMatch &&
		(pvAffinity == nil || pvAffinity.Match(node)) && toleratesPlacement(live, node) && toleratesPlacement(template, node)
}

func (r *Reconciler) consumerTemplate(ctx context.Context, pod *corev1.Pod) (*corev1.PodTemplateSpec, string, error) {
	owner := metav1.GetControllerOf(pod)
	if owner == nil {
		return nil, "BarePodUnsupported", nil
	}
	var object metav1.Object
	var template *corev1.PodTemplateSpec
	var err error
	if owner.APIVersion == "apps/v1" {
		switch owner.Kind {
		case "ReplicaSet":
			rs, getErr := r.Client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
			err = getErr
			if err == nil {
				object, template = rs, &rs.Spec.Template
			}
		case "StatefulSet":
			sts, getErr := r.Client.AppsV1().StatefulSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
			err = getErr
			if err == nil {
				object, template = sts, &sts.Spec.Template
			}
		}
	}
	if apierrors.IsNotFound(err) {
		return nil, "ConsumerControllerMissing", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("read consumer controller: %w", err)
	}
	if object == nil {
		return nil, "UnsupportedWorkloadController", nil
	}
	if owner.UID == "" || object.GetUID() != owner.UID || object.GetDeletionTimestamp() != nil {
		return nil, "ConsumerControllerChanged", nil
	}
	return template, "", nil
}

func unsupportedPlacement(spec corev1.PodSpec) string {
	if spec.SchedulerName != "" && spec.SchedulerName != corev1.DefaultSchedulerName {
		return "CustomSchedulerUnsupported"
	}
	if len(spec.SchedulingGates) != 0 {
		return "SchedulingGateUnsupported"
	}
	if a := spec.Affinity; a != nil {
		if a.PodAffinity != nil || a.PodAntiAffinity != nil {
			return "InterPodAffinityUnsupported"
		}
	}
	if len(spec.TopologySpreadConstraints) != 0 {
		return "TopologySpreadUnsupported"
	}
	if len(spec.ResourceClaims) != 0 {
		return "ResourceClaimUnsupported"
	}
	for _, volume := range spec.Volumes {
		if volume.Ephemeral != nil || volume.CSI != nil {
			return "SchedulerVolumeUnsupported"
		}
	}
	return ""
}

func toleratesPlacement(spec corev1.PodSpec, node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, toleration := range spec.Tolerations {
			if toleration.ToleratesTaint(klog.Background(), &taint, false) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

func (r *Reconciler) preflightPDB(ctx context.Context, pod *corev1.Pod) (string, error) {
	budgets, err := r.Client.PolicyV1().PodDisruptionBudgets(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("preflight PodDisruptionBudgets: %w", err)
	}
	matched := 0
	for _, budget := range budgets.Items {
		selector, err := metav1.LabelSelectorAsSelector(budget.Spec.Selector)
		if err != nil {
			return "InvalidPodDisruptionBudget", nil
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		matched++
		if budget.Status.ObservedGeneration < budget.Generation || budget.Status.DisruptionsAllowed < 1 {
			return "DisruptionBudgetDenied", nil
		}
	}
	if matched > 1 {
		return "MultipleDisruptionBudgets", nil
	}
	return "", nil
}

func pvcNames(spec corev1.PodSpec) map[string]bool {
	names := make(map[string]bool)
	for _, v := range spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			names[v.PersistentVolumeClaim.ClaimName] = true
		}
	}
	return names
}

// preEviction limits deferral to preparation before consumer disruption.
func preEviction(move volumeapi.Move) bool {
	return !move.Status.EvictionRequested && (move.Status.Phase == "" || move.Status.Phase == string(fsm.PhasePending) ||
		move.Status.Phase == string(fsm.PhasePreparing))
}
