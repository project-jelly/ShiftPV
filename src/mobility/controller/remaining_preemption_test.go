package controller

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

// Desired policy regression: preparation's non-preempting reservation policy
// must survive loss/recreation after approval. Expected to fail on e035075.
func TestReportRecreatedPlacementRemainsNonPreempting(t *testing.T) {
	r, repo, client := recoveryFixture(t, "source")
	r.PlacementPriorityClassName = "dedicated-placement"
	r.HelperImage = "busybox:1.37"
	assignJobUIDs(client)
	move := repo.moves[0]
	move.Spec.Recovery = ""
	move.Status.Phase = string(fsm.PhaseCopying)
	move.Status.CandidateNodes = []string{"destination"}
	move.Status.DestinationNode = "destination"
	if node := os.Getenv("SHIFT_PV_REPRO_NODE"); node != "" {
		move.Status.CandidateNodes, move.Status.DestinationNode = []string{node}, node
	}
	replacement, err := client.CoreV1().Pods("workload").Get(context.Background(), "consumer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replacement.Spec.NodeName = ""
	replacement.Spec.NodeSelector = nil
	replacement.Spec.Affinity = nil
	replacement.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
	preempt := corev1.PreemptLowerPriority
	replacement.Spec.PriorityClassName, replacement.Spec.PreemptionPolicy = "workload-high", &preempt
	cpu := "1000m"
	if configured := os.Getenv("SHIFT_PV_REPRO_CPU"); configured != "" {
		cpu = configured
	}
	replacement.Spec.Containers = []corev1.Container{{Name: "consumer", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}}}}
	if err := r.ensurePlacement(context.Background(), &move, observation{Replacement: replacement}); err != nil {
		t.Fatal(err)
	}
	pod, err := client.CoreV1().Pods(r.Namespace).Get(context.Background(), namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("SHIFT_PV_REPRO_PLACEMENT_OUTPUT"); output != "" {
		fixture := pod.DeepCopy()
		fixture.APIVersion, fixture.Kind = "v1", "Pod"
		// The native scheduler experiment has no Move CRD/owner. Only ownership
		// metadata is removed; the production-generated PodSpec stays intact.
		fixture.UID, fixture.ResourceVersion, fixture.OwnerReferences = "", "", nil
		data, err := json.MarshalIndent(fixture, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if pod.Spec.PriorityClassName != r.PlacementPriorityClassName || pod.Spec.PreemptionPolicy == nil || *pod.Spec.PreemptionPolicy != corev1.PreemptNever {
		policy := "<nil>"
		if pod.Spec.PreemptionPolicy != nil {
			policy = string(*pod.Spec.PreemptionPolicy)
		}
		t.Fatalf("PL-07: approved placement recreation inherited workload scheduling authority: priorityClass=%q policy=%s", pod.Spec.PriorityClassName, policy)
	}
}
