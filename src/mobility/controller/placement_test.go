package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestPlacementPodReservesCandidatesAndWorkloadFootprint(t *testing.T) {
	reconciler := &Reconciler{Namespace: "system", HelperImage: "helper"}
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid",
		Status: volumeapi.MoveStatus{CandidateNodes: []string{"node-b", "node-a"}},
	}
	replacement := &corev1.Pod{Spec: corev1.PodSpec{
		NodeSelector: map[string]string{"disk": "fast"},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}},
			}}},
		}},
		Tolerations: []corev1.Toleration{{Key: "storage", Operator: corev1.TolerationOpExists}},
		InitContainers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("500m"),
		}}}},
		Containers: []corev1.Container{{
			Ports: []corev1.ContainerPort{{HostPort: 8080, ContainerPort: 8080}},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi"),
			}},
		}},
	}}

	pod := reconciler.placementPod(move, replacement, namesFor(move.Name))
	if pod.Namespace != "system" || pod.Labels["shiftpv.io/role"] != placementRole || len(pod.OwnerReferences) != 1 || string(pod.OwnerReferences[0].UID) != move.UID {
		t.Fatalf("placement identity = %#v", pod.ObjectMeta)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.SecurityContext.RunAsGroup == nil || *pod.Spec.SecurityContext.RunAsGroup != 65532 {
		t.Fatalf("placement security context = %#v", pod.Spec.SecurityContext)
	}
	if pod.Spec.NodeSelector["disk"] != "fast" || len(pod.Spec.Tolerations) != 1 || len(pod.Spec.Containers[0].Ports) != 1 {
		t.Fatalf("placement constraints = %#v", pod.Spec)
	}
	requests := pod.Spec.Containers[0].Resources.Requests
	if requests.Cpu().Cmp(resource.MustParse("500m")) != 0 || requests.Memory().Cmp(resource.MustParse("64Mi")) != 0 {
		t.Fatalf("placement requests = %v", requests)
	}
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required.NodeSelectorTerms) != 1 || len(required.NodeSelectorTerms[0].MatchExpressions) != 2 {
		t.Fatalf("required node affinity = %#v", required)
	}
	hostname := required.NodeSelectorTerms[0].MatchExpressions[1]
	if hostname.Key != corev1.LabelHostname || len(hostname.Values) != 2 || hostname.Values[0] != "node-a" || hostname.Values[1] != "node-b" {
		t.Fatalf("candidate affinity = %#v", hostname)
	}
}

func TestPlacementLifecycleIsIdentityCheckedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid",
		Status: volumeapi.MoveStatus{ClaimNamespace: "workload", CandidateNodes: []string{"destination"}},
	}
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: "workload", UID: "replacement-uid"},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: placementHoldName}},
		},
	}
	client := fake.NewSimpleClientset(replacement)
	reconciler := &Reconciler{Client: client, Namespace: "system", HelperImage: "helper"}
	observed := observation{Replacement: replacement}

	if err := reconciler.ensurePlacement(ctx, &move, observed); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensurePlacement(ctx, &move, observed); err != nil {
		t.Fatalf("idempotent placement ensure failed: %v", err)
	}
	pods, _ := client.CoreV1().Pods("system").List(ctx, metav1.ListOptions{})
	if len(pods.Items) != 1 {
		t.Fatalf("placement Pods = %d, want 1", len(pods.Items))
	}

	move.Status.ReplacementName = replacement.Name
	move.Status.ReplacementUID = string(replacement.UID)
	move.Status.DestinationNode = "destination"
	if err := reconciler.pinReplacement(ctx, move); err != nil {
		t.Fatal(err)
	}
	pinned, _ := client.CoreV1().Pods("workload").Get(ctx, replacement.Name, metav1.GetOptions{})
	if pinned.Spec.NodeSelector[corev1.LabelHostname] != "destination" || !hasPlacementHold(pinned) {
		t.Fatalf("replacement was not held and pinned: %#v", pinned.Spec)
	}

	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() != "system" {
			return false, nil, nil
		}
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil {
			t.Fatal("placement deletion omitted UID precondition")
		}
		return false, nil, nil
	})
	if err := reconciler.deletePlacement(ctx, move); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.deletePlacement(ctx, move); err != nil {
		t.Fatalf("idempotent placement delete failed: %v", err)
	}
}

func TestPlacementRecreationStaysOnPersistedDestination(t *testing.T) {
	reconciler := &Reconciler{Namespace: "system", HelperImage: "helper"}
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Status: volumeapi.MoveStatus{
		CandidateNodes: []string{"node-a", "node-b"}, DestinationNode: "node-b",
	}}
	pod := reconciler.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	values := required.NodeSelectorTerms[0].MatchExpressions[0].Values
	if len(values) != 1 || values[0] != "node-b" {
		t.Fatalf("recreated placement candidates = %v, want only persisted destination", values)
	}
}

func TestOwnerCommitRequiresLiveReservationOnDestination(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, _, destination := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}, Status: volumeapi.MoveStatus{
		CandidateNodes: []string{"destination"}, DestinationNode: "destination", DestinationCopy: &destination, SourceCopy: &source, DestinationPoolUID: destination.PoolUID,
	}}
	repository := &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {
		UID: source.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CurrentCopy: &source,
	}}}
	reconciler := newTestReconciler(fake.NewSimpleClientset(), repository)
	observed := observation{Volume: identifiedTestState(volumeID, repository.volumes[volumeID], repository.pools), DestinationNode: "destination"}
	if err := reconciler.commitOwner(ctx, &move, observed); err == nil {
		t.Fatal("owner commit succeeded without a placement reservation")
	}
	if repository.volumes[volumeID].OwnerNode != "source" {
		t.Fatal("failed reservation check changed owner")
	}

	placement := reconciler.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
	placement.Spec.NodeName = "other-node"
	if _, err := reconciler.Client.CoreV1().Pods("system").Create(ctx, placement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.commitOwner(ctx, &move, observed); err == nil {
		t.Fatal("owner commit accepted a reservation on another node")
	}
	placement, _ = reconciler.Client.CoreV1().Pods("system").Get(ctx, placement.Name, metav1.GetOptions{})
	placement.Spec.NodeName = "destination"
	if _, err := reconciler.Client.CoreV1().Pods("system").Update(ctx, placement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.commitOwner(ctx, &move, observed); err != nil {
		t.Fatalf("owner commit rejected the live destination reservation: %v", err)
	}
	if repository.volumes[volumeID].OwnerNode != "destination" {
		t.Fatalf("owner was not committed: %#v", repository.volumes[volumeID])
	}
}

func TestPlacementRejectsForeignObjectAndUnsafeReplacement(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Status: volumeapi.MoveStatus{CandidateNodes: []string{"destination"}}}
	names := namesFor(move.Name)
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: names.PlacementPod, Namespace: "system", UID: "foreign"}}
	client := fake.NewSimpleClientset(foreign)
	reconciler := &Reconciler{Client: client, Namespace: "system", HelperImage: "helper"}
	replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "replacement", UID: "replacement"}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: placementHoldName}}}}
	if err := reconciler.ensurePlacement(ctx, &move, observation{Replacement: replacement}); err == nil {
		t.Fatal("foreign placement Pod was accepted")
	}

	move.Status.ClaimNamespace = "workload"
	move.Status.ReplacementName = "replacement"
	move.Status.ReplacementUID = "replacement"
	move.Status.DestinationNode = "destination"
	unsafe := replacement.DeepCopy()
	unsafe.Namespace = "workload"
	unsafe.Spec.SchedulingGates = nil
	client = fake.NewSimpleClientset(unsafe)
	reconciler.Client = client
	if err := reconciler.pinReplacement(ctx, move); err == nil {
		t.Fatal("ungated replacement was pinned")
	}
}
