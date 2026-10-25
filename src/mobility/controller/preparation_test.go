package controller

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestPreparedPlacementCoverageUsesReservedResources(t *testing.T) {
	runtimeClass := "sandbox"
	live := &corev1.Pod{Spec: corev1.PodSpec{
		RuntimeClassName: &runtimeClass, SchedulerName: corev1.DefaultSchedulerName,
		Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
		}}, Ports: []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}}}},
	}}
	for name, test := range map[string]struct {
		change func(*corev1.Pod, *corev1.Pod, *corev1.PodTemplateSpec)
		reason string
	}{
		"smaller raw template and defaulted scheduler": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.SchedulerName, template.Spec.RuntimeClassName = "", nil
			template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}
			template.Spec.Containers[0].Ports[0].Protocol = ""
		}},
		"larger template requests": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
		}, reason: "PreparedResourceRequestsChanged"},
		"larger template limit defaults omitted request": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			delete(template.Spec.Containers[0].Resources.Requests, corev1.ResourceCPU)
			template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
		}, reason: "PreparedResourceRequestsChanged"},
		"live in-place growth": {change: func(_ *corev1.Pod, consumer *corev1.Pod, _ *corev1.PodTemplateSpec) {
			consumer.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("2")
		}, reason: "PreparedResourceRequestsChanged"},
		"changed host port": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.Containers[0].Ports[0].HostPort = 9090
		}, reason: "PreparedHostPortsChanged"},
		"fewer host ports": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.Containers[0].Ports = nil
		}},
		"changed runtime": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			name := "other-runtime"
			template.Spec.RuntimeClassName = &name
		}, reason: "PreparedRuntimeChanged"},
		"changed network": {change: func(_, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.HostNetwork = true
		}, reason: "PreparedRuntimeChanged"},
		"admitted runtime overhead": {change: func(placement, consumer *corev1.Pod, template *corev1.PodTemplateSpec) {
			placement.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m")}
			consumer.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m")}
			template.Spec.Overhead = nil
		}},
		"unreserved overhead": {change: func(_ *corev1.Pod, consumer *corev1.Pod, _ *corev1.PodTemplateSpec) {
			consumer.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m")}
		}, reason: "PreparedResourceRequestsChanged"},
	} {
		t.Run(name, func(t *testing.T) {
			consumer := live.DeepCopy()
			template := &corev1.PodTemplateSpec{Spec: *live.Spec.DeepCopy()}
			r := &Reconciler{}
			placement := r.placementPod(volumeapi.Move{}, live, resourceNames{})
			test.change(placement, consumer, template)
			if reason := preparedPlacementCoverage(placement, consumer, template); reason != test.reason {
				t.Fatalf("coverage reason=%q, want %q", reason, test.reason)
			}
		})
	}
}

func TestPreparationStartingDisruptionRejectsMultipleReplacements(t *testing.T) {
	for _, originalGone := range []bool{false, true} {
		t.Run(map[bool]string{false: "original present", true: "original absent"}[originalGone], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			move := f.untilPrepared(t, 0)
			if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
				t.Fatal(err)
			}
			if f.move(t, 0).Status.Phase != "StartingDisruption" {
				t.Fatal("disruption intent was not stored")
			}
			w := f.workloads[0]
			if originalGone {
				if err := f.client.CoreV1().Pods(w.Namespace.Name).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"a-held", "z-pinned"} {
				pod := w.Consumer.DeepCopy()
				pod.Name, pod.UID, pod.ResourceVersion = name, "", ""
				if name == "a-held" {
					pod.Spec.NodeName = ""
					pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: placementHoldName}}
				}
				if _, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			f.client.ClearActions()
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			move = f.move(t, 0)
			if move.Status.Phase != "StartingDisruption" || move.Status.Reason != "MultipleConsumers" || move.Status.Preparation == nil || f.state(t, 0).Phase != volumeapi.PhaseReady {
				t.Fatalf("ambiguous consumer set did not retain the reserved disruption: %+v", move.Status)
			}
			assertPreparationReserved(t, f, preparationRequestedBytes)
			for _, action := range f.client.Actions() {
				if action.GetSubresource() == "eviction" {
					t.Fatal("a consumer was evicted after the consumer set changed")
				}
			}
			if err := f.client.CoreV1().Pods(w.Namespace.Name).Delete(f.ctx, "z-pinned", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if !originalGone {
				if err := f.client.CoreV1().Pods(w.Namespace.Name).Delete(f.ctx, w.Consumer.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			replacement, err := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, "a-held", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			unpublishDisruptedSource(t, f)
			assertDisruptedMoveResumes(t, f, move, replacement)
		})
	}
}

type preparationPoolReadRepository struct {
	Repository
	beforeReady func()
}

func (r *preparationPoolReadRepository) ReadyPoolForIdentity(ctx context.Context, name, uid, node string) (volumeapi.Pool, error) {
	if r.beforeReady != nil {
		before := r.beforeReady
		r.beforeReady = nil
		before()
	}
	return r.Repository.ReadyPoolForIdentity(ctx, name, uid, node)
}

func TestPreparationLateQuotaDenialReleasesUnreservedPlacementBeforeRetry(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	var placement *corev1.Pod
	for range 6 {
		if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
		var err error
		placement, err = f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(f.moveNames[0]).PlacementPod, metav1.GetOptions{})
		if err == nil {
			break
		}
		if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	if placement == nil {
		t.Fatal("no unreserved placement created")
	}
	f.schedulePlacements(t)
	setLimit := func(limit string) {
		pool, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedField(pool.Object, limit, "spec", "capacity", "limit")
		if err = f.dynamic.Tracker().Update(volumeapi.PoolResource, pool, ""); err != nil {
			t.Fatal(err)
		}
	}
	f.reconciler.Repository = &preparationPoolReadRepository{Repository: f.registry, beforeReady: func() { setLimit("1Gi") }}
	if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	if f.move(t, 0).Status.Preparation != nil || f.move(t, 0).Status.Reason != "DestinationReservationLimit" {
		t.Fatalf("late denial acquired authority or was not retryable: %+v", f.move(t, 0).Status)
	}
	if _, err := f.client.CoreV1().Pods("system").Get(f.ctx, placement.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unreserved placement retained after definitive denial: %v", err)
	}
	assertPreparationOriginals(t, f)
	setLimit("5Gi")
	f.untilPrepared(t, 0)
	if f.move(t, 0).Status.PlacementPodUID == string(placement.UID) {
		t.Fatal("retry adopted the deleted placement UID")
	}
	assertPreparationOriginals(t, f)
}

func TestPreparationProtectsMoveBeforeAnyPlanOrVolumeEffect(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	object, err := f.dynamic.Resource(volumeapi.MoveResource).Get(f.ctx, f.moveNames[0], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.SetFinalizers(nil)
	if err = f.dynamic.Tracker().Update(volumeapi.MoveResource, object, ""); err != nil {
		t.Fatal(err)
	}
	before := f.move(t, 0)
	if err = f.reconciler.reconcilePersistedMove(f.ctx, before); err != nil {
		t.Fatal(err)
	}
	after := f.move(t, 0)
	if len(after.Finalizers) != 1 || after.Finalizers[0] != volumeapi.MoveProtectionFinalizer || after.ResourceVersion == before.ResourceVersion {
		t.Fatalf("protection was not durably acquired: %+v", after)
	}
	if !reflect.DeepEqual(after.Status, before.Status) || f.state(t, 0).ActiveMove != "" {
		t.Fatal("plan or Volume changed in the finalizer write pass")
	}
	if len(f.client.Actions()) != 0 {
		t.Fatalf("workload inspected before reobserving protection: %v", f.client.Actions())
	}
	f.untilPrepared(t, 0)
	assertPreparationOriginals(t, f)
}

func TestPreparationPlacementDoesNotInheritWorkloadPriority(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	f.reconciler.PlacementPriorityClassName = "dedicated-placement"
	w := f.workloads[0]
	pod, err := f.client.CoreV1().Pods(w.Namespace.Name).Get(f.ctx, w.Consumer.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	high := int32(1000)
	preempt := corev1.PreemptLowerPriority
	pod.Spec.PriorityClassName, pod.Spec.Priority, pod.Spec.PreemptionPolicy = "workload-high", &high, &preempt
	if _, err = f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	move := f.untilPrepared(t, 0)
	placement, err := f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(move.Name).PlacementPod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if placement.Spec.PriorityClassName != "dedicated-placement" || placement.Spec.Priority == nil || *placement.Spec.Priority != 0 ||
		placement.Spec.PreemptionPolicy == nil || *placement.Spec.PreemptionPolicy != corev1.PreemptNever {
		t.Fatalf("preparation can inherit preemption authority: %+v", placement.Spec)
	}
	assertPreparationOriginals(t, f)
}

func TestPreparationAdvisoryFilterAvoidsFullNode(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	full, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, "destination-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(full.Object, "1Gi", "spec", "capacity", "limit")
	if err = f.dynamic.Tracker().Update(volumeapi.PoolResource, full, ""); err != nil {
		t.Fatal(err)
	}
	other := preparationPool("other", "5Gi")
	if err = f.dynamic.Tracker().Create(volumeapi.PoolResource, other, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.CoreV1().Nodes().Create(f.ctx, readyNode("other", false), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var placement *corev1.Pod
	for range 6 {
		if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
			t.Fatal(err)
		}
		placement, err = f.client.CoreV1().Pods("system").Get(f.ctx, namesFor(f.moveNames[0]).PlacementPod, metav1.GetOptions{})
		if err == nil {
			break
		}
		if !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	if placement == nil {
		t.Fatal("no placement for the node with free logical capacity")
	}
	terms := placement.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for _, term := range terms {
		last := term.MatchExpressions[len(term.MatchExpressions)-1]
		if !reflect.DeepEqual(last.Values, []string{"other"}) {
			t.Fatalf("full node remained a scheduler candidate: %+v", terms)
		}
	}
	if placement.Spec.PreemptionPolicy == nil || *placement.Spec.PreemptionPolicy != corev1.PreemptNever {
		t.Fatal("preparation can preempt another workload")
	}
	placement.Spec.NodeName = "other"
	if _, err = f.client.CoreV1().Pods("system").Update(f.ctx, placement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
		t.Fatal(err)
	}
	move := f.move(t, 0)
	if move.Status.Preparation == nil || move.Status.Preparation.NodeName != "other" || move.Status.Preparation.Pool.UID != "other-pool-uid" {
		t.Fatalf("did not reserve the available Pool: %+v", move.Status)
	}
	assertPreparationOriginals(t, f)
}
