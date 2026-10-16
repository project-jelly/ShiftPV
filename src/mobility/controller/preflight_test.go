package controller

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const preflightVolume = "shiftpv-0123456789abcdef0123456789abcdef"

func preflightFixture() (*Reconciler, *memoryRepository, *fake.Clientset) {
	repo := &memoryRepository{
		volumes: map[string]volumeapi.State{preflightVolume: {Phase: volumeapi.PhaseReady, OwnerNode: "source", PublishedNodes: []string{"source"}}},
		pools:   []volumeapi.Pool{{Name: "source", NodeName: "source", MountPath: "/pool"}, {Name: "destination", NodeName: "destination", MountPath: "/pool"}},
	}
	client := fake.NewSimpleClientset(mobilityObjects(preflightVolume)...)
	return newTestReconciler(client, repo), repo, client
}

// A new prepared Move has no source plan until its first eligible observation.
func pendingPreflightFixture() (*Reconciler, *memoryRepository, *fake.Clientset) {
	r, repo, client := preflightFixture()
	move := preparedFixtureMove(volumeapi.Move{Name: "move-test", UID: "move-uid",
		Spec: volumeapi.MoveSpec{VolumeID: preflightVolume, SourceNode: "source"}})
	move.Status = volumeapi.MoveStatus{Phase: string(fsm.PhasePending)}
	repo.moves = []volumeapi.Move{move}
	return r, repo, client
}

func TestPreflightDefersWithoutLockOrEviction(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		mutate       func(*corev1.Pod, *appsv1.ReplicaSet, *corev1.Node, *corev1.PersistentVolume)
	}{
		{"explicit selector", "NoCompatibleDestination", func(p *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "source"}
		}},
		{"live constraint", "NoCompatibleDestination", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			p.Spec.NodeSelector = map[string]string{"disk": "missing"}
		}},
		{"node affinity", "NoCompatibleDestination", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: hostnameSelector("source")}}
		}},
		{"legacy PV topology", "NoCompatibleDestination", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, pv *corev1.PersistentVolume) {
			pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: hostnameSelector("source")}
		}},
		{"taint", "NoCompatibleDestination", func(_ *corev1.Pod, _ *appsv1.ReplicaSet, n *corev1.Node, _ *corev1.PersistentVolume) {
			n.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "other", Effect: corev1.TaintEffectNoSchedule}}
		}},
		{"bare", "BarePodUnsupported", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			p.OwnerReferences = nil
		}},
		{"UID reuse", "ConsumerControllerChanged", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.UID = "new-rs"
		}},
		{"unknown owner", "UnsupportedWorkloadController", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			p.OwnerReferences[0].Kind = "DaemonSet"
		}},
		{"custom scheduler", "CustomSchedulerUnsupported", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			p.Spec.SchedulerName = "custom"
		}},
		{"multiple PVC", "MultiplePVCsUnsupported", func(p *corev1.Pod, _ *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			v := claimVolumes()[0]
			v.Name = "other"
			v.PersistentVolumeClaim.ClaimName = "other"
			p.Spec.Volumes = append(p.Spec.Volumes, v)
		}},
		{"explicit binding", "ExplicitNodeNameUnsupported", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.NodeName = "source"
		}},
		{"gate", "SchedulingGateUnsupported", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "other/gate"}}
		}},
		{"inter pod", "InterPodAffinityUnsupported", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname}}}}
		}},
		{"hard spread", "TopologySpreadUnsupported", func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *corev1.Node, _ *corev1.PersistentVolume) {
			rs.Spec.Template.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{WhenUnsatisfiable: corev1.DoNotSchedule}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, repo, client := preflightFixture()
			ctx := context.Background()
			pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
			rs, _ := client.AppsV1().ReplicaSets("workload").Get(ctx, "rs", metav1.GetOptions{})
			node, _ := client.CoreV1().Nodes().Get(ctx, "destination", metav1.GetOptions{})
			pv, _ := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
			test.mutate(pod, rs, node, pv)
			_, _ = client.CoreV1().Pods("workload").Update(ctx, pod, metav1.UpdateOptions{})
			_, _ = client.AppsV1().ReplicaSets("workload").Update(ctx, rs, metav1.UpdateOptions{})
			_, _ = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			_, _ = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			move := volumeapi.Move{Spec: volumeapi.MoveSpec{ProtocolVersion: volumeapi.MoveProtocolPreparedV1, VolumeID: preflightVolume, SourceNode: "source"}}
			observed, err := r.observe(ctx, move)
			if err != nil || observed.FSM.PreconditionsValid || observed.FSM.UnsafeReason != test.reason {
				t.Fatalf("observed=%+v err=%v", observed.FSM, err)
			}
			for range 3 {
				if err := r.ReconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if len(repo.moves) != 0 || repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
				t.Fatalf("unexpected mutations: %+v", repo)
			}
			assertNoEviction(t, client)
		})
	}
}

func hostnameSelector(node string) *corev1.NodeSelector {
	return &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{node}}}}}}
}

func assertNoEviction(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, a := range client.Actions() {
		if a.GetSubresource() == "eviction" {
			t.Fatalf("unexpected eviction: %+v", a)
		}
	}
}

func TestPreflightRecognizesInjectedPinAndToleratedDestination(t *testing.T) {
	r, repo, client := preflightFixture()
	ctx := context.Background()
	pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
	pod.Annotations = map[string]string{"shiftpv.io/placement": "owner"}
	pod.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "source"}
	pod.Spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	_, _ = client.CoreV1().Pods("workload").Update(ctx, pod, metav1.UpdateOptions{})
	rs, _ := client.AppsV1().ReplicaSets("workload").Get(ctx, "rs", metav1.GetOptions{})
	rs.Spec.Template.Spec.Tolerations = pod.Spec.Tolerations
	rs.Spec.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: hostnameSelector("destination")}}
	_, _ = client.AppsV1().ReplicaSets("workload").Update(ctx, rs, metav1.UpdateOptions{})
	node, _ := client.CoreV1().Nodes().Get(ctx, "destination", metav1.GetOptions{})
	node.Spec.Taints = []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoExecute}}
	_, _ = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err := r.discoverMoves(ctx); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 1 {
		t.Fatal("portable template with injected pin was rejected")
	}
}

func TestPreflightPDBRecheckedBeforePreparationAndEviction(t *testing.T) {
	for _, phase := range []fsm.Phase{fsm.PhasePending, fsm.PhasePreparing, fsm.PhaseStartingDisruption} {
		t.Run(string(phase), func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 1)
			if phase == fsm.PhasePreparing {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
			}
			if phase == fsm.PhaseStartingDisruption {
				move := f.untilPrepared(t, 0)
				if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
					t.Fatal(err)
				}
			}
			if got := f.move(t, 0).Status.Phase; got != string(phase) {
				t.Fatalf("fixture phase=%s", got)
			}
			budget := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "budget", Namespace: f.workloads[0].Namespace.Name, Generation: 2}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{}}, Status: policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: 1}}
			if _, err := f.client.PolicyV1().PodDisruptionBudgets(budget.Namespace).Create(f.ctx, budget, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
				t.Fatal(err)
			}
			denied := f.move(t, 0)
			wantPhase := string(phase)
			if phase == fsm.PhaseStartingDisruption {
				wantPhase = string(fsm.PhaseCanceling)
			}
			if denied.Status.Phase != wantPhase || denied.Status.Reason != "DisruptionBudgetDenied" {
				t.Fatalf("budget denial was not preserved: %+v", denied.Status)
			}
			if f.state(t, 0).Phase != volumeapi.PhaseReady {
				t.Fatal("budget denial disrupted the source")
			}
			assertNoEviction(t, f.client)
			if phase == fsm.PhaseStartingDisruption && denied.Status.Preparation == nil {
				t.Fatal("cancellation lost its reserved hold")
			}
			budget.Status.ObservedGeneration = 2
			if _, err := f.client.PolicyV1().PodDisruptionBudgets(budget.Namespace).UpdateStatus(f.ctx, budget, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if phase != fsm.PhaseStartingDisruption {
				for range 6 {
					if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
						t.Fatal(err)
					}
					f.schedulePlacements(t)
					if f.move(t, 0).Status.Reason == "" {
						break
					}
				}
				resumed := f.move(t, 0)
				if resumed.Status.Phase != string(fsm.PhasePreparing) || resumed.Status.Reason != "" {
					t.Fatalf("did not resume preparation: %+v", resumed.Status)
				}
				if f.state(t, 0).Phase != volumeapi.PhaseReady {
					t.Fatal("preparation made the source unpublishable")
				}
				assertNoEviction(t, f.client)
				return
			}
			// Once a reserved preparation is cancelled, a recovered PDB must
			// finish cancellation instead of resurrecting its disruption intent.
			for range 6 {
				if err := f.reconciler.reconcilePersistedMove(f.ctx, f.move(t, 0)); err != nil {
					t.Fatal(err)
				}
				if f.move(t, 0).Status.Phase == string(fsm.PhaseCancelled) && f.state(t, 0).ActiveMove == "" {
					break
				}
			}
			settled := f.move(t, 0)
			if settled.Status.Phase != string(fsm.PhaseCancelled) || settled.Status.Preparation != nil || f.state(t, 0).ActiveMove != "" || f.state(t, 0).Phase != volumeapi.PhaseReady {
				t.Fatalf("unsafe preparation did not cancel safely: %+v", settled.Status)
			}
			assertNoEviction(t, f.client)
		})
	}
}

func TestPreflightAPIFailureDoesNotEvict(t *testing.T) {
	for _, resource := range []string{"replicasets", "poddisruptionbudgets", "nodes"} {
		t.Run(resource, func(t *testing.T) {
			r, repo, client := preflightFixture()
			client.PrependReactor("*", resource, func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("API timeout") })
			if err := r.ReconcileAll(context.Background()); err == nil {
				t.Fatal("API error hidden")
			}
			if len(repo.moves) != 0 || repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
				t.Fatal("state changed")
			}
			assertNoEviction(t, client)
		})
	}
}

func TestPreflightRequiresExactPresentSourceCopy(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*volumeapi.Pool, volume.CopyIdentity)
		valid  bool
	}{
		{name: "copy omitted", mutate: func(pool *volumeapi.Pool, _ volume.CopyIdentity) {
			pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true}
		}},
		{name: "copy missing", mutate: func(pool *volumeapi.Pool, copy volume.CopyIdentity) {
			pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &copy, Present: false}}}
		}},
		{name: "copy problem", mutate: func(pool *volumeapi.Pool, copy volume.CopyIdentity) {
			pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &copy, Present: true, Problem: "IdentityMismatch"}}}
		}},
		{name: "pool replaced", mutate: func(pool *volumeapi.Pool, copy volume.CopyIdentity) {
			pool.UID = "replacement-pool-uid"
			pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &copy, Present: true}}}
		}},
		{name: "exact copy present", valid: true, mutate: func(pool *volumeapi.Pool, copy volume.CopyIdentity) {
			pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true, Copies: []volumeapi.CopyObservation{{Identity: &copy, Present: true}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, repo, client := preflightFixture()
			state := identifiedTestState(preflightVolume, repo.volumes[preflightVolume], repo.pools)
			sourcePool := identifiedTestPools(repo.pools)[0]
			test.mutate(&sourcePool, *state.CurrentCopy)
			destinationPool := identifiedTestPools(repo.pools)[1]
			repo.readyPoolsConfigured = true
			repo.readyPools = []volumeapi.Pool{sourcePool, destinationPool}
			move := volumeapi.Move{Spec: volumeapi.MoveSpec{ProtocolVersion: volumeapi.MoveProtocolPreparedV1, VolumeID: preflightVolume, SourceNode: "source"}}
			observed, err := r.observe(context.Background(), move)
			if err != nil {
				t.Fatal(err)
			}
			if observed.FSM.PreconditionsValid != test.valid {
				t.Fatalf("preconditions=%v reason=%q", observed.FSM.PreconditionsValid, observed.FSM.UnsafeReason)
			}
			if !test.valid && observed.FSM.UnsafeReason != "SourceUnavailable" {
				t.Fatalf("unsafe reason=%q", observed.FSM.UnsafeReason)
			}
			assertNoEviction(t, client)
		})
	}
}

func TestConsumerUIDDistinguishesSameNameReplacement(t *testing.T) {
	f := newPreparationRegistryFixture(t, 1)
	move := f.untilPrepared(t, 0)
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	move = f.move(t, 0)
	pod := f.workloads[0].Consumer.DeepCopy()
	if err := f.client.CoreV1().Pods(pod.Namespace).Delete(f.ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.UID = "replacement-consumer-uid"
	pod, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := f.reconciler.observe(f.ctx, move)
	if err != nil || observed.Consumer != nil || observed.Replacement == nil || observed.Replacement.UID != pod.UID {
		t.Fatalf("same-name replacement was treated as the original consumer: %+v error=%v", observed, err)
	}
	if err := f.reconciler.reconcilePersistedMove(f.ctx, move); err != nil {
		t.Fatal(err)
	}
	cancelled := f.move(t, 0)
	if cancelled.Status.Phase != string(fsm.PhaseCanceling) || cancelled.Status.Reason != "UnexpectedConsumerPlacement" || cancelled.Status.Preparation == nil || f.state(t, 0).Phase != volumeapi.PhaseReady {
		t.Fatalf("unheld replacement did not cancel the reserved disruption: %+v", cancelled.Status)
	}
	assertNoEviction(t, f.client)
}

func TestEvictionUsesObservedPodUID(t *testing.T) {
	r, _, client := preflightFixture()
	client.PrependReactor("create", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		e := a.(ktesting.CreateAction).GetObject().(*policyv1.Eviction)
		if e.DeleteOptions == nil || e.DeleteOptions.Preconditions == nil || e.DeleteOptions.Preconditions.UID == nil || *e.DeleteOptions.Preconditions.UID != "consumer-uid" {
			t.Fatalf("eviction=%+v", e)
		}
		return true, nil, nil
	})
	pod, _ := client.CoreV1().Pods("workload").Get(context.Background(), "consumer", metav1.GetOptions{})
	if err := r.evictConsumer(context.Background(), &volumeapi.Move{}, observation{Consumer: pod}); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightStatefulSetTemplateAndMissingOwner(t *testing.T) {
	ctx := context.Background()
	r, repo, client := preflightFixture()
	pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
	pod.OwnerReferences[0].Kind = "StatefulSet"
	pod.OwnerReferences[0].Name = "stateful"
	pod.OwnerReferences[0].UID = "sts-uid"
	_, _ = client.CoreV1().Pods("workload").Update(ctx, pod, metav1.UpdateOptions{})
	if err := r.discoverMoves(ctx); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 0 {
		t.Fatal("missing owner allowed")
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "stateful", Namespace: "workload", UID: "sts-uid"}, Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: claimVolumes()}}}}
	_, _ = client.AppsV1().StatefulSets("workload").Create(ctx, sts, metav1.CreateOptions{})
	if err := r.discoverMoves(ctx); err != nil {
		t.Fatal(err)
	}
	if len(repo.moves) != 1 {
		t.Fatal("eligible StatefulSet rejected")
	}
}

func TestPreflightPDBSelectorAndOverlap(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		selectors []*metav1.LabelSelector
		allowed   int32
		reason    string
	}{
		{"nil matches none", []*metav1.LabelSelector{nil}, 0, ""},
		{"nonmatching", []*metav1.LabelSelector{{MatchLabels: map[string]string{"app": "other"}}}, 0, ""},
		{"empty matches all", []*metav1.LabelSelector{{}}, 0, "DisruptionBudgetDenied"},
		{"multiple", []*metav1.LabelSelector{{}, {}}, 1, "MultipleDisruptionBudgets"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, _, client := preflightFixture()
			for i, selector := range test.selectors {
				_, err := client.PolicyV1().PodDisruptionBudgets("workload").Create(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprint("pdb-", i), Namespace: "workload"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: selector}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: test.allowed}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
			reason, err := r.preflightPDB(ctx, pod)
			if err != nil || reason != test.reason {
				t.Fatalf("reason=%s err=%v", reason, err)
			}
		})
	}
}

func TestPreflightLateSecondConsumerWaitsAndResumes(t *testing.T) {
	ctx := context.Background()
	r, repo, client := pendingPreflightFixture()
	pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
	pod.Name, pod.UID = "second-consumer", "second-uid"
	if _, err := client.CoreV1().Pods("workload").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.Phase != "Pending" || repo.moves[0].Status.Reason != "MultipleConsumers" {
		t.Fatalf("late ineligibility must wait, not become terminal: %+v", repo.moves[0].Status)
	}
	if repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
		t.Fatal("volume locked")
	}
	assertNoEviction(t, client)
	if err := client.CoreV1().Pods("workload").Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.Phase != string(fsm.PhasePreparing) {
		t.Fatalf("did not resume: %+v", repo.moves[0].Status)
	}
}

func TestPreflightRejectsRecreatedOrReboundClaim(t *testing.T) {
	for _, mutate := range []func(*corev1.PersistentVolumeClaim){
		func(pvc *corev1.PersistentVolumeClaim) { pvc.UID = "recreated-claim" },
		func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = "another-pv" },
	} {
		r, repo, client := preflightFixture()
		ctx := context.Background()
		pvc, _ := client.CoreV1().PersistentVolumeClaims("workload").Get(ctx, "claim", metav1.GetOptions{})
		mutate(pvc)
		_, _ = client.CoreV1().PersistentVolumeClaims("workload").Update(ctx, pvc, metav1.UpdateOptions{})
		if err := r.ReconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if len(repo.moves) != 0 || repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
			t.Fatal("stale volume joined new claim's migration")
		}
		assertNoEviction(t, client)
	}
}

func TestPendingEligibilityLossWaitsAndResumes(t *testing.T) {
	for _, condition := range []string{"namespace opt-out", "consumer missing"} {
		t.Run(condition, func(t *testing.T) {
			ctx := context.Background()
			r, repo, client := pendingPreflightFixture()
			pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
			ns, _ := client.CoreV1().Namespaces().Get(ctx, "workload", metav1.GetOptions{})
			if condition == "namespace opt-out" {
				ns.Labels = nil
				_, _ = client.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
			} else {
				_ = client.CoreV1().Pods("workload").Delete(ctx, pod.Name, metav1.DeleteOptions{})
			}
			if err := r.ReconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if repo.moves[0].Status.Phase != "Pending" || repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
				t.Fatal(repo.moves[0].Status)
			}
			assertNoEviction(t, client)
			if condition == "namespace opt-out" {
				ns.Labels = map[string]string{admissionNamespaceLabel: "enabled"}
				_, _ = client.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
			} else {
				_, _ = client.CoreV1().Pods("workload").Create(ctx, pod, metav1.CreateOptions{})
			}
			if err := r.ReconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if repo.moves[0].Status.Phase != string(fsm.PhasePreparing) {
				t.Fatal(repo.moves[0].Status)
			}
		})
	}
}

func TestPreflightLateSecondConsumerAfterSourceClaimDoesNotEvict(t *testing.T) {
	ctx := context.Background()
	r, repo, client := pendingPreflightFixture()
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.volumes[preflightVolume].ActiveMove != repo.moves[0].Name || repo.volumes[preflightVolume].Phase != volumeapi.PhaseReady {
		t.Fatal("source was not claimed without disruption")
	}
	if repo.moves[0].Status.Phase != string(fsm.PhasePreparing) {
		t.Fatal(repo.moves[0].Status)
	}
	pod, _ := client.CoreV1().Pods("workload").Get(ctx, "consumer", metav1.GetOptions{})
	pod.Name, pod.UID = "another", "another-uid"
	_, _ = client.CoreV1().Pods("workload").Create(ctx, pod, metav1.CreateOptions{})
	if err := r.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.moves[0].Status.Phase != string(fsm.PhasePreparing) || repo.moves[0].Status.Reason != "MultipleConsumers" {
		t.Fatal(repo.moves[0].Status)
	}
	assertNoEviction(t, client)
}
