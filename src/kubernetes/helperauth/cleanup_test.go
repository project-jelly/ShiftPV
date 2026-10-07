package helperauth

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestVolumeCleanupAuthorityRequiresDurableDeletionFence(t *testing.T) {
	const volumeID = "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	copy := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": copy.VolumeUID},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.VolumeResource: "ShiftPVVolumeList"}, object)
	registry := &volumeapi.Registry{Client: dynamicClient}
	cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
		OperationID: "delete-" + copy.VolumeUID, Target: copy, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: volumeID, UID: copy.VolumeUID},
	}}
	setVolumeStateFixture(t, dynamicClient, volumeID, volumeapi.State{Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy})
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
		t.Fatal("Ready volume was accepted without a deletion fence")
	}
	if _, err := registry.BeginDelete(context.Background(), volumeID, copy.VolumeUID, copy); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err != nil {
		t.Fatalf("durably fenced deletion was rejected: %v", err)
	}
	state, err := registry.Get(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	state.PublishedNodes = []string{copy.NodeName}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
		t.Fatal("published volume crossed deletion authority")
	}
}

func TestUnknownOrphanCleanupHasNoExecutableAuthority(t *testing.T) {
	cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
		OperationID: "review-orphan", Reason: "OrphanReclaim",
		Target: volume.CopyIdentity{
			InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
			VolumeID: "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", VolumeUID: "volume-uid",
			CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
		},
		Authority: cleanupapi.Authority{Kind: "Namespace", Name: "kube-system", UID: "installation"},
	}}
	if err := cleanup.Spec.Validate(); err == nil {
		t.Fatal("orphan cleanup intent unexpectedly validated")
	}
	if err := VerifyCleanupAuthority(context.Background(), nil, cleanup); err == nil {
		t.Fatal("unknown orphan observation gained destructive authority")
	}
}

func TestCleanupExecutorMustMatchExactRunningPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-uid"}, Spec: corev1.PodSpec{NodeName: "worker-a"}}
	executor := &cleanupapi.Executor{JobName: "cleanup-job", JobUID: "job-uid", PodUID: "pod-uid", NodeName: "worker-a"}
	if !MatchesCleanupExecutor(executor, "cleanup-job", "job-uid", pod) {
		t.Fatal("exact Pod-bound executor was rejected")
	}
	for name, mutate := range map[string]func(*cleanupapi.Executor, *corev1.Pod){
		"missing executor": func(current *cleanupapi.Executor, _ *corev1.Pod) { *current = cleanupapi.Executor{} },
		"job name":         func(current *cleanupapi.Executor, _ *corev1.Pod) { current.JobName = "replacement" },
		"job UID":          func(current *cleanupapi.Executor, _ *corev1.Pod) { current.JobUID = "replacement" },
		"Pod UID":          func(current *cleanupapi.Executor, _ *corev1.Pod) { current.PodUID = "replacement" },
		"node":             func(_ *cleanupapi.Executor, currentPod *corev1.Pod) { currentPod.Spec.NodeName = "worker-b" },
	} {
		t.Run(name, func(t *testing.T) {
			changedExecutor := *executor
			changedPod := pod.DeepCopy()
			mutate(&changedExecutor, changedPod)
			if MatchesCleanupExecutor(&changedExecutor, "cleanup-job", "job-uid", changedPod) {
				t.Fatal("changed executor identity was accepted")
			}
		})
	}
	if MatchesCleanupExecutor(nil, "cleanup-job", "job-uid", pod) || MatchesCleanupExecutor(executor, "cleanup-job", "job-uid", nil) {
		t.Fatal("missing executor or Pod was accepted")
	}
}

func TestOwnedByCleanupParentRequiresExactControllerReference(t *testing.T) {
	authority := cleanupapi.Authority{Kind: "ShiftPVVolume", Name: "shiftpv-volume", UID: "volume-uid"}
	controller := true
	owner := metav1.OwnerReference{APIVersion: "shiftpv.io/v1alpha1", Kind: authority.Kind, Name: authority.Name, UID: types.UID(authority.UID), Controller: &controller}
	if !OwnedByCleanupParent([]metav1.OwnerReference{owner}, authority) {
		t.Fatal("exact parent-owned reference was rejected")
	}
	if OwnedByCleanupParent(nil, authority) {
		t.Fatal("missing owner reference was accepted")
	}
	for name, mutate := range map[string]func(*metav1.OwnerReference){
		"group":      func(current *metav1.OwnerReference) { current.APIVersion = "batch/v1" },
		"kind":       func(current *metav1.OwnerReference) { current.Kind = "ShiftPVMove" },
		"name":       func(current *metav1.OwnerReference) { current.Name = "replacement" },
		"UID":        func(current *metav1.OwnerReference) { current.UID = "replacement" },
		"controller": func(current *metav1.OwnerReference) { current.Controller = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := owner
			mutate(&changed)
			if OwnedByCleanupParent([]metav1.OwnerReference{changed}, authority) {
				t.Fatal("changed parent reference was accepted")
			}
		})
	}
}

func TestCleanupAuthorityRechecksIntentPoolAndParentState(t *testing.T) {
	const volumeID = "shiftpv-cccccccccccccccccccccccccccccccc"
	target := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "copy-a", NodeName: "worker-a", Role: volume.RoleServing,
	}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": target.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	poolObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": target.PoolName, "uid": target.PoolUID, "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": target.NodeName, "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}},
	}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "kube-system", "uid": target.InstallationID},
	}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, volumeObject, poolObject, clusterIdentity)
	registry := &volumeapi.Registry{Client: dynamicClient}
	cleanups := &cleanupapi.Store{Client: dynamicClient}
	setVolumeStateFixture(t, dynamicClient, volumeID, volumeapi.State{Phase: volumeapi.PhaseReady, OwnerNode: target.NodeName, CurrentCopy: &target})
	if _, err := registry.BeginDelete(context.Background(), volumeID, target.VolumeUID, target); err != nil {
		t.Fatal(err)
	}
	spec := cleanupapi.Spec{
		OperationID: "delete-" + target.VolumeUID, Target: target, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: volumeID, UID: target.VolumeUID},
	}
	pending, err := cleanups.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	executor := cleanupapi.Executor{JobName: "cleanup-job", JobUID: "job-uid", NodeName: target.NodeName}
	if err := cleanups.UpdateStatus(context.Background(), pending, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &executor}); err != nil {
		t.Fatal(err)
	}
	running, err := cleanups.Get(context.Background(), spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	bound := executor
	bound.PodUID = "pod-uid"
	if err := cleanups.UpdateStatus(context.Background(), running, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &bound}); err != nil {
		t.Fatal(err)
	}
	approved, err := cleanups.Get(context.Background(), spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-pod", Namespace: "system", UID: "pod-uid"}, Spec: corev1.PodSpec{NodeName: target.NodeName}}
	options := CleanupOptions{Authority: spec.Authority, Approved: approved, JobName: executor.JobName, JobUID: executor.JobUID, Pod: pod}
	if err := CleanupAuthority(cleanups, registry, options)(context.Background(), false); err != nil {
		t.Fatalf("exact cleanup authority rejected: %v", err)
	}
	replacementPod := pod.DeepCopy()
	replacementPod.UID = "replacement-pod"
	replaced := options
	replaced.Pod = replacementPod
	if err := CleanupAuthority(cleanups, registry, replaced)(context.Background(), false); err == nil {
		t.Fatal("replacement executor Pod kept destructive authority")
	}
	pool, err := dynamicClient.Resource(volumeapi.PoolResource).Get(context.Background(), target.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pool.SetFinalizers(nil)
	if _, err := dynamicClient.Resource(volumeapi.PoolResource).Update(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := CleanupAuthority(cleanups, registry, options)(context.Background(), false); err == nil {
		t.Fatal("unprotected Pool kept destructive authority")
	}
	// Restore the Pool so the next step fails only on the installation identity.
	pool.SetFinalizers([]string{volumeapi.PoolProtectionFinalizer})
	if _, err := dynamicClient.Resource(volumeapi.PoolResource).Update(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := CleanupAuthority(cleanups, registry, options)(context.Background(), false); err != nil {
		t.Fatalf("restored Pool protection was rejected: %v", err)
	}
	if err := dynamicClient.Resource(namespaceResource).Delete(context.Background(), "kube-system", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := CleanupAuthority(cleanups, registry, options)(context.Background(), false); err == nil {
		t.Fatal("unreadable installation identity kept destructive authority")
	}
}

func TestRecoveryCleanupAuthorityRequiresExactTargetAndRetainedOwner(t *testing.T) {
	const (
		volumeID = "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		moveName = "move-recovery"
		moveUID  = "move-recovery-uid"
	)
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.Role = volume.RoleServing

	for name, test := range map[string]struct {
		reason, operationID, recoveryOwner string
		target, current                    volume.CopyIdentity
	}{
		"precommit rollback": {
			reason: "MoveRollback", operationID: "rollback-" + moveUID, recoveryOwner: source.NodeName,
			target: incoming, current: source,
		},
		"postcommit source cleanup": {
			reason: "MoveSource", operationID: "cleanup-" + moveUID, recoveryOwner: destination.NodeName,
			target: source, current: destination,
		},
	} {
		t.Run(name, func(t *testing.T) {
			moveObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
				"metadata": map[string]any{"name": moveName, "uid": moveUID},
				"spec":     map[string]any{"volumeID": volumeID, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
			}}
			volumeObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
				"metadata": map[string]any{"name": volumeID, "uid": source.VolumeUID},
				"spec":     map[string]any{"volumeID": volumeID},
			}}
			poolObject := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
				"metadata": map[string]any{
					"name": destination.PoolName, "uid": destination.PoolUID, "generation": int64(1),
					"finalizers": []any{volumeapi.PoolProtectionFinalizer},
				},
				"spec": map[string]any{"nodeName": destination.NodeName, "mountPath": "/destination-pool", "capacity": map[string]any{"limit": "1Gi"}},
			}}
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
			}, moveObject, volumeObject, poolObject)
			registry := &volumeapi.Registry{Client: client}
			moveStatus := volumeapi.MoveStatus{
				Phase: "Blocked", RecoveryPhase: "Retiring", RecoveryOwner: test.recoveryOwner,
				DestinationNode: destination.NodeName, DestinationPoolUID: destination.PoolUID,
				SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			}
			if err := registry.SetMoveStatus(context.Background(), moveName, moveUID, moveStatus); err != nil {
				t.Fatal(err)
			}
			statePhase := volumeapi.PhaseBlocked
			if test.reason == "MoveSource" {
				statePhase = volumeapi.PhaseReady
			}
			state := volumeapi.State{
				UID: source.VolumeUID, Phase: statePhase, OwnerNode: test.current.NodeName,
				ActiveMove: moveName, CurrentCopy: &test.current, PublishedNodes: []string{test.current.NodeName},
			}
			setVolumeStateFixture(t, client, volumeID, state)
			if test.reason == "MoveSource" {
				now := metav1.Now()
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			cleanup := cleanupapi.Cleanup{Spec: cleanupapi.Spec{
				OperationID: test.operationID, Target: test.target, Reason: test.reason,
				Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: moveName, UID: moveUID},
			}}
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err != nil {
				t.Fatalf("exact recovery cleanup rejected: %v", err)
			}
			if test.reason == "MoveSource" {
				state.PublishedNodes = nil
				setVolumeStateFixture(t, client, volumeID, state)
				if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
					t.Fatal("postcommit source cleanup was accepted before destination publish intent")
				}
				state.PublishedNodes = []string{test.current.NodeName}
				setVolumeStateFixture(t, client, volumeID, state)
				now := metav1.Now()
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: false}},
					},
				}); err != nil {
					t.Fatal(err)
				}
				if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
					t.Fatal("postcommit source cleanup was accepted before scanner publication proof")
				}
				if err := registry.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
					ObservedGeneration: 1,
					LastProbeTime:      now,
					Conditions:         []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
					Inventory: &volumeapi.PoolInventory{
						ObservedAt: now, Valid: true,
						Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			unknownReason := cleanup
			unknownReason.Spec.Reason = "MoveRelocate"
			if err := VerifyCleanupAuthority(context.Background(), registry, unknownReason); err == nil {
				t.Fatal("unimplemented move cleanup reason was accepted")
			}
			cleanup.Spec.OperationID = "replacement-operation"
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
				t.Fatal("replacement cleanup operation was accepted")
			}
			cleanup.Spec.OperationID = test.operationID
			state.PublishedNodes = append(state.PublishedNodes, test.target.NodeName)
			setVolumeStateFixture(t, client, volumeID, state)
			if err := VerifyCleanupAuthority(context.Background(), registry, cleanup); err == nil {
				t.Fatal("published cleanup target was accepted")
			}
		})
	}
}

// TestMoveSourceCleanupAuthorityHonorsOperatorPoolReadinessBudget is the
// motivating case for forwarding --pool-readiness-stale-after to the cleanup
// helper. The destination Pool probe is older than the operator's tightened
// budget but still inside the compiled default, so a helper Registry left at
// zero accepts a publication proof the controller already treats as stale and
// would purge the retained source copy on it.
//
// This pins the consumer half of the contract and holds on its own: it builds
// the Registry directly, so it does not exercise the CLI plumbing. The other
// half — that the parsed flag actually reaches this Registry, and that the
// controller puts the flag on the cleanup Job — is pinned by
// TestHelperForwardsPoolReadinessBudgetToRegistry and
// TestCleanupJobForwardsPoolReadinessBudget.
func TestMoveSourceCleanupAuthorityHonorsOperatorPoolReadinessBudget(t *testing.T) {
	const (
		volumeID = "shiftpv-dddddddddddddddddddddddddddddddd"
		moveName = "move-stale-budget"
		moveUID  = "move-stale-budget-uid"
	)
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing,
	}
	incoming := volume.CopyIdentity{
		InstallationID: source.InstallationID, PoolName: "destination-pool", PoolUID: "destination-pool-uid",
		VolumeID: volumeID, VolumeUID: source.VolumeUID, CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming,
	}
	destination := incoming
	destination.Role = volume.RoleServing

	moveObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": moveName, "uid": moveUID, "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": volumeID, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
	}}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": volumeID, "uid": source.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": volumeID},
	}}
	sourcePoolObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": source.PoolName, "uid": source.PoolUID, "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": source.NodeName, "mountPath": "/source-pool"},
	}}
	destinationPoolObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{
			"name": destination.PoolName, "uid": destination.PoolUID, "generation": int64(1),
			"finalizers": []any{volumeapi.PoolProtectionFinalizer},
		},
		"spec": map[string]any{"nodeName": destination.NodeName, "mountPath": "/destination-pool", "capacity": map[string]any{"limit": "1Gi"}},
	}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "kube-system", "uid": source.InstallationID},
	}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
		volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, moveObject, volumeObject, sourcePoolObject, destinationPoolObject, clusterIdentity)

	// The probe and inventory are two minutes old: fresh under the compiled
	// three-minute default, stale under the one-minute budget the operator set.
	probeAt := metav1.NewTime(time.Now().Truncate(time.Second))
	observed := probeAt.Time.Add(2 * time.Minute)

	setup := &volumeapi.Registry{Client: client}
	moveStatus := volumeapi.MoveStatus{
		Phase: "Blocked", RecoveryPhase: "Retiring", RecoveryOwner: destination.NodeName,
		DestinationNode: destination.NodeName, DestinationPoolUID: destination.PoolUID,
		SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
	}
	if err := setup.SetMoveStatus(context.Background(), moveName, moveUID, moveStatus); err != nil {
		t.Fatal(err)
	}
	setVolumeStateFixture(t, client, volumeID, volumeapi.State{
		UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: destination.NodeName,
		ActiveMove: moveName, CurrentCopy: &destination, PublishedNodes: []string{destination.NodeName},
	})
	if err := setup.SetPoolStatus(context.Background(), destination.PoolName, destination.PoolUID, destination.NodeName, volumeapi.PoolStatus{
		ObservedGeneration: 1,
		LastProbeTime:      probeAt,
		Conditions:         []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		Inventory: &volumeapi.PoolInventory{
			ObservedAt: probeAt, Valid: true,
			Copies: []volumeapi.CopyObservation{{Identity: &destination, Present: true, Published: true}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	cleanups := &cleanupapi.Store{Client: client}
	spec := cleanupapi.Spec{
		OperationID: "cleanup-" + moveUID, Target: source, Reason: "MoveSource",
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: moveName, UID: moveUID},
	}
	pending, err := cleanups.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	executor := cleanupapi.Executor{JobName: "cleanup-job", JobUID: "job-uid", NodeName: source.NodeName}
	if err := cleanups.UpdateStatus(context.Background(), pending, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &executor}); err != nil {
		t.Fatal(err)
	}
	running, err := cleanups.Get(context.Background(), spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	bound := executor
	bound.PodUID = "pod-uid"
	if err := cleanups.UpdateStatus(context.Background(), running, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &bound}); err != nil {
		t.Fatal(err)
	}
	approved, err := cleanups.Get(context.Background(), spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-pod", Namespace: "system", UID: "pod-uid"}, Spec: corev1.PodSpec{NodeName: source.NodeName}}
	options := CleanupOptions{Authority: spec.Authority, Approved: approved, JobName: executor.JobName, JobUID: executor.JobUID, Pod: pod}

	registryAt := func(staleAfter time.Duration) *volumeapi.Registry {
		return &volumeapi.Registry{
			Client:                  client,
			PoolReadinessStaleAfter: staleAfter,
			Now:                     func() time.Time { return observed },
		}
	}
	if err := CleanupAuthority(cleanups, registryAt(0), options)(context.Background(), false); err != nil {
		t.Fatalf("publication proof inside the compiled default was rejected: %v", err)
	}
	if err := CleanupAuthority(cleanups, registryAt(volumeapi.DefaultPoolReadinessStaleAfter), options)(context.Background(), false); err != nil {
		t.Fatalf("publication proof inside the explicit default budget was rejected: %v", err)
	}
	err = CleanupAuthority(cleanups, registryAt(time.Minute), options)(context.Background(), false)
	if err == nil {
		t.Fatal("publication proof older than the operator budget kept destructive authority")
	}
	if !errors.Is(err, volumeapi.ErrPoolNotReady) {
		t.Fatalf("stale publication proof was rejected for the wrong reason: %v", err)
	}
}
