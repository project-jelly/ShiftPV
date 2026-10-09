package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func readyIsolationPool() *unstructured.Unstructured {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "healthy", "uid": "healthy-uid", "generation": int64(1), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":     map[string]any{"nodeName": "worker", "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}},
		"status": map[string]any{
			"registrationApproved": true, "observedGeneration": int64(1), "lastProbeTime": now,
			"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "PoolReady", "observedGeneration": int64(1), "lastTransitionTime": now}},
			"inventory":  map[string]any{"valid": true, "observedAt": now},
		},
	}}
}

func TestPoolObservationIsolatesRejectedRegistrations(t *testing.T) {
	for _, path := range []string{"/.", "//", "/.."} {
		for _, approved := range []bool{false, true} {
			name := path + "/candidate"
			if approved {
				name = path + "/approved-peer"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				healthy := readyIsolationPool()
				rejected := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
					"metadata": map[string]any{"name": "rejected", "uid": "rejected-uid"},
					"spec":     map[string]any{"nodeName": "worker", "mountPath": path, "capacity": map[string]any{"limit": "1Gi"}},
					"status":   map[string]any{"registrationApproved": approved},
				}}
				client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, healthy)
				registry := &volumeapi.Registry{Client: client}
				r := newTestReconciler(fake.NewClientset(), registry)
				baseline, err := r.observePools(ctx)
				if err != nil || len(baseline.ready) != 1 {
					t.Fatalf("healthy baseline: %+v, %v", baseline, err)
				}
				if _, err := client.Resource(volumeapi.PoolResource).Create(ctx, rejected, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				index, err := r.observePools(ctx)
				if err != nil || len(index.registered) != 2 {
					t.Fatalf("registration erased identity or blocked observation: %+v, %v", index, err)
				}
				_, healthyReady := index.ready["healthy-uid"]
				if healthyReady == approved {
					t.Fatalf("approved=%v healthyReady=%v; candidates must be isolated and approved peer evidence retained", approved, healthyReady)
				}
				copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "rejected", PoolUID: "rejected-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-uid", NodeName: "worker", Role: volume.RoleServing}
				if _, found := indexedCopyPool(index.registered, &copy); !found {
					t.Fatal("referenced rejected Pool identity was hidden")
				}
				if _, found := indexedCopyPool(index.ready, &copy); found {
					t.Fatal("invalid referenced Pool became a usable candidate")
				}
				r.Client = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}})
				observed := observation{Volume: volumeapi.State{CurrentCopy: &copy}}
				move := volumeapi.Move{Spec: volumeapi.MoveSpec{SourceNode: "worker"}}
				if _, err := r.observeSource(ctx, move, index, &observed); err != nil || observed.FSM.SourceHealthy {
					t.Fatalf("referenced invalid source was accepted: %+v, %v", observed.FSM, err)
				}
			})
		}
	}
}

func TestPoolObservationRejectsUntrustworthySnapshot(t *testing.T) {
	for _, failure := range []string{"list", "decode", "duplicate UID"} {
		t.Run(failure, func(t *testing.T) {
			pool := readyIsolationPool()
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{volumeapi.PoolResource: "ShiftPVPoolList"}, pool)
			sentinel := errors.New("Pool API unavailable")
			switch failure {
			case "list":
				client.PrependReactor("list", "shiftpvpools", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, sentinel })
			case "decode":
				_ = unstructured.SetNestedField(pool.Object, "invalid", "status", "observedGeneration")
				_, _ = client.Resource(volumeapi.PoolResource).UpdateStatus(context.Background(), pool, metav1.UpdateOptions{})
			case "duplicate UID":
				peer := pool.DeepCopy()
				peer.SetName("duplicate")
				_, _ = client.Resource(volumeapi.PoolResource).Create(context.Background(), peer, metav1.CreateOptions{})
			}
			r := newTestReconciler(fake.NewClientset(), &volumeapi.Registry{Client: client})
			if _, err := r.observePools(context.Background()); err == nil || failure == "list" && !errors.Is(err, sentinel) {
				t.Fatalf("untrustworthy %s snapshot accepted: %v", failure, err)
			}
		})
	}
}

type discoveryFailureRepository struct {
	*memoryRepository
	failure error
}

func (r *discoveryFailureRepository) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return nil, r.failure
}

func TestDiscoveryFailureDoesNotPreventApprovedMoveCompletion(t *testing.T) {
	r, repo, _ := cleanupMoveFixture()
	settleCleanupFixture(t, r, repo)
	repo.moves[0].Status.Phase = string(fsm.PhaseCompleting)
	sentinel := errors.New("discovery unavailable")
	r.Repository = &discoveryFailureRepository{memoryRepository: repo, failure: sentinel}
	if err := r.ReconcileAll(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("discovery error lost: %v", err)
	}
	move := repo.moves[0]
	state := repo.volumes[move.Spec.VolumeID]
	if move.Status.Phase != string(fsm.PhaseSucceeded) || state.ActiveMove != "" || state.OwnerNode != "destination" {
		t.Fatalf("approved completion was starved: move=%+v state=%+v", move.Status, state)
	}
}

func TestDiscoveryIsolatesOwnerReadFailure(t *testing.T) {
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	otherID := "shiftpv-1123456789abcdef0123456789abcdef"
	otherBadID := "shiftpv-2123456789abcdef0123456789abcdef"
	repo := &memoryRepository{volumes: map[string]volumeapi.State{id: {Phase: volumeapi.PhaseReady, OwnerNode: "failing"}, otherBadID: {Phase: volumeapi.PhaseReady, OwnerNode: "failing"}, otherID: {Phase: volumeapi.PhaseReady, OwnerNode: "healthy"}}}
	client := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "healthy"}})
	sentinel := errors.New("owner read unavailable")
	client.PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.GetAction).GetName() == "failing" {
			return true, nil, sentinel
		}
		return false, nil, nil
	})
	r := newTestReconciler(client, repo)
	if err := r.discoverMoves(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("owner read failure lost: %v", err)
	}
	healthyReads := 0
	failedReads := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "nodes" {
			switch action.(ktesting.GetAction).GetName() {
			case "healthy":
				healthyReads++
			case "failing":
				failedReads++
			}
		}
	}
	if healthyReads != 1 || failedReads != 2 || len(repo.moves) != 0 {
		t.Fatalf("discovery skipped a volume or granted work without preflight: healthy=%d failed=%d moves=%v", healthyReads, failedReads, repo.moves)
	}
}
