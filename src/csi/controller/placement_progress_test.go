package controller

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// Exercise the API-backed registration and ledger, while keeping filesystem
// effects and the consumer's Kubernetes reads injectable.
type placementProgressFixture struct {
	api      *dynamicfake.FakeDynamicClient
	registry *volumeapi.Registry
	client   *fake.Clientset
	service  *Service
	request  *csi.CreateVolumeRequest
	probe    *fakePoolCapacityProbe
	effects  *fakeDirectoryOperator
	retries  *retryRecorder
}

func newPlacementProgressFixture(t *testing.T) *placementProgressFixture {
	t.Helper()
	api, registry, newService, probe := initializationFixture(t)
	pvc := scheduledScratchPVC("worker-a")
	pvc.OwnerReferences = nil
	client := fake.NewClientset(pvc, scratchConsumerPod(""), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pvc.Namespace}})
	service := newService()
	service.Client = client
	service.ConsumerPlacement = consumer.Inspector{Reader: consumer.KubernetesReader{Client: client}}
	retries := &retryRecorder{}
	service.RetryRequests = retries
	req := validCreateRequest("worker-a")
	req.Parameters = map[string]string{PVCNamespaceKey: pvc.Namespace, PVCNameKey: pvc.Name, PoolGroupKey: "group-b"}
	f := &placementProgressFixture{api: api, registry: registry, client: client, service: service, request: req,
		probe: probe, effects: service.Operator.(*fakeDirectoryOperator), retries: retries}
	f.updatePool(t, "pool-b", func(pool *unstructured.Unstructured) {
		if err := unstructured.SetNestedField(pool.Object, "group-b", "spec", "poolGroup"); err != nil {
			t.Fatal(err)
		}
	})
	return f
}

func TestPlacementProgressReschedulesToRegisteredGroup(t *testing.T) {
	f := newPlacementProgressFixture(t)
	ctx := context.Background()
	if _, err := f.service.CreateVolume(ctx, f.request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("node without requested group must permit rescheduling: %v", err)
	}
	f.assertUnstarted(t, 0)
	if len(f.retries.calls) != 0 {
		t.Fatalf("reschedulable request retained a node-local waiter: %+v", f.retries.calls)
	}
	// The provisioner/scheduler owns the annotation change. The CSI service
	// signals rescheduling and must not create an intent on the first node.
	f.selectNode(t, "worker-b")
	if _, err := f.service.CreateVolume(ctx, f.request); err != nil {
		t.Fatalf("retry on the registered group node did not progress: %v", err)
	}
	f.assertCreated(t, "worker-b", "pool-b-uid", "")
}

func TestPlacementProgressKeepsFixedAndUnknownConsumers(t *testing.T) {
	for name, consumerNode := range map[string]string{"fixed": "worker-a", "unknown": "worker-b"} {
		t.Run(name, func(t *testing.T) {
			f := newPlacementProgressFixture(t)
			pod, err := f.client.CoreV1().Pods("vmtest").Get(context.Background(), "importer", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Spec.NodeName = consumerNode
			if _, err := f.client.CoreV1().Pods("vmtest").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := f.service.CreateVolume(context.Background(), f.request); status.Code(err) != codes.Unavailable {
					t.Fatalf("%s consumer lost its selected node: %v", name, err)
				}
			}
			f.assertUnstarted(t, 0)
			f.assertWaitingOnOriginalNode(t)
		})
	}
}

func TestPlacementProgressWaitsForRegisteredPoolObservation(t *testing.T) {
	for _, unavailable := range []string{"NotReady", "stale generation", "incomplete inventory"} {
		t.Run(unavailable, func(t *testing.T) {
			f := newPlacementProgressFixture(t)
			f.updatePool(t, "pool-a", func(pool *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(pool.Object, "group-b", "spec", "poolGroup"); err != nil {
					t.Fatal(err)
				}
			})
			ready, err := f.api.Resource(volumeapi.PoolResource).Get(context.Background(), "pool-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			f.updatePool(t, "pool-a", func(pool *unstructured.Unstructured) {
				switch unavailable {
				case "NotReady":
					conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
					conditions[0].(map[string]any)["status"] = "False"
					conditions[0].(map[string]any)["reason"] = "MountUnavailable"
					if err := unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions"); err != nil {
						t.Fatal(err)
					}
				case "stale generation":
					pool.SetGeneration(2)
				case "incomplete inventory":
					if err := unstructured.SetNestedField(pool.Object, false, "status", "inventory", "valid"); err != nil {
						t.Fatal(err)
					}
				}
			})
			if _, err := f.service.CreateVolume(context.Background(), f.request); status.Code(err) != codes.Unavailable {
				t.Fatalf("registered Pool observation failure must preserve placement: %v", err)
			}
			f.assertUnstarted(t, 0)
			f.assertWaitingOnOriginalNode(t)
			// A fresh controller must converge from API observations, without an
			// in-memory placement choice or an intent from the failed attempt.
			f.updatePool(t, "pool-a", func(pool *unstructured.Unstructured) {
				pool.Object["status"] = ready.Object["status"]
				if err := unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration"); err != nil {
					t.Fatal(err)
				}
				conditions, _, _ := unstructured.NestedSlice(pool.Object, "status", "conditions")
				conditions[0].(map[string]any)["observedGeneration"] = pool.GetGeneration()
				if err := unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions"); err != nil {
					t.Fatal(err)
				}
			})
			f.service = configuredService(&Service{
				Client: f.client, Namespace: "shiftpv-system", Operator: f.effects,
				Volumes: f.registry, CapacityPools: f.registry, CapacityProbe: f.probe,
				Cleanups: f.service.Cleanups, RetryRequests: f.retries,
			})
			if _, err := f.service.CreateVolume(context.Background(), f.request); err != nil {
				t.Fatalf("recovered observation did not resume creation: %v", err)
			}
			f.assertCreated(t, "worker-a", "pool-a-uid", "")
		})
	}
}

func TestPlacementProgressKeepsLegacyInitializationWithoutCandidate(t *testing.T) {
	f := newPlacementProgressFixture(t)
	// Start with no registration on the legacy request's immutable node. The
	// recovery below registers a new Pool instead of changing a Pool's group.
	newPool, err := f.api.Resource(volumeapi.PoolResource).Get(context.Background(), "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.api.Tracker().Delete(volumeapi.PoolResource, "", "pool-a"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(newPool.Object, "group-b", "spec", "poolGroup"); err != nil {
		t.Fatal(err)
	}
	id := legacyInitialization(t, f.api, f.request.Name)
	original, err := f.registry.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.CreateVolume(context.Background(), f.request); status.Code(err) != codes.Unavailable {
		t.Fatalf("legacy initialNode was released without a candidate: %v", err)
	}
	f.assertUnstarted(t, 1)
	f.assertWaitingOnOriginalNode(t)
	waiting, err := f.registry.Get(context.Background(), id)
	if err != nil || waiting.UID != original.UID || !waiting.Initializing() || waiting.InitialPool != nil || waiting.InitialNode != "worker-a" {
		t.Fatalf("unavailable placement changed the legacy intent: %+v %v", waiting, err)
	}
	observedStatus := newPool.Object["status"]
	unstructured.RemoveNestedField(newPool.Object, "status")
	registered, err := f.api.Resource(volumeapi.PoolResource).Create(context.Background(), newPool, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The API does not retain status supplied at Create. Model the node's
	// subsequent successful observation before retrying the original request.
	registered.Object["status"] = observedStatus
	if _, err := f.api.Resource(volumeapi.PoolResource).UpdateStatus(context.Background(), registered, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.CreateVolume(context.Background(), f.request); err != nil {
		t.Fatalf("legacy creation did not converge after registration: %v", err)
	}
	f.assertCreated(t, "worker-a", "pool-a-uid", original.UID)
}

func (f *placementProgressFixture) updatePool(t *testing.T, name string, change func(*unstructured.Unstructured)) {
	t.Helper()
	pool, err := f.api.Resource(volumeapi.PoolResource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change(pool)
	if _, err := f.api.Resource(volumeapi.PoolResource).Update(context.Background(), pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func (f *placementProgressFixture) selectNode(t *testing.T, node string) {
	t.Helper()
	pvc, err := f.client.CoreV1().PersistentVolumeClaims("vmtest").Get(context.Background(), "scratch", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pvc.Annotations[consumer.SelectedNode] = node
	if _, err := f.client.CoreV1().PersistentVolumeClaims("vmtest").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.request.AccessibilityRequirements.Preferred[0].Segments[TopologyKey] = node
}

func (f *placementProgressFixture) assertUnstarted(t *testing.T, volumeCount int) {
	t.Helper()
	states, err := f.registry.ListVolumes(context.Background())
	if err != nil || len(states) != volumeCount || f.probe.callCount() != 0 || f.effects.createCalls != 0 {
		t.Fatalf("placement denial allocated state or ran effects: volumes=%+v probes=%d effects=%d err=%v", states, f.probe.callCount(), f.effects.createCalls, err)
	}
	moves, err := f.registry.ListMoves(context.Background())
	if err != nil || len(moves) != 0 {
		t.Fatalf("placement denial created Move holds: %+v %v", moves, err)
	}
	assertInitialReservation(t, f.registry, "pool-a-uid", 0)
	assertInitialReservation(t, f.registry, "pool-b-uid", 0)
}

func (f *placementProgressFixture) assertWaitingOnOriginalNode(t *testing.T) {
	t.Helper()
	pvc, err := f.client.CoreV1().PersistentVolumeClaims("vmtest").Get(context.Background(), "scratch", metav1.GetOptions{})
	if err != nil || pvc.Annotations[consumer.SelectedNode] != "worker-a" || len(f.retries.calls) == 0 {
		t.Fatalf("selected node or retry request lost: pvc=%+v retries=%+v err=%v", pvc, f.retries.calls, err)
	}
	for i, call := range f.retries.calls {
		if call != (consumer.Request{Namespace: "vmtest", Name: "scratch", UID: "uid", Node: "worker-a"}) || f.retries.groups[i] != "group-b" {
			t.Fatalf("retry uses another placement: %+v", f.retries)
		}
	}
}

func (f *placementProgressFixture) assertCreated(t *testing.T, node, poolUID, expectedUID string) {
	t.Helper()
	id, _ := volume.IDFromName(f.request.Name)
	states, err := f.registry.ListVolumes(context.Background())
	state := states[id]
	if err != nil || len(states) != 1 || state.Phase != volumeapi.PhaseReady || state.OwnerNode != node || state.InitialNode != node || state.CurrentCopy == nil || state.CurrentCopy.PoolUID != poolUID || (expectedUID != "" && state.UID != expectedUID) {
		t.Fatalf("creation changed identity or placement: states=%+v err=%v", states, err)
	}
	if f.probe.callCount() != 1 || f.effects.createCalls != 1 || f.effects.createdNode != node {
		t.Fatalf("creation effects were duplicated or misplaced: probes=%d effects=%+v", f.probe.callCount(), f.effects)
	}
	for _, uid := range []string{"pool-a-uid", "pool-b-uid"} {
		want := int64(0)
		if uid == poolUID {
			want = f.request.CapacityRange.RequiredBytes
		}
		assertInitialReservation(t, f.registry, uid, want)
	}
}
