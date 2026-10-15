package controller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

const preparationRequestedBytes int64 = 2 << 30

// This fixture exercises the production Registry. The fake API only supplies
// UID/resourceVersion storage semantics; CRD validation and real scheduler
// behavior remain covered by the API primitive and Kind tests.
type preparationRegistryFixture struct {
	ctx                  context.Context
	dynamic              *dynamicfake.FakeDynamicClient
	client               *fake.Clientset
	registry             *volumeapi.Registry
	reconciler           *Reconciler
	workloads            []*mobilityFixture
	volumeIDs, moveNames []string
	mu                   sync.Mutex
	sequence             int64
	// Called after storage, with no nested fake-client calls or blocking waits.
	afterWrite func(schema.GroupVersionResource, *unstructured.Unstructured) error
}

func newPreparationRegistryFixture(t *testing.T, count int) *preparationRegistryFixture {
	t.Helper()
	f := &preparationRegistryFixture{ctx: context.Background(), sequence: 100}
	objects := []runtime.Object{preparationPool("source", "10Gi"), preparationPool("destination", "5Gi")}
	coreObjects := []runtime.Object{readyNode("source", true), readyNode("destination", false)}
	for index := range count {
		id := fmt.Sprintf("shiftpv-%032x", index+1)
		name := fmt.Sprintf("move-%d", index+1)
		w := newMobilityFixture(id)
		w.SourceNode, w.DestinationNode = nil, nil
		w.Namespace.Name = fmt.Sprintf("workload-%d", index+1)
		w.PV.Name, w.PV.UID = fmt.Sprintf("pv-%d", index+1), types.UID(fmt.Sprintf("pv-uid-%d", index+1))
		w.Claim.Name, w.Claim.Namespace = fmt.Sprintf("claim-%d", index+1), w.Namespace.Name
		w.Claim.UID = types.UID(fmt.Sprintf("claim-uid-%d", index+1))
		w.Claim.Spec.VolumeName = w.PV.Name
		w.Claim.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}
		w.Claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		w.Claim.Status.Phase = corev1.ClaimBound
		w.PV.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}
		w.PV.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		w.PV.Status.Phase = corev1.VolumeBound
		w.PV.Spec.ClaimRef = &corev1.ObjectReference{Name: w.Claim.Name, Namespace: w.Claim.Namespace, UID: w.Claim.UID}
		w.Consumer.Name, w.Consumer.Namespace = fmt.Sprintf("consumer-%d", index+1), w.Namespace.Name
		w.Consumer.UID = types.UID(fmt.Sprintf("consumer-uid-%d", index+1))
		w.Consumer.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = w.Claim.Name
		w.Consumer.Status.Phase = corev1.PodRunning
		w.Consumer.Spec.Containers = []corev1.Container{{Name: "workload", Image: "test"}}
		w.ReplicaSet.Namespace = w.Namespace.Name
		w.ReplicaSet.Spec.Template.Spec = *w.Consumer.Spec.DeepCopy()
		w.ReplicaSet.Spec.Template.Spec.NodeName = ""
		for _, object := range w.Objects() {
			accessor, err := meta.Accessor(object)
			if err != nil {
				t.Fatal(err)
			}
			accessor.SetResourceVersion("1")
			if accessor.GetUID() == "" {
				accessor.SetUID(types.UID(accessor.GetName() + "-uid"))
			}
			coreObjects = append(coreObjects, object)
		}
		source, _, _ := testCopyIdentities(id, "source", "destination")
		source.VolumeUID, source.CopyID = fmt.Sprintf("volume-uid-%d", index+1), fmt.Sprintf("source-copy-%d", index+1)
		copy, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&source)
		if err != nil {
			t.Fatal(err)
		}
		sourcePool := objects[0].(*unstructured.Unstructured)
		copies, _, _ := unstructured.NestedSlice(sourcePool.Object, "status", "inventory", "copies")
		copies = append(copies, map[string]any{"identity": copy, "present": true, "published": true})
		if err := unstructured.SetNestedSlice(sourcePool.Object, copies, "status", "inventory", "copies"); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
			"metadata": map[string]any{"name": id, "uid": source.VolumeUID, "resourceVersion": "1", "generation": int64(1), "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
			"spec":     map[string]any{"volumeID": id, "requestName": name, "initialNode": "source", "capacityBytes": preparationRequestedBytes},
			"status":   map[string]any{"phase": "Ready", "ownerNode": "source", "currentCopy": copy, "publishedNodes": []any{"source"}},
		}}, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
			"metadata": map[string]any{"name": name, "uid": name + "-uid", "resourceVersion": "1", "generation": int64(1), "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
			"spec":     map[string]any{"volumeID": id, "sourceNode": "source", "protocolVersion": volumeapi.MoveProtocolPreparedV1},
			"status":   map[string]any{"phase": "Pending"},
		}})
		f.workloads, f.volumeIDs, f.moveNames = append(f.workloads, w), append(f.volumeIDs, id), append(f.moveNames, name)
	}
	f.dynamic = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
	}, objects...)
	f.dynamic.PrependReactor("update", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		object, err := f.commitDynamicUpdate(action.GetResource(), action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured))
		if err == nil && f.afterWrite != nil {
			err = f.afterWrite(action.GetResource(), object.DeepCopy())
		}
		return true, object, err
	})
	f.dynamic.PrependReactor("create", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sequence++
		if object.GetName() == "" {
			object.SetName(object.GetGenerateName() + strconv.FormatInt(f.sequence, 10))
		}
		object.SetUID(types.UID(object.GetName() + "-uid"))
		object.SetResourceVersion("1")
		return false, nil, nil
	})
	f.client = fake.NewSimpleClientset(coreObjects...)
	f.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sequence++
		pod.UID = types.UID(fmt.Sprintf("pod-uid-%d", f.sequence))
		pod.ResourceVersion = "1"
		return false, nil, nil
	})
	f.registry = &volumeapi.Registry{Client: f.dynamic}
	f.reconciler = newTestReconciler(f.client, f.registry, func(r *Reconciler) {
		r.CapacityProbe = fakeMoveCapacityProbe{usage: 1 << 20, stats: poolcapacity.Filesystem{AvailableBytes: 20 << 30}}
		r.Cleanups = &cleanupapi.Store{Client: f.dynamic}
	})
	return f
}

func preparationPool(node, limit string) *unstructured.Unstructured {
	pool := readyIsolationPool()
	pool.SetName(node + "-pool")
	pool.SetUID(types.UID(node + "-pool-uid"))
	pool.SetResourceVersion("1")
	pool.Object["spec"] = map[string]any{"nodeName": node, "mountPath": "/" + node, "poolGroup": "default", "capacity": map[string]any{"limit": limit}}
	return pool
}

// commitDynamicUpdate also lets a test deliver a captured PUT after its caller
// timed out. It performs no fake-client API calls while holding the lock.
func (f *preparationRegistryFixture) commitDynamicUpdate(resource schema.GroupVersionResource, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, err := f.dynamic.Tracker().Get(resource, object.GetNamespace(), object.GetName())
	if err != nil {
		return nil, err
	}
	current := stored.(*unstructured.Unstructured)
	if object.GetUID() != current.GetUID() || object.GetResourceVersion() != current.GetResourceVersion() {
		return nil, apierrors.NewConflict(resource.GroupResource(), object.GetName(), fmt.Errorf("UID/resourceVersion changed"))
	}
	next := object.DeepCopy()
	version, err := strconv.ParseInt(current.GetResourceVersion(), 10, 64)
	if err != nil {
		return nil, err
	}
	next.SetResourceVersion(strconv.FormatInt(version+1, 10))
	if err = f.dynamic.Tracker().Update(resource, next, next.GetNamespace()); err != nil {
		return nil, err
	}
	return next, nil
}

func (f *preparationRegistryFixture) move(t *testing.T, index int) volumeapi.Move {
	t.Helper()
	move, err := f.registry.GetMove(f.ctx, f.moveNames[index])
	if err != nil {
		t.Fatal(err)
	}
	return move
}
func (f *preparationRegistryFixture) state(t *testing.T, index int) volumeapi.State {
	t.Helper()
	state, err := f.registry.Get(f.ctx, f.volumeIDs[index])
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func (f *preparationRegistryFixture) schedulePlacements(t *testing.T) {
	t.Helper()
	pods, err := f.client.CoreV1().Pods("system").List(f.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels["shiftpv.io/role"] != placementRole || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil {
			continue
		}
		pod.Spec.NodeName = "destination"
		if _, err = f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}
func (f *preparationRegistryFixture) untilPrepared(t *testing.T, index int) volumeapi.Move {
	t.Helper()
	for range 20 {
		move := f.move(t, index)
		if move.Status.Preparation != nil {
			return move
		}
		if err := f.reconciler.reconcilePreparation(f.ctx, move); err != nil {
			t.Fatal(err)
		}
		f.schedulePlacements(t)
	}
	t.Fatalf("preparation never acquired: %+v", f.move(t, index))
	return volumeapi.Move{}
}

func (f *preparationRegistryFixture) refreshPools(t *testing.T) {
	t.Helper()
	for _, name := range []string{"source-pool", "destination-pool"} {
		object, err := f.dynamic.Resource(volumeapi.PoolResource).Get(f.ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedField(object.Object, time.Now().UTC().Format(time.RFC3339Nano), "status", "lastProbeTime")
		if _, err = f.dynamic.Resource(volumeapi.PoolResource).UpdateStatus(f.ctx, object, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}
