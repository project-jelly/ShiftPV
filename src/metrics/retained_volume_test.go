package metrics

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

const retainedVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"

func retainedVolumeFixture(t *testing.T) (*Controller, *dynamicfake.FakeDynamicClient, *fake.Clientset) {
	t.Helper()
	copy := metricCopy("pool-a", "pool-a-uid", "a")
	copy.VolumeID = retainedVolumeID
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(copy)
	if err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": retainedVolumeID, "uid": copy.VolumeUID, "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": retainedVolumeID, "capacityBytes": int64(1 << 30)},
		"status":   map[string]any{"phase": volumeapi.PhaseReady, "ownerNode": copy.NodeName, "currentCopy": encoded},
	}}
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": copy.PoolName, "uid": copy.PoolUID},
		"spec":     map[string]any{"nodeName": copy.NodeName, "mountPath": "/pool", "capacity": map[string]any{"limit": "10Gi"}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.MoveResource: "ShiftPVMoveList",
	}, object, pool)
	pv := driverPersistentVolume("retained-pv", retainedVolumeID, "Released", "1Gi")
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	client := fake.NewClientset(&pv)
	return &Controller{
		Exporter: New("metadata"), Inventory: &volumeapi.Registry{Client: dynamicClient},
		Cleanups: &cleanupapi.Store{Client: dynamicClient}, PersistentVolumes: persistentVolumeStore{Client: client},
	}, dynamicClient, client
}

func TestRetainedVolumeWithoutPersistentVolumeIsObservedAndRecovers(t *testing.T) {
	ctx := context.Background()
	c, dynamicClient, client := retainedVolumeFixture(t)
	before, err := c.Inventory.ListVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check := func(count, bytes string) {
		t.Helper()
		if err := c.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		contains(t, output(t, c.Exporter),
			`shiftpv_volumes_without_persistent_volume{pool="pool-a"} `+count,
			`shiftpv_volumes_without_persistent_volume_reserved_bytes{pool="pool-a"} `+bytes,
			`shiftpv_pool_reserved_bytes{node="a",pool="pool-a"} 1.073741824e+09`)
	}
	check("0", "0")
	if err := client.CoreV1().PersistentVolumes().Delete(ctx, "retained-pv", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	check("1", "1.073741824e+09")
	// A static PV restoring the same handle explains the retained owner again.
	pv := driverPersistentVolume("restored-pv", retainedVolumeID, "Available", "1Gi")
	if _, err := client.CoreV1().PersistentVolumes().Create(ctx, &pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	check("0", "0")
	after, err := c.Inventory.ListVolumes(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("observation changed durable storage state: %v", err)
	}
	for _, action := range dynamicClient.Actions() {
		if action.GetVerb() != "list" {
			t.Fatalf("metrics issued a storage mutation: %s", action.GetVerb())
		}
	}
}

func TestUnreferencedVolumeClassificationExcludesLifecycleWork(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*unstructured.Unstructured)
		want   int
	}{
		{"pending creation", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, volumeapi.PhasePending, "status", "phase")
		}, 0},
		{"node creating", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, volumeapi.PhaseNodeCreating, "status", "phase")
		}, 0},
		// CompleteCreate precedes PV publication. This is absence evidence, not
		// proof that a PV was deleted; the alert must allow a publication grace.
		{"ready before PV publication", func(*unstructured.Unstructured) {}, 1},
		{"preserved blocked owner", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, volumeapi.PhaseBlocked, "status", "phase")
		}, 1},
		{"deleting phase", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, volumeapi.PhaseDeleting, "status", "phase")
		}, 0},
		{"deletion timestamp", func(v *unstructured.Unstructured) { now := metav1.Now(); v.SetDeletionTimestamp(&now) }, 0},
		{"ready active move", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(v.Object, "move", "status", "activeMove")
		}, 0},
		{"cleanup intent", func(v *unstructured.Unstructured) {
			_ = unstructured.SetNestedMap(v.Object, map[string]any{}, "status", "cleanup")
		}, 0},
		{"old copy identity", func(v *unstructured.Unstructured) { v.SetUID("replaced-volume-uid") }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, dynamicClient, client := retainedVolumeFixture(t)
			object, err := dynamicClient.Resource(volumeapi.VolumeResource).Get(ctx, retainedVolumeID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tc.change(object)
			if _, err := dynamicClient.Resource(volumeapi.VolumeResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := client.CoreV1().PersistentVolumes().Delete(ctx, "retained-pv", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			// Test classification through the real State decoder without asking
			// deliberately incomplete Move/cleanup fixtures to pass other metrics.
			volumes, err := c.Inventory.ListVolumes(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pools, err := c.Inventory.ListPools(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c.Exporter.Cache.update("metadata", retainedVolumeSamples(pools, nil, volumes), true)
			contains(t, output(t, c.Exporter), fmt.Sprintf(`shiftpv_volumes_without_persistent_volume{pool="pool-a"} %d`, tc.want))
		})
	}
}

func TestPersistentVolumeListFailureDoesNotInferAbsence(t *testing.T) {
	ctx := context.Background()
	c, _, client := retainedVolumeFixture(t)
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("PV list unavailable")
	})
	if err := c.Refresh(ctx); err == nil {
		t.Fatal("failed list reported success")
	}
	contains(t, output(t, c.Exporter), `shiftpv_metrics_snapshot_success{source="metadata"} 0`,
		`shiftpv_volumes_without_persistent_volume{pool="pool-a"} 0`)
}

func TestMissingPersistentVolumeInventoryDoesNotInferAbsence(t *testing.T) {
	c, _, _ := retainedVolumeFixture(t)
	c.PersistentVolumes = nil
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	families, err := c.Exporter.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "shiftpv_volumes_without_persistent_volume" {
			t.Fatal("missing PV inventory was treated as a successful empty list")
		}
	}
}

func TestUnreferencedVolumeIgnoresForeignDriverAndBoundsPoolLabels(t *testing.T) {
	c, _, _ := retainedVolumeFixture(t)
	volumes, err := c.Inventory.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pv := driverPersistentVolume("foreign", retainedVolumeID, "Bound", "1Gi")
	pv.Spec.CSI.Driver = "another.csi.example"
	c.Exporter.Cache.update("metadata", retainedVolumeSamples(nil, []corev1.PersistentVolume{pv}, volumes), true)
	contains(t, output(t, c.Exporter), `shiftpv_volumes_without_persistent_volume{pool="unknown"} 1`)
}
