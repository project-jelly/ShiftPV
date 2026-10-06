package volumeapi

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func probePoolFixture() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool", "uid": "uid", "resourceVersion": "1", "generation": int64(2), "annotations": map[string]any{"other": "preserved"}},
		"spec":     map[string]any{"nodeName": "node", "mountPath": "/mnt/pool", "scanEpoch": int64(1)},
		"status":   map[string]any{"capacityProbeSupported": true, "observedGeneration": int64(2)},
	}}
}

func TestCapacityProbePreservesStatusAndInventoryFenceAcrossConflicts(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleDynamicClient(runtime.NewScheme(), probePoolFixture())
	registry := &Registry{Client: client}
	pool, err := registry.PoolForIdentity(ctx, "pool", "uid", "node")
	if err != nil {
		t.Fatal(err)
	}
	conflicts := 0
	client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if conflicts == 0 {
			conflicts++
			return true, nil, apierrors.NewConflict(PoolResource.GroupResource(), "pool", errors.New("concurrent readiness"))
		}
		return false, nil, nil
	})
	if err := registry.RequestPoolCapacityProbe(ctx, pool, "request"); err != nil {
		t.Fatal(err)
	}
	pool.CapacityProbeRequest = "request"
	result := PoolCapacityProbeResult{RequestID: strings.Repeat("a", 32), Evidence: strings.Repeat("b", 64), TotalBytes: 100, AvailableBytes: 50, ObservedAt: metav1.Now()}
	if err := registry.RecordPoolCapacityProbe(ctx, pool, result); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetPoolStatus(ctx, pool.Name, pool.UID, pool.NodeName, pool.Status); err != nil {
		t.Fatal(err)
	}
	stored, err := registry.PoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName)
	if err != nil || stored.Status.CapacityProbe == nil || stored.Status.CapacityProbe.RequestID != result.RequestID {
		t.Fatalf("readiness clobbered live result: status=%+v err=%v", stored.Status, err)
	}
	object, _ := client.Resource(PoolResource).Get(ctx, pool.Name, metav1.GetOptions{})
	epoch, _, _ := unstructured.NestedInt64(object.Object, "spec", "scanEpoch")
	if epoch != 1 || object.GetGeneration() != 2 || object.GetAnnotations()["other"] != "preserved" {
		t.Fatalf("read request changed inventory fence or annotations: %v", object.Object)
	}
}

func TestCapacityProbeCannotWriteAcrossPoolIdentityChanges(t *testing.T) {
	for _, mode := range []string{"uid", "node", "generation", "mount", "deleting", "request", "empty", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			object := probePoolFixture()
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			registry := &Registry{Client: client}
			pool, err := registry.PoolForIdentity(ctx, "pool", "uid", "node")
			if err != nil {
				t.Fatal(err)
			}
			request := "request"
			switch mode {
			case "uid":
				pool.UID = "old-uid"
			case "node":
				pool.NodeName = "other-node"
			case "generation":
				pool.Generation--
			case "mount":
				pool.Status.MountIdentity = &PoolMountIdentity{Device: "8:1"}
			case "deleting":
				stamp := metav1.Now()
				object.SetDeletionTimestamp(&stamp)
				if err := client.Tracker().Update(PoolResource, object, ""); err != nil {
					t.Fatal(err)
				}
			case "empty":
				request = ""
			case "oversized":
				request = strings.Repeat("x", 513)
			}
			if mode == "request" {
				pool.CapacityProbeRequest = "obsolete-request"
				if err := registry.RecordPoolCapacityProbe(ctx, pool, PoolCapacityProbeResult{}); !errors.Is(err, ErrStateConflict) {
					t.Fatalf("obsolete response was written: %v", err)
				}
			} else if err := registry.RequestPoolCapacityProbe(ctx, pool, request); err == nil {
				t.Fatal("ambiguous request was persisted")
			}
		})
	}
}
