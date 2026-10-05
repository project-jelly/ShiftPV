package volumeapi

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

func enableFixedCapacity(t *testing.T, object *unstructured.Unstructured, device string, start uint64) {
	t.Helper()
	unit := &PoolCapacityUnit{Device: device, Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: start, Sectors: 1024}}}
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(unit)
	if err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedMap(object.Object, data, "status", "capacityUnit"); err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(object.Object, PoolCapacityPolicyFixedBlock, "spec", "capacityPolicy")
	_ = unstructured.SetNestedField(object.Object, "/mnt/"+object.GetName(), "spec", "mountPath")
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	conditions = append(conditions, map[string]any{"type": PoolConditionCapacityIndependent, "status": "True", "reason": "CapacityAllocationVerified", "message": "verified", "observedGeneration": int64(1), "lastTransitionTime": "2026-09-07T00:00:00Z"})
	_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
}
func TestMultipleFixedPoolsRequireNonoverlappingAllocationEvidence(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent", true: "shared"}[shared], func(t *testing.T) {
			a, b := pool("a", "node"), pool("b", "node")
			a.SetUID("a-uid")
			b.SetUID("b-uid")
			enableFixedCapacity(t, a, "253:0", 0)
			offset := uint64(1024)
			if shared {
				offset = 512
			}
			enableFixedCapacity(t, b, "253:1", offset)
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, a, b)
			registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
			pools, err := registry.ReadyPools(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := 2
			if shared {
				want = 0
			}
			if len(pools) != want {
				t.Fatalf("Ready Pools=%d want=%d", len(pools), want)
			}
			_, err = registry.ReadyPoolForIdentity(context.Background(), "a", "a-uid", "node")
			if (err != nil) != shared {
				t.Fatalf("exact Pool independence gate err=%v", err)
			}
		})
	}
}
func TestFixedPoolMissingEvidenceIsNotReady(t *testing.T) {
	object := pool("a", "node")
	enableFixedCapacity(t, object, "8:1", 0)
	parsed, err := poolFrom(object)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Status.CapacityUnit = nil
	if ready, _ := parsed.ReadyAt(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC), time.Minute); ready {
		t.Fatal("fixed Pool without anchored evidence is Ready")
	}
	parsed.Status.CapacityUnit = &PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Sectors: 1024}}}
	parsed.Status.Conditions[1].Status = metav1.ConditionFalse
	if ready, _ := parsed.ReadyAt(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC), time.Minute); ready {
		t.Fatal("unverified fixed allocation is Ready")
	}
}

func TestFixedPoolStatusCannotReplaceOrClearCapacityAnchor(t *testing.T) {
	object := pool("a", "node")
	object.SetUID("a-uid")
	enableFixedCapacity(t, object, "8:1", 0)
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	registry := &Registry{Client: client}
	parsed, err := poolFrom(object)
	if err != nil {
		t.Fatal(err)
	}
	original := parsed.Status
	changed := original
	unit := *original.CapacityUnit
	unit.Device = "8:2"
	changed.CapacityUnit = &unit
	for _, status := range []PoolStatus{changed, {}} {
		if err := registry.SetPoolStatus(context.Background(), "a", "a-uid", "node", status); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("capacity identity replacement accepted: %v", err)
		}
	}
	if err := registry.SetPoolStatus(context.Background(), "a", "a-uid", "node", original); err != nil {
		t.Fatal(err)
	}
}

func TestNestedPoolPathsAreUnusableEvenWithDisjointExtents(t *testing.T) {
	a, b := pool("a", "node"), pool("b", "node")
	a.SetUID("a-uid")
	b.SetUID("b-uid")
	enableFixedCapacity(t, a, "8:1", 0)
	enableFixedCapacity(t, b, "8:2", 1024)
	_ = unstructured.SetNestedField(b.Object, "/mnt/a/nested", "spec", "mountPath")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, a, b)
	registry := &Registry{Client: client, Now: func() time.Time { return time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC) }}
	ready, err := registry.ReadyPools(context.Background())
	if err != nil || len(ready) != 0 {
		t.Fatalf("nested roots accepted: %v %v", ready, err)
	}
	if _, err := registry.ReadyPoolForIdentity(context.Background(), "a", "a-uid", "node"); !errors.Is(err, ErrPoolNotReady) {
		t.Fatalf("nested exact root accepted: %v", err)
	}
}

func TestPeerCapacityProbeMustBeCurrentForPlacement(t *testing.T) {
	a, b := pool("a", "node"), pool("b", "node")
	a.SetUID("a-uid")
	b.SetUID("b-uid")
	enableFixedCapacity(t, a, "8:1", 0)
	enableFixedCapacity(t, b, "8:2", 1024)
	left, err := poolFrom(a)
	if err != nil {
		t.Fatal(err)
	}
	right, err := poolFrom(b)
	if err != nil {
		t.Fatal(err)
	}
	right.Status.Conditions[1].Status = metav1.ConditionFalse
	right.Status.Conditions[1].Reason = "CapacityIdentityChanged"
	if ready, reason := PoolCapacityIndependent(left, []Pool{left, right}); ready || reason != "CapacityPeerUnproven" {
		t.Fatalf("stale peer anchor accepted: %v %s", ready, reason)
	}
}
