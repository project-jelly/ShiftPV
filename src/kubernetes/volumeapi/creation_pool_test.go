package volumeapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestCreationPoolSeparatesInventoryFromBackingReadiness(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		change  func(*unstructured.Unstructured)
		allowed bool
	}{
		{"invalid inventory", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, false, "status", "inventory", "valid")
		}, true},
		{"missing inventory", func(p *unstructured.Unstructured) { unstructured.RemoveNestedField(p.Object, "status", "inventory") }, true},
		{"truncated inventory", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, true, "status", "inventory", "truncated")
		}, true},
		{"stale inventory", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, now.Add(-time.Hour).Format(time.RFC3339), "status", "inventory", "observedAt")
		}, true},
		{"changed Pool UID", func(p *unstructured.Unstructured) { p.SetUID("replacement") }, false},
		{"changed node", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, "other", "spec", "nodeName")
		}, false},
		{"missing protection", func(p *unstructured.Unstructured) { p.SetFinalizers(nil) }, false},
		{"deleting Pool", func(p *unstructured.Unstructured) { stamp := metav1.NewTime(now); p.SetDeletionTimestamp(&stamp) }, false},
		{"outdated generation", func(p *unstructured.Unstructured) { p.SetGeneration(2) }, false},
		{"stale backing", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, now.Add(-time.Hour).Format(time.RFC3339), "status", "lastProbeTime")
		}, false},
		{"future backing", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, now.Add(time.Hour).Format(time.RFC3339), "status", "lastProbeTime")
		}, false},
		{"failed backing", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(p.Object, []any{map[string]any{"type": PoolConditionReady, "status": "False", "observedGeneration": int64(1), "reason": "PathMissing"}}, "status", "conditions")
		}, false},
		{"missing mount identity", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, PoolMountPolicyRequireMountPoint, "spec", "mountPolicy")
		}, false},
		{"missing capacity identity", func(p *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(p.Object, PoolCapacityPolicyFixedBlock, "spec", "capacityPolicy")
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := pool("pool", "node")
			object.SetUID("pool-uid")
			tc.change(object)
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"}, object)
			registry := &Registry{Client: client, Now: func() time.Time { return now }}
			copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", NodeName: "node", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "initial-volume-uid", Role: volume.RoleServing}
			_, err := registry.CreationPoolForIdentity(context.Background(), copy)
			if tc.allowed && err != nil || !tc.allowed && err == nil {
				t.Fatalf("creation backing allowed=%v err=%v", tc.allowed, err)
			}
			if tc.allowed {
				if _, err := registry.ReadyPoolForIdentity(context.Background(), copy.PoolName, copy.PoolUID, copy.NodeName); !errors.Is(err, ErrPoolNotReady) {
					t.Fatalf("incomplete inventory admitted new placement: %v", err)
				}
			}
		})
	}
}
