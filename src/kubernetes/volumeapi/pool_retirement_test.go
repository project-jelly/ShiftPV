package volumeapi

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
)

func TestPoolRetirementApprovalUsesCurrentUIDRequestAndProtection(t *testing.T) {
	for _, test := range []struct {
		name, request, approval, uid string
		approved, protected, fail    bool
	}{
		{"approve", "pool-uid", "", "pool-uid", true, true, false},
		{"request cancelled", "", "", "pool-uid", true, true, true},
		{"wrong request", "old-uid", "", "pool-uid", true, true, true},
		{"replaced Pool", "pool-uid", "", "replacement-uid", true, true, true},
		{"unapproved", "pool-uid", "", "pool-uid", false, true, true},
		{"unprotected", "pool-uid", "", "pool-uid", true, false, true},
		{"retry after request removed", "", "pool-uid", "pool-uid", true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
				"spec":   map[string]any{"nodeName": "node", "mountPath": "/mnt/original", "capacity": map[string]any{"limit": "1Gi"}},
				"status": map[string]any{"registrationApproved": test.approved},
			}}
			pool.SetName("pool")
			pool.SetUID(types.UID(test.uid))
			pool.SetAnnotations(map[string]string{PoolRetirementRequestAnnotation: test.request, PoolRetirementApprovalAnnotation: test.approval})
			if test.protected {
				pool.SetFinalizers([]string{PoolProtectionFinalizer})
			}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), pool)
			registry := &Registry{Client: client}
			err := registry.ApprovePoolRetirement(context.Background(), "pool", "pool-uid")
			if (err != nil) != test.fail || test.fail && !errors.Is(err, ErrStateConflict) {
				t.Fatalf("approval error=%v fail=%v", err, test.fail)
			}
			stored, err := client.Resource(PoolResource).Get(context.Background(), "pool", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !test.fail && stored.GetAnnotations()[PoolRetirementApprovalAnnotation] != "pool-uid" {
				t.Fatal("retirement was not durably recorded")
			}
			if stored.GetAnnotations()[PoolIdentityReleaseAnnotation] != "" || len(stored.GetFinalizers()) != len(pool.GetFinalizers()) {
				t.Fatal("retirement changed physical-release authority or protection")
			}
			if !test.fail {
				client.ClearActions()
				if err := registry.ApprovePoolRetirement(context.Background(), "pool", "pool-uid"); err != nil {
					t.Fatal(err)
				}
				for _, action := range client.Actions() {
					if action.GetVerb() != "get" {
						t.Fatalf("idempotent approval performed %s", action.GetVerb())
					}
				}
			}
		})
	}
}

func TestPoolRetirementImmediatelyFencesNewReadiness(t *testing.T) {
	now := time.Now()
	pool := Pool{UID: "pool-uid", Generation: 1, Status: PoolStatus{ObservedGeneration: 1, LastProbeTime: metav1.NewTime(now),
		Conditions: []metav1.Condition{{Type: PoolConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	if ok, _ := pool.ReadyAt(now, time.Minute); !ok {
		t.Fatal("fixture was not initially ready")
	}
	pool.RetirementRequest = pool.UID
	if ok, reason := pool.ReadyAt(now, time.Minute); ok || reason != "PoolRetirementPending" {
		t.Fatalf("pending request accepted new allocation: %v %s", ok, reason)
	}
	pool.RetirementApproval, pool.RetirementRequest = pool.UID, ""
	if ok, reason := pool.ReadyAt(now, time.Minute); ok || reason != "PoolRegistrationRetired" {
		t.Fatalf("request removal reactivated retired registration: %v %s", ok, reason)
	}
	pool.RetirementApproval = "old-uid"
	if ok, _ := pool.ReadyAt(now, time.Minute); !ok {
		t.Fatal("a stale UID approval retired a different registration")
	}
}

func TestRetiredRecordDoesNotAmbiguateLegacyNodeLookup(t *testing.T) {
	makePool := func(name, uid string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
			"metadata": map[string]any{"name": name, "uid": uid},
			"spec":     map[string]any{"nodeName": "node", "mountPath": "/mnt/" + name},
		}}
	}
	retired, active := makePool("retired", "old-uid"), makePool("active", "new-uid")
	retired.SetAnnotations(map[string]string{PoolRetirementApprovalAnnotation: "old-uid"})
	client := fake.NewSimpleDynamicClient(runtime.NewScheme(), retired, active)
	r := &Registry{Client: client}
	for _, lookup := range []func(context.Context, string) (Pool, error){r.PoolForNode, r.PoolForNodeLifecycle} {
		pool, err := lookup(context.Background(), "node")
		if err != nil || pool.UID != "new-uid" {
			t.Fatalf("retained record blocked the active registration: pool=%+v error=%v", pool, err)
		}
	}
	when := metav1.Now()
	retired.SetDeletionTimestamp(&when)
	_, err := client.Resource(PoolResource).Update(context.Background(), retired, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := r.PoolForNodeLifecycle(context.Background(), "node")
	if err != nil || pool.UID != "old-uid" {
		t.Fatalf("normal physical deletion lost its exact target: pool=%+v error=%v", pool, err)
	}
}
