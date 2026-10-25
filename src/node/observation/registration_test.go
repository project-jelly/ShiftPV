//go:build linux || darwin

package observation

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/lifecycle/poolcontroller"
	"github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

type rejectedAllocation struct{}

func (rejectedAllocation) InspectCapacityUnit(volumeapi.Pool) (*volumeapi.PoolCapacityUnit, readiness.Check) {
	return nil, readiness.Check{Known: true, Reason: "CapacityAllocationUnproven", Message: "virtual block allocation is unsupported"}
}

func (rejectedAllocation) Inspect(volumeapi.Pool) readiness.Result {
	panic("rejected allocation must not reach the write probe")
}

type activeInstallation struct{}

func (activeInstallation) Quiescing(context.Context) (string, bool, error) { return "", false, nil }

// Keep the real registry, scanner, readiness and deletion checker in this test:
// returning a made-up empty inventory would hide the failed FixedBlock gate.
func TestRejectedFixedBlockPoolDeletionConvergesWithoutReleasingStorage(t *testing.T) {
	ctx := context.Background()
	host, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unmanaged"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	deleted := metav1.NewTime(time.Now().UTC().Add(-time.Second))
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "rejected", "uid": "pool-uid", "generation": int64(1),
			"deletionTimestamp": deleted.Format(time.RFC3339Nano), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec": map[string]any{"nodeName": "node", "poolGroup": "default", "mountPath": "/pool", "mountPolicy": volumeapi.PoolMountPolicyRequireMountPoint,
			"capacityPolicy": volumeapi.PoolCapacityPolicyFixedBlock, "capacity": map[string]any{"limit": "1Gi"}},
	}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
	}, object)
	registry := &volumeapi.Registry{Client: api}
	scanner := &Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 8}
	node, err := readiness.NewReconciler("node", registry, rejectedAllocation{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory, node.Release = scanner.Scan, scanner.ReleasePool
	checker := &uninstall.Checker{Client: fake.NewClientset(), Volumes: registry, Cleanups: &cleanupapi.Store{Client: api}}
	controller := &poolcontroller.Reconciler{Pools: registry, Safety: checker, Quiesce: activeInstallation{}, PoolLocks: &capacity.Locker{}, Interval: time.Second}
	read := func() volumeapi.Pool {
		t.Helper()
		pools, err := registry.ListPoolRegistrations(ctx)
		if err != nil || len(pools) != 1 {
			t.Fatalf("pools=%v error=%v", pools, err)
		}
		return pools[0]
	}
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	pool := read()
	if pool.RegistrationApproved() || pool.Status.Inventory == nil || !pool.Status.Inventory.Valid {
		t.Fatalf("rejected registration cannot produce cancellation evidence: %+v", pool.Status)
	}
	if ready, _ := pool.ReadyAt(time.Now(), time.Minute); ready {
		t.Fatal("deleting candidate became Ready")
	}
	if released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased); released == nil || released.Status != metav1.ConditionFalse {
		t.Fatal("registration canceled without controller approval")
	}
	// Read-only empty-path evidence cannot bypass outstanding API obligations.
	volumeObject := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": "shiftpv-0123456789abcdef0123456789abcdef", "uid": "volume-uid"},
		"spec":     map[string]any{"volumeID": "shiftpv-0123456789abcdef0123456789abcdef", "requestName": "pvc", "initialNode": "node", "capacityBytes": int64(1)},
		"status":   map[string]any{"phase": volumeapi.PhasePending, "ownerNode": "node"},
	}}
	if _, err := api.Resource(volumeapi.VolumeResource).Create(ctx, volumeObject, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if read().IdentityReleaseApproval != "" {
		t.Fatal("outstanding volume hold was ignored")
	}
	if err := api.Resource(volumeapi.VolumeResource).Delete(ctx, volumeObject.GetName(), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	pool = read()
	if pool.IdentityReleaseApproval != pool.UID || !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) {
		t.Fatalf("approval bypassed node confirmation: %+v", pool)
	}
	// Approval is not enough if local evidence changes before confirmation.
	if err := os.Mkdir(filepath.Join(root, ".shiftpv"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(read().Finalizers, volumeapi.PoolProtectionFinalizer) {
		t.Fatal("approval bypassed changed local evidence")
	}
	if err := os.Remove(filepath.Join(root, ".shiftpv")); err != nil {
		t.Fatal(err)
	}
	// A process restart must resume using only durable API and filesystem state.
	node, err = readiness.NewReconciler("node", registry, rejectedAllocation{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory, node.Release = scanner.Scan, scanner.ReleasePool
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	pool = read()
	released := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionIdentityReleased)
	if released == nil || released.Status != metav1.ConditionTrue || released.Reason != "PoolRegistrationCanceled" {
		t.Fatalf("cancellation evidence missing: %+v", released)
	}
	if slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) || pool.RegistrationApproved() {
		t.Fatalf("cancellation did not converge: %+v", pool)
	}
	if data, err := os.ReadFile(filepath.Join(root, "unmanaged")); err != nil || string(data) != "preserve" {
		t.Fatalf("cancellation touched unmanaged storage: %q %v", data, err)
	}
	if _, err := os.Lstat(filepath.Join(root, ".shiftpv")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancellation initialized storage identity: %v", err)
	}
}

func TestRejectedFixedBlockCancellationRejectsLocalEvidence(t *testing.T) {
	for _, scenario := range []string{"metadata", "volume", "symlink", "published", "publication error", "installation error", "canceled", "approved", "capacity anchor", "mount anchor", "legacy approval"} {
		t.Run(scenario, func(t *testing.T) {
			host, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(host, "pool")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			deleted := metav1.NewTime(time.Now().Add(-time.Second))
			pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/pool", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock,
				Generation: 1, DeletionTimestamp: &deleted, IdentityReleaseApproval: "pool-uid"}
			scanner := &Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 8}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "metadata":
				store, err := ownership.Open(root, ownership.PoolIdentity{InstallationID: "installation", PoolUID: pool.UID})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			case "volume":
				if err := os.MkdirAll(filepath.Join(root, "volumes", "unknown"), 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(root, filepath.Join(host, "alias")); err != nil {
					t.Fatal(err)
				}
				pool.MountPath = "/alias"
			case "published":
				scanner.Publications = publications{published: true}
			case "publication error":
				scanner.Publications = publications{err: errors.New("mount table unavailable")}
			case "installation error":
				scanner.Installation = installation{err: errors.New("API unavailable")}
			case "canceled":
				cancel()
			case "approved":
				pool.Status.RegistrationApproved = true
			case "capacity anchor":
				pool.Status.CapacityUnit = &volumeapi.PoolCapacityUnit{}
			case "mount anchor":
				pool.Status.MountIdentity = &volumeapi.PoolMountIdentity{}
			case "legacy approval":
				pool.Status.Conditions = []metav1.Condition{{Type: volumeapi.PoolConditionReady, Status: metav1.ConditionTrue}}
				pool.Status.Inventory = &volumeapi.PoolInventory{Valid: true}
			}
			inventory := scanner.Scan(ctx, pool, time.Now())
			if inventory.Valid {
				t.Fatalf("ambiguous candidate accepted: %+v", inventory)
			}
			if err := scanner.ReleasePool(ctx, pool); err == nil {
				t.Fatal("ambiguous candidate release accepted")
			}
		})
	}
}

func TestRejectedFixedBlockCancellationAfterTestPathRemoval(t *testing.T) {
	for _, scenario := range []string{"missing directory", "missing ancestors", "symlink ancestor", "approved"} {
		t.Run(scenario, func(t *testing.T) {
			host, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			deleted := metav1.NewTime(time.Now())
			pool := volumeapi.Pool{UID: "pool-uid", NodeName: "node", MountPath: "/missing", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock,
				DeletionTimestamp: &deleted, MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint}
			if scenario == "missing ancestors" {
				pool.MountPath = "/missing/parent/pool"
			}
			if scenario == "symlink ancestor" {
				if err := os.Symlink(host, filepath.Join(host, "alias")); err != nil {
					t.Fatal(err)
				}
				pool.MountPath = "/alias/missing"
			}
			if scenario == "approved" {
				pool.Status.RegistrationApproved = true
			}
			scanner := &Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 8}
			wantValid := scenario != "symlink ancestor" && scenario != "approved"
			inventory := scanner.Scan(context.Background(), pool, time.Now())
			if inventory.Valid != wantValid {
				t.Fatalf("inventory=%+v wantValid=%t", inventory, wantValid)
			}
			if err := scanner.ReleasePool(context.Background(), pool); (err == nil) != wantValid {
				t.Fatalf("release error=%v wantValid=%t", err, wantValid)
			}
			entries, err := os.ReadDir(host)
			if err != nil {
				t.Fatal(err)
			}
			wantEntries := 0
			if scenario == "symlink ancestor" {
				wantEntries = 1
			}
			if len(entries) != wantEntries {
				t.Fatal("cancellation recreated the missing test path")
			}
		})
	}
}

func TestApprovedPoolReleaseRecoversAfterCanceledStatusWrite(t *testing.T) {
	ctx := context.Background()
	host, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := ownership.Open(root, ownership.PoolIdentity{InstallationID: "installation", PoolUID: "pool-uid"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	deleted := metav1.NewTime(time.Now().Add(-time.Second))
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool",
		"metadata": map[string]any{"name": "pool", "uid": "pool-uid", "generation": int64(1),
			"deletionTimestamp": deleted.Format(time.RFC3339Nano), "finalizers": []any{volumeapi.PoolProtectionFinalizer}},
		"spec":   map[string]any{"nodeName": "node", "poolGroup": "default", "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}},
		"status": map[string]any{"registrationApproved": true},
	}}
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.PoolResource: "ShiftPVPoolList", volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList",
	}, object)
	registry := &volumeapi.Registry{Client: api}
	scanner := &Scanner{HostRoot: host, TargetRoot: "/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 8}
	node, err := readiness.NewReconciler("node", registry, readiness.NewProbe(host), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory, node.Release = scanner.Scan, scanner.ReleasePool
	checker := &uninstall.Checker{Client: fake.NewClientset(), Volumes: registry, Cleanups: &cleanupapi.Store{Client: api}}
	controller := &poolcontroller.Reconciler{Pools: registry, Safety: checker, Quiesce: activeInstallation{}, PoolLocks: &capacity.Locker{}, Interval: time.Second}
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	// The filesystem effect succeeds, then cancellation prevents publishing its
	// confirmation. Recovery must use durable state, never the old callback.
	releaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	node.Release = func(ctx context.Context, pool volumeapi.Pool) error {
		if err := scanner.ReleasePool(ctx, pool); err != nil {
			return err
		}
		cancel()
		return nil
	}
	if err := node.Reconcile(releaseCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost confirmation=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, ".shiftpv", "pool.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("identity effect did not finish: %v", err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	pools, err := registry.ListPoolRegistrations(ctx)
	if err != nil || len(pools) != 1 || !slices.Contains(pools[0].Finalizers, volumeapi.PoolProtectionFinalizer) {
		t.Fatalf("lost confirmation released protection: pools=%+v error=%v", pools, err)
	}
	node, err = readiness.NewReconciler("node", registry, readiness.NewProbe(host), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	node.Inventory, node.Release = scanner.Scan, scanner.ReleasePool
	if err := node.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	pools, err = registry.ListPoolRegistrations(ctx)
	if err != nil || len(pools) != 1 || slices.Contains(pools[0].Finalizers, volumeapi.PoolProtectionFinalizer) {
		t.Fatalf("release replay did not converge: pools=%+v error=%v", pools, err)
	}
}
