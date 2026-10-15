//go:build linux

package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	mountutils "k8s.io/mount-utils"
)

type peerBackingFixture struct {
	node       *Node
	api        *dynamicfake.FakeDynamicClient
	state      volumeapi.State
	cleanup    cleanupapi.Cleanup
	probe      *readiness.Probe
	extents    map[string][]capacityunit.Extent
	peerFailed bool
	checks     int
}

func (f *peerBackingFixture) Resolve(device string) ([]capacityunit.Extent, error) {
	if device == "8:2" && f.peerFailed {
		return nil, fmt.Errorf("peer block allocation unavailable")
	}
	return f.extents[device], nil
}

// Registry and native filesystem effects are real. Only mount/sysfs evidence is
// injected, so this fixture does not qualify a real device or mount failure.
func newPeerBackingFixture(t *testing.T, operation string) *peerBackingFixture {
	t.Helper()
	_, node, api, _, state := fixture(t)
	host, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node.HostRoot = host
	f := &peerBackingFixture{node: node, api: api, state: state, extents: map[string][]capacityunit.Extent{
		"8:1": {{Device: "8:0", Start: 100, Sectors: 100}},
		"8:2": {{Device: "8:0", Start: 300, Sectors: 100}},
	}}
	for _, path := range []string{"mnt/pool", "mnt/peer"} {
		if err := os.MkdirAll(filepath.Join(host, path), 0700); err != nil {
			t.Fatal(err)
		}
	}
	f.probe, err = readiness.NewProbeWithDependencies(host, readiness.ProbeDependencies{
		Allocations: f,
		InspectDirectory: func(path string) error {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return fmt.Errorf("Pool root is not a directory")
			}
			return nil
		},
		Write: func(string) error { t.Error("approved effect ran a registration write probe"); return nil },
		StatFS: func(string) (capacity.Filesystem, error) {
			t.Error("approved effect measured capacity for new admission")
			return capacity.Filesystem{}, nil
		},
		ListMounts: func() ([]mountutils.MountInfo, error) {
			return []mountutils.MountInfo{
				{MountPoint: filepath.Join(host, "mnt/pool"), Major: 8, Minor: 1, Source: "/dev/a", FsType: "ext4", Root: "/"},
				{MountPoint: filepath.Join(host, "mnt/peer"), Major: 8, Minor: 2, Source: "/dev/b", FsType: "ext4", Root: "/"},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range []volumeapi.Pool{
		{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/pool", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock},
		{Name: "pool-b", UID: "peer-uid", NodeName: "worker-a", MountPath: "/mnt/peer", CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock},
	} {
		unit, check := f.probe.InspectCapacityUnit(pool)
		if !check.OK {
			t.Fatal(check)
		}
		object := base.DeepCopy()
		object.SetName(pool.Name)
		object.SetUID(types.UID(pool.UID))
		_ = unstructured.SetNestedField(object.Object, pool.MountPath, "spec", "mountPath")
		_ = unstructured.SetNestedField(object.Object, pool.CapacityPolicy, "spec", "capacityPolicy")
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(unit)
		if err != nil {
			t.Fatal(err)
		}
		_ = unstructured.SetNestedMap(object.Object, encoded, "status", "capacityUnit")
		_ = unstructured.SetNestedField(object.Object, true, "status", "registrationApproved")
		conditions := []any{}
		for _, kind := range []string{volumeapi.PoolConditionReady, volumeapi.PoolConditionAccessible,
			volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable,
			volumeapi.PoolConditionCapacityIndependent, volumeapi.PoolConditionCapacityBackingVerified} {
			conditions = append(conditions, map[string]any{"type": kind, "status": "True", "observedGeneration": int64(1), "reason": "Verified"})
		}
		_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
		if pool.Name == "pool-a" {
			_, err = api.Resource(volumeapi.PoolResource).Update(ctx, object, metav1.UpdateOptions{})
		} else {
			_, err = api.Resource(volumeapi.PoolResource).Create(ctx, object, metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := node.Volumes.ReadyPoolForIdentity(ctx, pool.Name, pool.UID, pool.NodeName); err != nil {
			t.Fatalf("healthy Pool did not admit before peer failure: %v", err)
		}
	}
	// Model API generation changes for scan requests, without a permissive
	// readiness fake or a fabricated native execution result.
	api.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	node.VerifyBacking = func(_ string, pool volumeapi.Pool) error { f.checks++; return f.probe.VerifyBacking(pool) }
	if err := node.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	if operation == "delete" {
		if err := node.execute(ctx, testID); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f.dataPath(), []byte("approved copy must survive rejected deletion\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := node.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
			t.Fatal(err)
		}
		f.state, err = node.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
		if err != nil {
			t.Fatal(err)
		}
		f.cleanup, err = node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: f.state.DeletionOperationID,
			Target: *state.CurrentCopy, Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
		if err != nil {
			t.Fatal(err)
		}
		if err := bindNodeCleanup(ctx, node, f.cleanup); err != nil {
			t.Fatal(err)
		}
	}
	f.checks = 0
	return f
}

func (f *peerBackingFixture) dataPath() string {
	return filepath.Join(f.node.HostRoot, "mnt/pool/volumes", testID, "payload")
}

func (f *peerBackingFixture) updatePool(t *testing.T, name string, change func(*unstructured.Unstructured)) {
	t.Helper()
	ctx := context.Background()
	object, err := f.api.Resource(volumeapi.PoolResource).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change(object)
	if _, err := f.api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func (f *peerBackingFixture) failPeer(t *testing.T, pendingScan bool) {
	t.Helper()
	f.peerFailed = true
	for _, name := range []string{"pool-a", "pool-b"} {
		f.updatePool(t, name, func(object *unstructured.Unstructured) {
			conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
			for _, item := range conditions {
				condition := item.(map[string]any)
				if condition["type"] == volumeapi.PoolConditionReady || condition["type"] == volumeapi.PoolConditionCapacityIndependent ||
					(name == "pool-b" && condition["type"] == volumeapi.PoolConditionCapacityBackingVerified) {
					condition["status"], condition["reason"] = "False", "CapacityPeerUnproven"
				}
			}
			_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
		})
	}
	if pendingScan {
		if _, err := f.node.Volumes.RequestPoolScan(context.Background(), *f.state.CurrentCopy); err != nil {
			t.Fatal(err)
		}
	}
	for _, identity := range [][2]string{{"pool-a", "pool-uid"}, {"pool-b", "peer-uid"}} {
		if _, err := f.node.Volumes.ReadyPoolForIdentity(context.Background(), identity[0], identity[1], "worker-a"); err == nil {
			t.Fatalf("peer failure admitted new allocation on %s", identity[0])
		}
		pool, err := f.node.Volumes.PoolForIdentity(context.Background(), identity[0], identity[1], "worker-a")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.probe.VerifyBacking(pool); (err != nil) != (identity[0] == "pool-b") {
			t.Fatalf("live backing for %s: %v", identity[0], err)
		}
	}
}

func TestNativeApprovedEffectsProgressDespitePeerBackingFailure(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		for _, pendingScan := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pending-scan=%v", operation, pendingScan), func(t *testing.T) {
				f := newPeerBackingFixture(t, operation)
				f.failPeer(t, pendingScan)
				ctx := context.Background()
				if err := f.node.execute(ctx, testID); err != nil {
					t.Fatalf("approved native effect blocked by peer: %v", err)
				}
				if f.checks == 0 {
					t.Fatal("effect bypassed live anchored backing verification")
				}
				current, err := f.node.Volumes.Get(ctx, testID)
				if err != nil || current.CurrentCopy == nil || *current.CurrentCopy != *f.state.CurrentCopy {
					t.Fatalf("approved copy identity changed: %+v %v", current, err)
				}
				if operation == "create" {
					if !volumeapi.ValidCreationReceipt(current) {
						t.Fatalf("creation lacks exact durable receipt: %+v", current)
					}
					if _, err := os.Stat(filepath.Dir(f.dataPath())); err != nil {
						t.Fatalf("native copy absent: %v", err)
					}
					return
				}
				f.assertCleanupFence(t)
			})
		}
	}
}

func (f *peerBackingFixture) assertCleanupFence(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := os.Stat(filepath.Dir(f.dataPath())); !os.IsNotExist(err) {
		t.Fatalf("native purge did not remove exact copy: %v", err)
	}
	verified, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
	if err != nil || verified.Status.Phase != cleanupapi.PhaseVerifying || verified.Status.Receipt == nil ||
		!verified.Status.Receipt.Purged || !verified.Status.Receipt.Retired || verified.Status.Receipt.ExecutorUID != f.node.Identity.PodUID {
		t.Fatalf("cleanup lacks exact receipt: %+v %v", verified, err)
	}
	fenced, settled, err := f.node.Cleanups.ReconcileAbsence(ctx, verified)
	if err != nil || settled || fenced.Status.AbsenceProof == nil {
		t.Fatalf("missing post-effect fence: %+v %v", fenced, err)
	}
	if _, settled, err := f.node.Cleanups.ReconcileAbsence(ctx, fenced); err != nil || settled {
		t.Fatalf("stale inventory settled cleanup: %v", err)
	}
	current, err := f.node.Volumes.Get(ctx, testID)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, f.state.CurrentCopy.PoolUID)
	if err != nil || reserved != f.state.CapacityBytes || current.Phase != volumeapi.PhaseDeleting {
		t.Fatalf("receipt prematurely released authority/hold: %+v reserved=%d %v", current, reserved, err)
	}
	// Only a complete inventory after the new generation fence proves absence.
	// The peer remains failed and allocation Ready remains false throughout.
	f.updatePool(t, "pool-a", func(object *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
		_ = unstructured.SetNestedMap(object.Object, map[string]any{"valid": true, "observedAt": time.Now().UTC().Format(time.RFC3339Nano)}, "status", "inventory")
	})
	completed, settled, err := f.node.Cleanups.ReconcileAbsence(ctx, fenced)
	if err != nil || !settled || completed.Status.Phase != cleanupapi.PhaseCompleted {
		t.Fatalf("fresh absence did not settle cleanup: %+v %v", completed, err)
	}
}

func TestNativePeerFailureDoesNotBypassOwnBackingOrAuthority(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		for _, change := range []string{"own backing condition", "live allocation changed", "Pool UID changed", "current copy changed"} {
			t.Run(operation+"/"+change, func(t *testing.T) {
				f := newPeerBackingFixture(t, operation)
				f.failPeer(t, true)
				ctx := context.Background()
				switch change {
				case "own backing condition":
					f.updatePool(t, "pool-a", func(object *unstructured.Unstructured) {
						conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
						for _, item := range conditions {
							condition := item.(map[string]any)
							if condition["type"] == volumeapi.PoolConditionCapacityBackingVerified {
								condition["status"], condition["reason"] = "False", "CapacityIdentityChanged"
							}
						}
						_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
					})
				case "live allocation changed":
					f.extents["8:1"] = []capacityunit.Extent{{Device: "8:0", Start: 500, Sectors: 100}}
				case "Pool UID changed":
					f.updatePool(t, "pool-a", func(object *unstructured.Unstructured) { object.SetUID("recreated-pool") })
				case "current copy changed":
					reads := 0
					f.api.PrependReactor("get", "shiftpvvolumes", func(ktesting.Action) (bool, runtime.Object, error) {
						reads++
						if reads != 2 {
							return false, nil, nil
						}
						// Change authority after the executor captures its work,
						// before the first filesystem mutation is authorized.
						stored, err := f.api.Tracker().Get(volumeapi.VolumeResource, "", testID)
						if err != nil {
							return true, nil, err
						}
						object := stored.(*unstructured.Unstructured).DeepCopy()
						_ = unstructured.SetNestedField(object.Object, "changed-copy", "status", "currentCopy", "copyID")
						return false, nil, f.api.Tracker().Update(volumeapi.VolumeResource, object, "")
					})
				}
				if err := f.node.execute(ctx, testID); err == nil {
					t.Fatal("changed backing or authority authorized a native effect")
				}
				if operation == "create" {
					current, err := f.node.Volumes.Get(ctx, testID)
					if err != nil || current.CreationReceipt != nil {
						t.Fatalf("rejected creation recorded receipt: %+v %v", current, err)
					}
					if _, err := os.Stat(filepath.Dir(f.dataPath())); !os.IsNotExist(err) {
						t.Fatalf("rejected creation made a copy: %v", err)
					}
					return
				}
				cleanup, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
				if err != nil || cleanup.Status.Receipt != nil {
					t.Fatalf("rejected deletion recorded receipt: %+v %v", cleanup, err)
				}
				data, err := os.ReadFile(f.dataPath())
				if err != nil || string(data) != "approved copy must survive rejected deletion\n" {
					t.Fatalf("rejected deletion modified data: %q %v", data, err)
				}
			})
		}
	}
}
