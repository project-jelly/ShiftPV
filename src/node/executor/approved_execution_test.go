package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	mountutils "k8s.io/mount-utils"
)

type approvedBacking struct {
	extents []capacityunit.Extent
	mount   mountutils.MountInfo
	checks  int
}

func (b *approvedBacking) Resolve(string) ([]capacityunit.Extent, error) { return b.extents, nil }

// Files and native creation are real. Mount/sysfs evidence is injected; this
// fixture cannot qualify an actual LVM device or mount-loss recovery.
func installApprovedBacking(t *testing.T, node *Node, api *dynamicfake.FakeDynamicClient) *approvedBacking {
	t.Helper()
	ctx := context.Background()
	host, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node.HostRoot = host
	if err := os.MkdirAll(filepath.Join(host, "mnt/pool"), 0700); err != nil {
		t.Fatal(err)
	}
	b := &approvedBacking{extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}},
		mount: mountutils.MountInfo{MountPoint: host, Major: 8, Minor: 1, Source: "/dev/a", FsType: "ext4", Root: "/"}}
	probe, err := readiness.NewProbeWithDependencies(host, readiness.ProbeDependencies{
		Allocations: b,
		InspectDirectory: func(path string) error {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return fmt.Errorf("not a directory")
			}
			return nil
		},
		Write: func(string) error { t.Error("execution ran a registration write probe"); return nil },
		StatFS: func(string) (capacity.Filesystem, error) {
			t.Error("execution ran admission capacity measurement")
			return capacity.Filesystem{}, nil
		},
		ListMounts: func() ([]mountutils.MountInfo, error) { return []mountutils.MountInfo{b.mount}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := node.Volumes.PoolForIdentity(ctx, "pool-a", "pool-uid", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	pool.CapacityPolicy = volumeapi.PoolCapacityPolicyFixedBlock
	unit, check := probe.InspectCapacityUnit(pool)
	if !check.OK {
		t.Fatal(check)
	}
	object, err := api.Resource(volumeapi.PoolResource).Get(ctx, pool.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(object.Object, pool.CapacityPolicy, "spec", "capacityPolicy")
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(unit)
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedMap(object.Object, encoded, "status", "capacityUnit")
	_ = unstructured.SetNestedField(object.Object, true, "status", "registrationApproved")
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	conditions = append(conditions, map[string]any{"type": volumeapi.PoolConditionCapacityIndependent, "status": "True", "observedGeneration": int64(1), "reason": "OK"})
	_ = unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions")
	// Model another operation's scan request after approval. No fresh scan is
	// supplied by the test before creation executes.
	object.SetGeneration(2)
	_ = unstructured.SetNestedField(object.Object, int64(1), "spec", "scanEpoch")
	if _, err := api.Resource(volumeapi.PoolResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	node.VerifyBacking = func(_ string, p volumeapi.Pool) error { b.checks++; return probe.VerifyBacking(p) }
	return b
}

func TestApprovedCreationProgressesWhileSiblingScanIsPending(t *testing.T) {
	client, node, api, _, state := fixture(t)
	ctx := context.Background()
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	backing := installApprovedBacking(t, node, api)
	if err := node.execute(ctx, testID); err != nil {
		t.Fatalf("approved creation waited for unrelated scan: %v", err)
	}
	current, err := client.Volumes.Get(ctx, testID)
	if err != nil || !volumeapi.ValidCreationReceipt(current) || current.CreationOperationID != state.CreationOperationID || *current.CurrentCopy != *state.CurrentCopy {
		t.Fatalf("creation lost its exact receipt or identity: %+v %v", current, err)
	}
	if backing.checks != 4 {
		t.Fatalf("live backing checks=%d, want four", backing.checks)
	}
	reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, state.CurrentCopy.PoolUID)
	if err != nil || reserved != state.CapacityBytes {
		t.Fatalf("reservation changed: %d %v", reserved, err)
	}
	if _, err := client.Volumes.ReadyPoolForIdentity(ctx, "pool-a", "pool-uid", "worker-a"); err == nil {
		t.Fatal("pending scan admitted new placement")
	}
}

func TestPendingScanNeverBypassesLiveApprovedBackingChecks(t *testing.T) {
	for _, change := range []string{"mount", "extents", "missing anchor"} {
		t.Run(change, func(t *testing.T) {
			client, node, api, _, state := fixture(t)
			ctx := context.Background()
			if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
				t.Fatal(err)
			}
			backing := installApprovedBacking(t, node, api)
			switch change {
			case "mount":
				backing.mount.Source = "/dev/changed"
			case "extents":
				backing.extents = []capacityunit.Extent{{Device: "8:0", Start: 200, Sectors: 100}}
			case "missing anchor":
				object, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				unstructured.RemoveNestedField(object.Object, "status", "capacityUnit")
				if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := node.execute(ctx, testID); err == nil {
				t.Fatal("unsafe backing authorized an effect")
			}
			current, err := client.Volumes.Get(ctx, testID)
			if err != nil || current.CreationReceipt != nil {
				t.Fatalf("unsafe backing recorded receipt: %+v %v", current, err)
			}
			if _, err := os.Stat(filepath.Join(node.HostRoot, "mnt/pool/volumes", testID)); !os.IsNotExist(err) {
				t.Fatalf("unsafe backing created data: %v", err)
			}
		})
	}
}
