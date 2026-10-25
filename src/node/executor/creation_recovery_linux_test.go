//go:build linux

package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	nodeobservation "github.com/project-jelly/ShiftPV/src/node/observation"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// API transport and publication lookup are fakes. Probe, Scanner, Reconciler,
// status writes, authorization and native recovery use production code.
// Persisted-state fixtures do not qualify SIGKILL or power-loss durability.
func TestNativeCreationRecoversThroughPoolReconciliation(t *testing.T) {
	for _, boundary := range []string{
		"before_copy_intent",
		"before_stage_mkdir",
		"before_placement_marker",
		"before_final_rename",
		"before_api_receipt",
		"before_controller_completion",
	} {
		t.Run(boundary, func(t *testing.T) {
			client, node, _, _, state := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			node.HostRoot = t.TempDir()
			root := filepath.Join(node.HostRoot, "mnt", "pool")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
				t.Fatal(err)
			}
			copy := *state.CurrentCopy
			prepareCreationBoundary(t, ctx, node, root, copy, boundary)
			approved, err := client.Volumes.Get(ctx, testID)
			if err != nil {
				t.Fatal(err)
			}
			receipt := approved.CreationReceipt
			scanner := &nodeobservation.Scanner{
				HostRoot: node.HostRoot, TargetRoot: "/pods", Installation: client.Volumes,
				Publications: unpublishedCreation{}, Limit: 256,
			}
			reconciler, err := readiness.NewReconciler(node.Identity.NodeName, client.Volumes, readiness.NewProbe(node.HostRoot), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			reconciler.Inventory = scanner.Scan
			// Recreate the executor under the same Pod UID and durable approval.
			restarted := &Node{Identity: node.Identity, Pods: node.Pods, Volumes: node.Volumes, Cleanups: node.Cleanups, HostRoot: node.HostRoot}
			for attempt := 0; attempt < 3; attempt++ {
				pool := reconcileCreationPool(t, ctx, reconciler, client, copy)
				if attempt == 0 && (boundary == "before_placement_marker" || boundary == "before_final_rename") {
					if pool.Status.Inventory.Valid || pool.Status.Inventory.Message != "CopyObservationProblem" {
						t.Fatalf("fixture did not expose the incomplete serving stage: %+v", pool.Status.Inventory)
					}
				}
				// An incomplete inventory must still reject new placement, even
				// though the exact already-approved creation must be recoverable.
				if !pool.Status.Inventory.Valid {
					if _, err := client.Volumes.ReadyPoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName); !errors.Is(err, volumeapi.ErrPoolNotReady) {
						t.Fatalf("invalid inventory admitted new placement: %v", err)
					}
				}
				if err := restarted.execute(ctx, testID); err != nil {
					t.Errorf("retry %d: approved creation could not recover: %v", attempt+1, err)
				}
				current, err := client.Volumes.Get(ctx, testID)
				if err != nil {
					t.Fatal(err)
				}
				if current.UID != approved.UID || !reflect.DeepEqual(current.CurrentCopy, approved.CurrentCopy) || current.CreationOperationID != approved.CreationOperationID || !reflect.DeepEqual(current.CreationExecutor, approved.CreationExecutor) || current.Phase != volumeapi.PhaseNodeCreating {
					t.Fatalf("retry changed creation approval: %+v", current)
				}
				reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, copy.PoolUID)
				if err != nil || reserved != state.CapacityBytes {
					t.Fatalf("retry changed reservation: reserved=%d err=%v", reserved, err)
				}
				if !volumeapi.ValidCreationReceipt(current) {
					if err := client.Volumes.CompleteCreate(ctx, testID, current.UID, copy); !errors.Is(err, volumeapi.ErrStateConflict) {
						t.Fatalf("controller accepted an incomplete creation: %v", err)
					}
					continue
				}
				if err := ownership.VerifyServingPath(root, copy); err != nil {
					t.Fatalf("receipt has no exact serving placement: %v", err)
				}
				if receipt != nil && *receipt != *current.CreationReceipt {
					t.Fatal("retry replaced the recorded receipt")
				}
				receipt = current.CreationReceipt
			}
			pool := reconcileCreationPool(t, ctx, reconciler, client, copy)
			inventory := pool.Status.Inventory
			if !inventory.Valid || inventory.Truncated || inventory.Message != "" || len(inventory.Copies) != 1 || inventory.Copies[0].Identity == nil || *inventory.Copies[0].Identity != copy || !inventory.Copies[0].Present {
				t.Fatalf("recovery did not produce an exact complete inventory: %+v", inventory)
			}
			if err := client.Volumes.CompleteCreate(ctx, testID, approved.UID, copy); err != nil {
				t.Fatal(err)
			}
			completed, err := client.Volumes.Get(ctx, testID)
			if err != nil || completed.Phase != volumeapi.PhaseReady {
				t.Fatalf("creation did not complete: state=%+v err=%v", completed, err)
			}
		})
	}
}

func TestNativeCreationRecoveryPreservesStageOnAuthorityOrIdentityLoss(t *testing.T) {
	for _, change := range []string{"operation", "executor", "publication", "Pool protection", "copy intent", "placement inode"} {
		t.Run(change, func(t *testing.T) {
			client, node, dynamic, _, state := fixture(t)
			ctx := context.Background()
			node.HostRoot = t.TempDir()
			root := filepath.Join(node.HostRoot, "mnt", "pool")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
				t.Fatal(err)
			}
			copy := *state.CurrentCopy
			prepareCreationBoundary(t, ctx, node, root, copy, "before_final_rename")
			scanner := &nodeobservation.Scanner{HostRoot: node.HostRoot, TargetRoot: "/pods", Installation: client.Volumes, Publications: unpublishedCreation{}, Limit: 256}
			reconciler, err := readiness.NewReconciler(node.Identity.NodeName, client.Volumes, readiness.NewProbe(node.HostRoot), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			reconciler.Inventory = scanner.Scan
			pool := reconcileCreationPool(t, ctx, reconciler, client, copy)
			if pool.Status.Inventory.Valid {
				t.Fatal("fixture did not expose an incomplete stage")
			}
			stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)
			switch change {
			case "operation", "executor", "publication":
				object, err := dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var changeErr error
				switch change {
				case "operation":
					changeErr = unstructured.SetNestedField(object.Object, "other-operation", "status", "creationOperationID")
				case "executor":
					changeErr = unstructured.SetNestedField(object.Object, "other-pod", "status", "creationExecutor", "podUID")
				case "publication":
					changeErr = unstructured.SetNestedStringSlice(object.Object, []string{node.Identity.NodeName}, "status", "publishedNodes")
				}
				if changeErr != nil {
					t.Fatal(changeErr)
				}
				if _, err := dynamic.Resource(volumeapi.VolumeResource).UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "Pool protection":
				object, err := dynamic.Resource(volumeapi.PoolResource).Get(ctx, copy.PoolName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				object.SetFinalizers(nil)
				if _, err := dynamic.Resource(volumeapi.PoolResource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "copy intent":
				if err := os.Remove(filepath.Join(root, ".shiftpv", "copy-"+copy.CopyID+".json")); err != nil {
					t.Fatal(err)
				}
			case "placement inode":
				// Keep the original inode alive so the replacement cannot reuse it.
				if err := os.Rename(stage, stage+"-preserved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(stage, 0700); err != nil {
					t.Fatal(err)
				}
			}
			sentinel := filepath.Join(stage, "data")
			if err := os.WriteFile(sentinel, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(stage)
			if err != nil {
				t.Fatal(err)
			}
			if change == "executor" {
				if err := node.creationAuthority(ctx, copy, root); !errors.Is(err, volumeapi.ErrStateConflict) {
					t.Fatalf("old executor retained creation authority: %v", err)
				}
			}
			// Work assigned to another executor is ignored by the watch path.
			if err := node.execute(ctx, testID); err == nil && change != "executor" {
				t.Fatal("changed authority or identity completed creation")
			}
			after, err := os.Stat(stage)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("rejection changed the staged inode: %v", err)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "preserve" {
				t.Fatalf("rejection changed staged data: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, "volumes", testID)); !os.IsNotExist(err) {
				t.Fatalf("rejection promoted the staged directory: %v", err)
			}
			current, err := client.Volumes.Get(ctx, testID)
			if err != nil || current.CreationReceipt != nil || current.Phase != volumeapi.PhaseNodeCreating {
				t.Fatalf("rejection completed the API receipt: state=%+v err=%v", current, err)
			}
			reserved, err := capacity.ReservedBytesForPool(map[string]volumeapi.State{testID: current}, nil, copy.PoolUID)
			if err != nil || reserved != state.CapacityBytes {
				t.Fatalf("rejection released reservation: reserved=%d err=%v", reserved, err)
			}
		})
	}
}

type unpublishedCreation struct{}

func (unpublishedCreation) HasPublishedTarget(string, string) (bool, error) { return false, nil }

func reconcileCreationPool(t *testing.T, ctx context.Context, reconciler *readiness.Reconciler, client *Client, copy volume.CopyIdentity) volumeapi.Pool {
	t.Helper()
	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := client.Volumes.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != pool.Generation || pool.Status.ObservedGeneration != pool.Generation || pool.Status.Inventory == nil {
		t.Fatalf("backing readiness is unavailable: %+v", pool.Status)
	}
	return pool
}

func prepareCreationBoundary(t *testing.T, ctx context.Context, node *Node, root string, copy volume.CopyIdentity, boundary string) {
	t.Helper()
	if boundary == "before_copy_intent" {
		store, err := ownership.Open(root, ownership.PoolIdentity{InstallationID: copy.InstallationID, PoolUID: copy.PoolUID})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	// Produce canonical markers with live authority checks, then rewind only
	// the test filesystem to the interrupted state, preserving device/inode.
	if err := ownership.PrepareServing(ctx, root, copy, func(ctx context.Context) error { return node.creationAuthority(ctx, copy, root) }); err != nil {
		t.Fatal(err)
	}
	switch boundary {
	case "before_stage_mkdir", "before_placement_marker", "before_final_rename":
		stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)
		if err := os.Rename(filepath.Join(root, "volumes", copy.VolumeID), stage); err != nil {
			t.Fatal(err)
		}
		if boundary != "before_final_rename" {
			if err := os.Remove(filepath.Join(root, ".shiftpv", "placements", "placement-"+copy.CopyID+".json")); err != nil {
				t.Fatal(err)
			}
		}
		if boundary == "before_stage_mkdir" {
			if err := os.Remove(stage); err != nil {
				t.Fatal(err)
			}
		}
	case "before_controller_completion":
		if err := node.execute(ctx, testID); err != nil {
			t.Fatal(err)
		}
	}
}
