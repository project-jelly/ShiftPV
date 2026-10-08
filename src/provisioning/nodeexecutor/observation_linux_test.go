//go:build linux

package nodeexecutor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestNativeCreationObservationKeepsReceiptOnRetry(t *testing.T) {
	client, node, _, _, state := fixture(t)
	ctx := context.Background()
	node.HostRoot = t.TempDir()
	if err := os.MkdirAll(filepath.Join(node.HostRoot, "mnt", "pool"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	node.ObserveStep = func(step string, _ time.Duration) { counts[step]++ }
	if err := node.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"node_effect_lock_wait": 1, "node_effect_intent_read": 1, "node_create_effect": 1, "node_create_pool_root": 1, "node_create_authority": 4, "node_create_prepare_local": 1, "node_create_receipt_local": 1, "node_create_receipt_record": 1}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("steps=%v want=%v", counts, want)
	}
	created, err := client.Volumes.Get(ctx, testID)
	if err != nil || !volumeapi.ValidCreationReceipt(created) {
		t.Fatalf("creation receipt missing: state=%+v err=%v", created, err)
	}
	if err := node.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	want["node_effect_lock_wait"]++
	want["node_effect_intent_read"]++
	retried, err := client.Volumes.Get(ctx, testID)
	if err != nil || !reflect.DeepEqual(created, retried) || !reflect.DeepEqual(counts, want) {
		t.Fatalf("retry repeated creation or changed receipt: err=%v steps=%v", err, counts)
	}
}

func TestCreationObservationCompletesDespiteInventoryChangeAfterEffect(t *testing.T) {
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
	injected := false
	dynamic.PrependReactor("get", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if _, err := os.Stat(filepath.Join(root, "volumes", testID)); err != nil || injected {
			return false, nil, nil
		}
		injected = true
		object, err := dynamic.Tracker().Get(volumeapi.PoolResource, "", action.(ktesting.GetAction).GetName())
		if err != nil {
			return true, nil, err
		}
		pool := object.(*unstructured.Unstructured).DeepCopy()
		if err := unstructured.SetNestedField(pool.Object, false, "status", "inventory", "valid"); err != nil {
			return true, nil, err
		}
		return true, pool, dynamic.Tracker().Update(volumeapi.PoolResource, pool, "")
	})
	counts := map[string]int{}
	node.ObserveStep = func(step string, _ time.Duration) { counts[step]++ }
	if err := node.execute(ctx, testID); err != nil || !injected {
		t.Fatalf("inventory change blocked approved creation: injected=%v err=%v", injected, err)
	}
	created, err := client.Volumes.Get(ctx, testID)
	if err != nil || !volumeapi.ValidCreationReceipt(created) || counts["node_create_receipt_record"] != 1 || counts["node_create_prepare_local"] != 1 {
		t.Fatalf("approved creation lacks receipt: state=%+v steps=%v err=%v", created, counts, err)
	}
	if _, err := client.Volumes.ReadyPoolForIdentity(ctx, "pool-a", "pool-uid", node.Identity.NodeName); !errors.Is(err, volumeapi.ErrPoolNotReady) {
		t.Fatalf("invalid inventory admitted new placement: %v", err)
	}
	if err := node.execute(ctx, testID); err != nil {
		t.Fatal(err)
	}
	resumed, err := client.Volumes.Get(ctx, testID)
	if err != nil || !reflect.DeepEqual(created, resumed) || counts["node_create_effect"] != 1 || counts["node_create_receipt_record"] != 1 {
		t.Fatalf("retry changed intent or receipt: state=%+v steps=%v err=%v", resumed, counts, err)
	}
}

func TestNativeCreationObservationSeparatesGateFromBlockedAuthority(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "API failure"}[fail], func(t *testing.T) {
			client, node, dynamic, _, state := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			node.HostRoot = t.TempDir()
			root := filepath.Join(node.HostRoot, "mnt", "pool")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
				t.Fatal(err)
			}
			blocked, release := make(chan struct{}), make(chan struct{})
			reads := 0
			failure := errors.New("Pool API unavailable")
			dynamic.PrependReactor("get", "shiftpvpools", func(ktesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 2 {
					close(blocked)
					select {
					case <-release:
					case <-ctx.Done():
						return true, nil, ctx.Err()
					}
					if fail {
						return true, nil, failure
					}
				}
				return false, nil, nil
			})
			steps := make(chan string, 32)
			node.ObserveStep = func(step string, _ time.Duration) { steps <- step }
			done := make(chan error, 1)
			go func() { done <- node.execute(ctx, testID) }()
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatal("authority did not reach injected API boundary")
			}
			for _, want := range []string{"node_effect_lock_wait", "node_effect_intent_read", "node_create_pool_root"} {
				select {
				case got := <-steps:
					if got != want {
						t.Fatalf("before authority release: got=%s want=%s", got, want)
					}
				default:
					t.Fatalf("%s includes protected API wait", want)
				}
			}
			if len(steps) != 0 {
				t.Fatal("blocked authority or local work reported completion")
			}
			close(release)
			select {
			case err := <-done:
				if fail && !errors.Is(err, failure) || !fail && err != nil {
					t.Fatalf("effect result changed: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("effect did not finish after API release")
			}
			close(steps)
			counts := map[string]int{}
			for step := range steps {
				counts[step]++
			}
			if counts["node_create_authority"] == 0 || counts["node_create_prepare_local"] != 1 || counts["node_create_effect"] != 1 {
				t.Fatalf("finished or failed steps missing: %v", counts)
			}
			current, err := client.Volumes.Get(ctx, testID)
			if err != nil {
				t.Fatal(err)
			}
			if fail {
				if current.CreationReceipt != nil || counts["node_create_receipt_local"] != 0 || counts["node_create_receipt_record"] != 0 {
					t.Fatalf("failed authority proceeded to receipt: state=%+v steps=%v", current, counts)
				}
				if _, err := os.Stat(filepath.Join(root, "volumes", testID)); !os.IsNotExist(err) {
					t.Fatalf("failed authority created serving directory: %v", err)
				}
			} else if !volumeapi.ValidCreationReceipt(current) || counts["node_create_receipt_local"] != 1 || counts["node_create_receipt_record"] != 1 {
				t.Fatalf("successful effect lacks receipt: state=%+v steps=%v", current, counts)
			}
		})
	}
}
