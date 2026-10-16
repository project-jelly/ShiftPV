//go:build linux

package executor

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestMoveCleanupRechecksExactAuthorityBeforePurge(t *testing.T) {
	changes := map[string]func(*testing.T, moveCleanupFixture){
		"Volume UID": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.VolumeResource, testID, func(o *unstructured.Unstructured) { o.SetUID("replacement") })
		},
		"ActiveMove": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.VolumeResource, testID, func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "other-move", "status", "activeMove")
			})
		},
		"Move UID": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) { o.SetUID("replacement") })
		},
		"Move Volume": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "spec", "volumeID")
			})
		},
		"Move finalizer": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) { o.SetFinalizers(nil) })
		},
		"operation": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "different-operation", "status", "cleanup", "spec", "operationID")
			})
		},
		"target": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "different-copy", "status", "cleanup", "spec", "target", "copyID")
			})
		},
		"executor": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "replacement-pod", "status", "cleanup", "status", "executor", "podUID")
			})
		},
		"Pool UID": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.PoolResource, "pool-a", func(o *unstructured.Unstructured) { o.SetUID("replacement") })
		},
		"Pool protection": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.PoolResource, "pool-a", func(o *unstructured.Unstructured) { o.SetFinalizers(nil) })
		},
		"source authority": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(o.Object, "Copying", "status", "phase")
			})
		},
		"publication": func(t *testing.T, f moveCleanupFixture) {
			f.mutate(t, volumeapi.VolumeResource, testID, func(o *unstructured.Unstructured) {
				_ = unstructured.SetNestedStringSlice(o.Object, []string{"worker-a"}, "status", "publishedNodes")
			})
		},
	}
	for _, reason := range []string{"MoveSource", "MoveRollback"} {
		for name, change := range changes {
			for _, during := range []bool{false, true} {
				t.Run(reason+"/"+name+map[bool]string{false: "/before", true: "/before-retire"}[during], func(t *testing.T) {
					f := newMoveCleanupFixture(t, reason, volume.RoleServing)
					// Direct RPC fixtures represent a process with both caches synced.
					f.node.moveCleanupReady.Store(true)
					if during {
						changed := false
						f.node.VerifyBacking = func(string, volumeapi.Pool) error {
							if !changed {
								changed = true
								change(t, f)
							}
							return nil
						}
					} else {
						change(t, f)
					}
					if err := f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM); err == nil {
						t.Fatal("changed authority accepted")
					}
					f.requirePayload(t)
				})
			}
		}
	}
}

func TestMoveCleanupRefusesLostDestinationProofAndLiveBacking(t *testing.T) {
	for _, change := range []string{"destination publication", "destination inventory", "backing", "Pod identity"} {
		t.Run(change, func(t *testing.T) {
			f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
			f.node.moveCleanupReady.Store(true)
			switch change {
			case "destination publication":
				f.mutate(t, volumeapi.PoolResource, "pool-b", func(o *unstructured.Unstructured) {
					_ = unstructured.SetNestedSlice(o.Object, nil, "status", "inventory", "copies")
				})
			case "destination inventory":
				f.mutate(t, volumeapi.PoolResource, "pool-b", func(o *unstructured.Unstructured) {
					_ = unstructured.SetNestedField(o.Object, false, "status", "inventory", "valid")
				})
			case "backing":
				f.node.VerifyBacking = func(string, volumeapi.Pool) error { return errors.New("anchored mount changed") }
			case "Pod identity":
				pod, err := f.kube.CoreV1().Pods("shiftpv").Get(context.Background(), "node-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.UID = "replacement"
				if _, err := f.kube.CoreV1().Pods("shiftpv").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM); err == nil {
				t.Fatal("lost proof authorized cleanup")
			}
			f.requirePayload(t)
		})
	}
}

func TestMoveCleanupRPCAndWatchShareOneReceipt(t *testing.T) {
	f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
	f.node.moveCleanupReady.Store(true)
	var writes atomic.Int32
	f.api.PrependReactor("update", "shiftpvmoves", func(a ktesting.Action) (bool, runtime.Object, error) {
		o := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if phase, _, _ := unstructured.NestedString(o.Object, "status", "cleanup", "status", "phase"); phase == cleanupapi.PhaseVerifying {
			writes.Add(1)
		}
		return false, nil, nil
	})
	var workers sync.WaitGroup
	errors := make(chan error, 12)
	for i := range 12 {
		workers.Go(func() {
			if i%2 == 0 {
				errors <- f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM)
			} else {
				errors <- f.node.execute(context.Background(), testID)
			}
		})
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 1 {
		t.Fatalf("duplicate receipt writes: %d", writes.Load())
	}
}

func TestMoveCleanupLocalReceiptReplayRequiresCurrentAuthority(t *testing.T) {
	for _, replay := range []string{"same Pod", "replacement Pod", "revoked Move"} {
		t.Run(replay, func(t *testing.T) {
			f := newMoveCleanupFixture(t, "MoveRollback", volume.RoleIncoming)
			lost := false
			f.api.PrependReactor("update", "shiftpvmoves", func(a ktesting.Action) (bool, runtime.Object, error) {
				o := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				if phase, _, _ := unstructured.NestedString(o.Object, "status", "cleanup", "status", "phase"); phase == cleanupapi.PhaseVerifying && !lost {
					lost = true
					return true, nil, errors.New("receipt update unavailable")
				}
				return false, nil, nil
			})
			if err := f.node.execute(context.Background(), testID); err == nil {
				t.Fatal("expected receipt write loss")
			}
			if _, err := os.Stat(f.path); !os.IsNotExist(err) {
				t.Fatalf("purge did not finish: %v", err)
			}
			restarted := &Node{Identity: f.node.Identity, Pods: f.node.Pods, Volumes: f.node.Volumes, Cleanups: f.node.Cleanups, HostRoot: f.node.HostRoot}
			switch replay {
			case "replacement Pod":
				pod, err := f.kube.CoreV1().Pods("shiftpv").Get(context.Background(), "node-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.UID = "replacement-pod"
				if _, err := f.kube.CoreV1().Pods("shiftpv").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				restarted.Identity.PodUID = string(pod.UID)
				if err := bindNodeCleanup(context.Background(), restarted, f.cleanup); err != nil {
					t.Fatal(err)
				}
			case "revoked Move":
				f.mutate(t, volumeapi.MoveResource, "move-a", func(o *unstructured.Unstructured) {
					_ = unstructured.SetNestedField(o.Object, "Verifying", "status", "recoveryPhase")
				})
			}
			err := restarted.execute(context.Background(), testID)
			current, readErr := f.node.Cleanups.Get(context.Background(), f.cleanup.Spec.Authority)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if replay == "revoked Move" {
				if err == nil || current.Status.Receipt != nil {
					t.Fatalf("revoked authority replayed receipt: %+v %v", current, err)
				}
				return
			}
			if err != nil || current.Status.Receipt == nil || current.Status.Receipt.ExecutorUID != restarted.Identity.PodUID {
				t.Fatalf("exact local receipt not recovered: %+v %v", current, err)
			}
		})
	}
}

func TestMoveCleanupRPCNeverRedispatchesAnotherIntent(t *testing.T) {
	f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
	f.node.moveCleanupReady.Store(true)
	reads := 0
	f.api.PrependReactor("get", "shiftpvmoves", func(a ktesting.Action) (bool, runtime.Object, error) {
		reads++
		// resolve GetMove, journal Get, helperauth GetMove. Change the durable
		// operation after returning the exact original authority for validation.
		if reads != 3 {
			return false, nil, nil
		}
		value, err := f.api.Tracker().Get(volumeapi.MoveResource, "", "move-a")
		if err != nil {
			return true, nil, err
		}
		old := value.(*unstructured.Unstructured).DeepCopy()
		next := old.DeepCopy()
		next.SetUID("new-move-uid")
		_ = unstructured.SetNestedField(next.Object, "new-move-uid", "status", "cleanup", "spec", "authority", "uid")
		_ = unstructured.SetNestedField(next.Object, volumeapi.MoveCleanupOperationID("new-move-uid"), "status", "cleanup", "spec", "operationID")
		if err := f.api.Tracker().Update(volumeapi.MoveResource, next, ""); err != nil {
			return true, nil, err
		}
		return true, old, nil
	})
	if err := f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM); err == nil {
		t.Fatal("stale RPC accepted changed intent")
	}
	f.requirePayload(t)
	current, err := f.node.Cleanups.Get(context.Background(), cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a", UID: "new-move-uid"})
	if err != nil || current.Status.Receipt != nil {
		t.Fatalf("old RPC executed new operation: %+v %v", current, err)
	}
}

func TestMoveCleanupRetriesAcceptedReceiptWithoutAnotherEffect(t *testing.T) {
	f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
	f.node.moveCleanupReady.Store(true)
	writes := 0
	f.api.PrependReactor("update", "shiftpvmoves", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		phase, _, _ := unstructured.NestedString(object.Object, "status", "cleanup", "status", "phase")
		if phase != cleanupapi.PhaseVerifying {
			return false, nil, nil
		}
		writes++
		if err := f.api.Tracker().Update(volumeapi.MoveResource, object, ""); err != nil {
			return true, nil, err
		}
		return true, nil, context.DeadlineExceeded
	})
	if err := f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected accepted response loss: %v", err)
	}
	first, err := f.node.Cleanups.Get(context.Background(), f.cleanup.Spec.Authority)
	if err != nil || first.Status.Receipt == nil {
		t.Fatalf("accepted receipt missing: %+v %v", first, err)
	}
	if err := f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM); err != nil {
		t.Fatal(err)
	}
	after, err := f.node.Cleanups.Get(context.Background(), f.cleanup.Spec.Authority)
	if err != nil || after.Status.Receipt == nil || *after.Status.Receipt != *first.Status.Receipt || writes != 1 {
		t.Fatalf("receipt replay mutated outcome: %+v writes=%d %v", after, writes, err)
	}
}
