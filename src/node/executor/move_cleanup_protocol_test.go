//go:build linux || darwin

package executor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	ktesting "k8s.io/client-go/testing"
)

// A malformed local receipt used to win over the durable protocol check in
// the recovery worker. That could mutate an unsupported Move to NeedsReview
// without ever reaching helperauth. Exercise each entry with the real Registry,
// journal Store and local receipt reader; no purge is needed for this fault.
func TestMoveCleanupProtocolGatePrecedesLocalReceipt(t *testing.T) {
	for _, reason := range []string{"MoveSource", "MoveRollback"} {
		for _, version := range []string{"", "future-v2", volumeapi.MoveProtocolPreparedV1} {
			for _, entry := range []string{"reconcile", "watch", "RPC"} {
				t.Run(reason+"/"+version+"/"+entry, func(t *testing.T) {
					f := newMoveCleanupFixture(t, reason, volume.RoleServing)
					f.mutate(t, volumeapi.MoveResource, f.cleanup.Spec.Authority.Name, func(object *unstructured.Unstructured) {
						if version == "" {
							unstructured.RemoveNestedField(object.Object, "spec", "protocolVersion")
						} else {
							_ = unstructured.SetNestedField(object.Object, version, "spec", "protocolVersion")
						}
					})
					sum := sha256.Sum256([]byte(f.cleanup.Spec.OperationID))
					receipt := filepath.Join(f.node.HostRoot, "mnt/pool/.shiftpv", fmt.Sprintf("receipt-%x.json", sum[:16]))
					if err := os.WriteFile(receipt, []byte(`{"version":0,"value":{}}`), 0600); err != nil {
						t.Fatal(err)
					}
					beforeFiles := snapshotMoveCleanupFiles(t, f.node.HostRoot)
					beforeMove := cleanupParent(t, f, volumeapi.MoveResource, f.cleanup.Spec.Authority.Name)
					beforeVolume := cleanupParent(t, f, volumeapi.VolumeResource, testID)
					writes := 0
					f.api.PrependReactor("update", "shiftpvmoves", func(ktesting.Action) (bool, runtime.Object, error) {
						writes++
						return false, nil, nil
					})
					var err error
					switch entry {
					case "reconcile":
						err = f.node.execute(context.Background(), testID)
					case "RPC":
						f.node.moveCleanupReady.Store(true)
						err = f.node.Execute(context.Background(), f.request(), protocol.Operation_RECLAIM)
					case "watch":
						runCleanupProtocolWatch(t, f)
					}
					if entry != "watch" {
						if version == volumeapi.MoveProtocolPreparedV1 && !errors.Is(err, ownership.ErrIdentity) {
							t.Fatalf("prepared cleanup did not inspect its malformed receipt: %v", err)
						}
						if version != volumeapi.MoveProtocolPreparedV1 && !errors.Is(err, volumeapi.ErrStateConflict) {
							t.Fatalf("unsupported protocol was not rejected before local evidence: %v", err)
						}
					}
					if !reflect.DeepEqual(beforeFiles, snapshotMoveCleanupFiles(t, f.node.HostRoot)) {
						t.Fatal("protocol or malformed-receipt handling changed local files")
					}
					f.requirePayload(t)
					if !reflect.DeepEqual(beforeVolume.Object, cleanupParent(t, f, volumeapi.VolumeResource, testID).Object) {
						t.Fatal("Node changed Volume ownership or capacity authority")
					}
					afterMove := cleanupParent(t, f, volumeapi.MoveResource, f.cleanup.Spec.Authority.Name)
					if version != volumeapi.MoveProtocolPreparedV1 {
						if writes != 0 || !reflect.DeepEqual(beforeMove.Object, afterMove.Object) {
							t.Fatalf("unsupported Move status or resourceVersion changed; writes=%d", writes)
						}
						return
					}
					current, err := f.node.Cleanups.Get(context.Background(), f.cleanup.Spec.Authority)
					if err != nil || current.Status.Phase != cleanupapi.PhaseNeedsReview || current.Status.Reason != "CopyEvidenceInvalid" || current.Status.Receipt != nil || current.Status.AbsenceProof != nil || writes != 1 {
						t.Fatalf("prepared Move lost evidence failure: %+v writes=%d err=%v", current, writes, err)
					}
					// The permitted journal failure does not release or change its
					// parent Move plan, execution hold, phase, or finalizer.
					unstructured.RemoveNestedField(beforeMove.Object, "status", "cleanup")
					unstructured.RemoveNestedField(beforeMove.Object, "status", "cleanupPhase")
					unstructured.RemoveNestedField(afterMove.Object, "status", "cleanup")
					unstructured.RemoveNestedField(afterMove.Object, "status", "cleanupPhase")
					if !reflect.DeepEqual(beforeMove.Object, afterMove.Object) {
						t.Fatal("evidence failure changed its parent Move authority")
					}
				})
			}
		}
	}
}

func cleanupParent(t *testing.T, f moveCleanupFixture, resource schema.GroupVersionResource, name string) *unstructured.Unstructured {
	t.Helper()
	object, err := f.api.Resource(resource).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func snapshotMoveCleanupFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var contents []byte
		if !entry.IsDir() {
			contents, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		files[path] = fmt.Sprintf("%s:%x", info.Mode(), contents)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func runCleanupProtocolWatch(t *testing.T, f moveCleanupFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	volumes, moves := watch.NewRaceFreeFake(), watch.NewRaceFreeFake()
	volumeOpened, moveOpened := make(chan struct{}, 1), make(chan struct{}, 1)
	f.api.PrependWatchReactor("shiftpvvolumes", func(ktesting.Action) (bool, watch.Interface, error) {
		volumeOpened <- struct{}{}
		return true, volumes, nil
	})
	f.api.PrependWatchReactor("shiftpvmoves", func(ktesting.Action) (bool, watch.Interface, error) {
		moveOpened <- struct{}{}
		return true, moves, nil
	})
	attempts := make(chan struct{}, 2)
	f.node.ObserveStep = func(step string, _ time.Duration) {
		if step == "node_effect_lock_wait" {
			select {
			case attempts <- struct{}{}:
			default:
			}
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.node.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("protocol-check watch worker did not stop")
		}
	}()
	for _, signal := range []<-chan struct{}{volumeOpened, moveOpened} {
		awaitCleanupProtocolSignal(t, signal, "intent watch did not open")
	}
	initialWatchObjects(volumes, cleanupParent(t, f, volumeapi.VolumeResource, testID))
	initialWatchObjects(moves, cleanupParent(t, f, volumeapi.MoveResource, f.cleanup.Spec.Authority.Name))
	// A second serialized attempt proves that the first rejection finished;
	// worker shutdown below joins any final attempt before assertions.
	for range 2 {
		awaitCleanupProtocolSignal(t, attempts, "watch recovery did not finish its first protocol check")
	}
}

func awaitCleanupProtocolSignal(t *testing.T, ready <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}
