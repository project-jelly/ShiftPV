//go:build linux

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	ktesting "k8s.io/client-go/testing"
)

func initialWatchObjects(stream *watch.RaceFreeFakeWatcher, object *unstructured.Unstructured) {
	initial := object.DeepCopy()
	initial.SetResourceVersion("1")
	stream.Add(initial)
	bookmark := &unstructured.Unstructured{}
	bookmark.SetResourceVersion("1")
	bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	stream.Action(watch.Bookmark, bookmark)
}

func TestMoveCleanupStartupWaitsForBothWatchesBeforeCapability(t *testing.T) {
	for _, last := range []string{"Volume", "Move"} {
		t.Run(last, func(t *testing.T) {
			f := newMoveCleanupFixture(t, "MoveSource", volume.RoleServing)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pod, err := f.kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Annotations[volumeapi.NodeMoveCleanupAnnotation] = "v1"
			if _, err := f.kube.CoreV1().Pods("shiftpv").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
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
			done := make(chan error, 1)
			go func() { done <- f.node.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(3 * time.Second):
					t.Error("Node did not stop")
				}
			})
			for _, opened := range []<-chan struct{}{volumeOpened, moveOpened} {
				select {
				case <-opened:
				case <-time.After(3 * time.Second):
					t.Fatal("intent watch did not open")
				}
			}
			v, err := f.api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			m, err := f.api.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if last == "Move" {
				initialWatchObjects(volumes, v)
			} else {
				initialWatchObjects(moves, m)
			}
			pod, err = f.kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
			if err != nil || pod.Annotations[volumeapi.NodeMoveCleanupAnnotation] != "" || f.node.moveCleanupReady.Load() {
				t.Fatalf("stale/early capability: %+v %v", pod.Annotations, err)
			}
			if err := f.node.Execute(ctx, f.request(), protocol.Operation_RECLAIM); err == nil {
				t.Fatal("Move RPC executed before watch synchronization")
			}
			f.requirePayload(t)
			if last == "Move" {
				initialWatchObjects(moves, m)
			} else {
				initialWatchObjects(volumes, v)
			}
			deadline := time.After(5 * time.Second)
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				current, err := f.node.Cleanups.Get(ctx, f.cleanup.Spec.Authority)
				if err != nil {
					t.Fatal(err)
				}
				if current.Status.Phase == cleanupapi.PhaseVerifying && current.Status.Receipt != nil {
					break
				}
				select {
				case <-tick.C:
				case <-deadline:
					t.Fatal("persisted Move journal did not recover from initial watch")
				}
			}
			pod, err = f.kube.CoreV1().Pods("shiftpv").Get(ctx, "node-a", metav1.GetOptions{})
			if err != nil || pod.Annotations[volumeapi.NodeMoveCleanupAnnotation] != "v1" {
				t.Fatalf("synced capability missing: %+v %v", pod.Annotations, err)
			}
		})
	}
}
