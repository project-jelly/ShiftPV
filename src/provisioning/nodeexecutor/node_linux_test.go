//go:build linux

package nodeexecutor

import (
	"context"
	"errors"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeRecoversPersistedIntentOnStartup(t *testing.T) {
	client, node, dynamic, kube, state := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.HostRoot = t.TempDir()
	if err := os.MkdirAll(filepath.Join(node.HostRoot, "mnt", "pool"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	var advertiseWrites atomic.Int32
	kube.PrependReactor("update", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if advertiseWrites.Add(1) == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "node-a", errors.New("concurrent Pod status"))
		}
		return false, nil, nil
	})
	stream := watch.NewRaceFreeFake()
	opened := make(chan struct{}, 1)
	dynamic.PrependWatchReactor("shiftpvvolumes", func(ktesting.Action) (bool, watch.Interface, error) { opened <- struct{}{}; return true, stream, nil })
	var receipts atomic.Int32
	dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		if _, found, _ := unstructured.NestedMap(object.Object, "status", "creationReceipt"); found {
			receipts.Add(1)
		}
		if err := dynamic.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
			return true, nil, err
		}
		stream.Modify(object.DeepCopy())
		return true, object, nil
	})
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx) }()
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not open")
	}
	initial, _ := dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	initial.SetResourceVersion("1")
	stream.Add(initial.DeepCopy())
	bookmark := initial.DeepCopy()
	bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	stream.Action(watch.Bookmark, bookmark)
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.Volumes.Get(ctx, testID)
		if err != nil {
			t.Fatal(err)
		}
		if volumeapi.ValidCreationReceipt(current) {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("persisted intent lost after worker startup")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	if receipts.Load() != 1 {
		t.Fatalf("receipt status triggered repeated effects: %d", receipts.Load())
	}
}
