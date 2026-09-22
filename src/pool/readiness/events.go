package readiness

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// WatchPoolChanges wakes the local scanner when a new generation (including an
// explicit absence scan request) or lifecycle request arrives. The informer is
// only a wake-up source: Reconcile always reads the current Pool from the API.
// Status-only updates must not trigger another scan of our own status write.
func WatchPoolChanges(ctx context.Context, client dynamic.Interface, nodeName string) <-chan struct{} {
	wake := make(chan struct{}, 1)
	pools := client.Resource(volumeapi.PoolResource)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return pools.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return pools.Watch(ctx, options)
		},
	}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	_, _ = informer.AddEventHandler(poolChangeHandler(nodeName, wake))
	go informer.RunWithContext(ctx)
	return wake
}

func poolChangeHandler(nodeName string, wake chan<- struct{}) cache.ResourceEventHandlerFuncs {
	notify := func(object any) {
		if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
			object = tombstone.Obj
		}
		pool, ok := object.(*unstructured.Unstructured)
		if !ok {
			return
		}
		node, _, _ := unstructured.NestedString(pool.Object, "spec", "nodeName")
		if node != nodeName {
			return
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    notify,
		DeleteFunc: notify,
		UpdateFunc: func(oldObject, newObject any) {
			oldPool := oldObject.(*unstructured.Unstructured)
			newPool := newObject.(*unstructured.Unstructured)
			if oldPool.GetUID() == newPool.GetUID() && oldPool.GetGeneration() == newPool.GetGeneration() &&
				oldPool.GetDeletionTimestamp().Equal(newPool.GetDeletionTimestamp()) &&
				oldPool.GetAnnotations()[volumeapi.PoolIdentityReleaseAnnotation] == newPool.GetAnnotations()[volumeapi.PoolIdentityReleaseAnnotation] {
				return
			}
			notify(oldPool)
			notify(newPool)
		},
	}
}
