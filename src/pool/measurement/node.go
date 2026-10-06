package measurement

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type nodeRegistry interface {
	probeRegistry
	RecordPoolCapacityProbe(context.Context, volumeapi.Pool, volumeapi.PoolCapacityProbeResult) error
}

type poolKey struct{ name, uid string }

// Node executes only reads requested through the Pool API. Status writes do
// not enqueue another read. Relists recover requests after restart/watch loss.
type Node struct {
	NodeName string
	Pools    nodeRegistry
	Probe    filesystemProbe
}

func (n *Node) Run(ctx context.Context, client dynamic.Interface) error {
	if n.NodeName == "" || n.Pools == nil || n.Probe == nil || client == nil {
		return fmt.Errorf("Node capacity probe configuration is incomplete")
	}
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[poolKey]())
	defer queue.ShutDown()
	pools := client.Resource(volumeapi.PoolResource)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return pools.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return pools.Watch(ctx, options)
		},
	}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	_, _ = informer.AddEventHandler(n.handler(queue.Add))
	go informer.RunWithContext(ctx)
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil
	}
	go func() {
		<-ctx.Done()
		queue.ShutDown()
	}()
	for {
		key, shutdown := queue.Get()
		if shutdown {
			return nil
		}
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := n.answer(readCtx, key)
		cancel()
		if err != nil {
			klog.V(2).InfoS("Node capacity probe retry", "pool", key.name, "err", err)
			queue.AddRateLimited(key)
		} else {
			queue.Forget(key)
		}
		queue.Done(key)
	}
}

func (n *Node) handler(enqueue func(poolKey)) cache.ResourceEventHandlerFuncs {
	notify := func(object any) {
		pool, ok := object.(*unstructured.Unstructured)
		if !ok || pool.GetDeletionTimestamp() != nil || pool.GetAnnotations()[volumeapi.PoolCapacityProbeRequestAnnotation] == "" {
			return
		}
		node, _, _ := unstructured.NestedString(pool.Object, "spec", "nodeName")
		if node == n.NodeName {
			enqueue(poolKey{pool.GetName(), string(pool.GetUID())})
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: notify,
		UpdateFunc: func(oldObject, newObject any) {
			oldPool, newPool := oldObject.(*unstructured.Unstructured), newObject.(*unstructured.Unstructured)
			if oldPool.GetAnnotations()[volumeapi.PoolCapacityProbeRequestAnnotation] != newPool.GetAnnotations()[volumeapi.PoolCapacityProbeRequestAnnotation] {
				notify(newObject)
			}
		},
	}
}

func (n *Node) answer(ctx context.Context, key poolKey) error {
	pool, err := n.Pools.ReadyPoolForIdentity(ctx, key.name, key.uid, n.NodeName)
	if err != nil {
		if errors.Is(err, volumeapi.ErrPoolNotFound) || errors.Is(err, volumeapi.ErrStateConflict) {
			return nil
		}
		return err
	}
	r, err := parseRequest(pool.CapacityProbeRequest)
	if err != nil {
		return nil // malformed input cannot keep the worker retrying
	}
	if pool.Status.CapacityProbe != nil && pool.Status.CapacityProbe.RequestID == r.ID {
		return nil
	}
	result := volumeapi.PoolCapacityProbeResult{RequestID: r.ID, Evidence: r.Evidence, ObservedAt: metav1.Now()}
	evidence, err := matchingEvidence(pool, pool)
	if err == nil && evidence != r.Evidence {
		err = fmt.Errorf("requested Pool evidence changed")
	}
	if err == nil {
		stats, readErr := n.Probe.StatFSForPool(ctx, pool)
		err = readErr
		result.TotalBytes, result.AvailableBytes, result.AvailableInodes = stats.TotalBytes, stats.AvailableBytes, stats.AvailableInodes
	}
	if err != nil {
		result.Error = err.Error()
		if len(result.Error) > 1024 {
			result.Error = result.Error[:1024]
		}
	}
	err = n.Pools.RecordPoolCapacityProbe(ctx, pool, result)
	if errors.Is(err, volumeapi.ErrStateConflict) {
		return nil // a newer request/generation supersedes this answer
	}
	return err
}
