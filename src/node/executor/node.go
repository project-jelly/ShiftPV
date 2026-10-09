package executor

import (
	"context"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/executor/internal/execution"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

// PodStore is scoped to the executor namespace and exposes only self-Pod reads
// and capability advertisement.
type PodStore interface {
	Get(context.Context, string, metav1.GetOptions) (*corev1.Pod, error)
	Update(context.Context, *corev1.Pod, metav1.UpdateOptions) (*corev1.Pod, error)
}

// Node watches durable Volume intents. Relists recover work after a process
// restart. It never selects a Pool, grants capacity, or completes a deletion.
type Node struct {
	Identity volumeapi.NodeExecutor
	Pods     PodStore
	Volumes  *volumeapi.Registry
	Cleanups *cleanupapi.Store
	HostRoot string
	// VerifyPool overrides live host observation for fault injection. Nil uses
	// readiness.VerifyHostPool at every authority boundary.
	VerifyPool     func(string, volumeapi.Pool) error
	RPCCertificate string
	RPCPort        string
	ObserveStep    func(string, time.Duration)
	gate           execution.Gate
}

func (n *Node) Run(ctx context.Context) error {
	if !n.Identity.Valid() || n.Volumes == nil || n.Cleanups == nil || n.Pods == nil {
		return fmt.Errorf("resident Node executor configuration is incomplete")
	}
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer queue.ShutDown()
	volumes := n.Volumes.Client.Resource(volumeapi.VolumeResource)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc:  func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) { return volumes.List(ctx, o) },
		WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) { return volumes.Watch(ctx, o) },
	}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	notify := func(value any) {
		object, ok := value.(*unstructured.Unstructured)
		if ok && n.assigned(object) {
			queue.Add(object.GetName())
		}
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: notify, UpdateFunc: func(_, value any) { notify(value) }})
	go informer.RunWithContext(ctx)
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil
	}
	if err := n.advertise(ctx); err != nil {
		return err
	}
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Go(func() { n.work(ctx, queue) })
	}
	<-ctx.Done()
	queue.ShutDown()
	workers.Wait()
	return nil
}
func (n *Node) assigned(object *unstructured.Unstructured) bool {
	uid, _, _ := unstructured.NestedString(object.Object, "status", "creationExecutor", "podUID")
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	if uid == n.Identity.PodUID && phase == volumeapi.PhaseNodeCreating {
		return true
	}
	uid, _, _ = unstructured.NestedString(object.Object, "status", "cleanup", "status", "executor", "podUID")
	kind, _, _ := unstructured.NestedString(object.Object, "status", "cleanup", "status", "executor", "kind")
	phase, _, _ = unstructured.NestedString(object.Object, "status", "cleanup", "status", "phase")
	return uid == n.Identity.PodUID && kind == cleanupapi.ExecutorNode && phase == cleanupapi.PhaseRunning
}
func (n *Node) advertise(ctx context.Context) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := n.Pods.Get(ctx, n.Identity.PodName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(pod.UID) != n.Identity.PodUID || pod.Spec.NodeName != n.Identity.NodeName || pod.DeletionTimestamp != nil {
			return volumeapi.ErrStateConflict
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[volumeapi.NodeEffectsAnnotation] = "v1"
		if n.RPCCertificate != "" {
			pod.Annotations[security.CertificateAnnotation] = n.RPCCertificate
			pod.Annotations[security.PortAnnotation] = n.RPCPort
		}
		_, err = n.Pods.Update(ctx, pod, metav1.UpdateOptions{})
		return err
	})
}
func (n *Node) work(ctx context.Context, queue workqueue.TypedRateLimitingInterface[string]) {
	for {
		name, shutdown := queue.Get()
		if shutdown {
			return
		}
		operationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := n.execute(operationCtx, name)
		cancel()
		if err != nil {
			klog.ErrorS(err, "resident Node effect retry", "volume", name)
			queue.AddRateLimited(name)
		} else {
			queue.Forget(name)
		}
		queue.Done(name)
	}
}
