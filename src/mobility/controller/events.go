package controller

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
)

const mobilityEventSelector = "app.kubernetes.io/name=shiftpv,app.kubernetes.io/component=mobility"

type watchOpen func(context.Context) (watch.Interface, error)

// WatchEvents supplies a shared, coalescing wake-up stream for mobility state.
// Reconciler.Interval remains the bounded safety net when a watch disconnects or
// an event is missed; no CSI request owns a Kubernetes watch.
func WatchEvents(ctx context.Context, client kubernetes.Interface, dynamicClient dynamic.Interface, namespace string) <-chan struct{} {
	wake := make(chan struct{}, 1)
	options := func(selector string) metav1.ListOptions {
		timeout := int64(300)
		return metav1.ListOptions{LabelSelector: selector, TimeoutSeconds: &timeout}
	}
	watches := map[string]watchOpen{
		"nodes": func(ctx context.Context) (watch.Interface, error) {
			return client.CoreV1().Nodes().Watch(ctx, options(""))
		},
		"mobility-pods": func(ctx context.Context) (watch.Interface, error) {
			return client.CoreV1().Pods(namespace).Watch(ctx, options(mobilityEventSelector))
		},
		"workload-pods": func(ctx context.Context) (watch.Interface, error) {
			return client.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx, options(admission.ManagedLabel+"=true"))
		},
		"source-usage-pods": func(ctx context.Context) (watch.Interface, error) {
			return client.CoreV1().Pods(namespace).Watch(ctx, options(helperpod.SourceUsagePurposeLabel+"=true"))
		},
		"mobility-jobs": func(ctx context.Context) (watch.Interface, error) {
			return client.BatchV1().Jobs(namespace).Watch(ctx, options(mobilityEventSelector))
		},
		"shiftpv-volumes": func(ctx context.Context) (watch.Interface, error) {
			return dynamicClient.Resource(volumeapi.VolumeResource).Watch(ctx, options(""))
		},
		"shiftpv-moves": func(ctx context.Context) (watch.Interface, error) {
			return dynamicClient.Resource(volumeapi.MoveResource).Watch(ctx, options(""))
		},
		"shiftpv-pools": func(ctx context.Context) (watch.Interface, error) {
			return dynamicClient.Resource(volumeapi.PoolResource).Watch(ctx, options(""))
		},
	}
	for name, open := range watches {
		go runEventWatch(ctx, name, open, wake, time.Second)
	}
	return wake
}

func runEventWatch(ctx context.Context, name string, open watchOpen, wake chan<- struct{}, retryDelay time.Duration) {
	for ctx.Err() == nil {
		stream, err := open(ctx)
		if err == nil {
			for active := true; active; {
				select {
				case <-ctx.Done():
					stream.Stop()
					return
				case _, ok := <-stream.ResultChan():
					if !ok {
						active = false
						continue
					}
					notifyReconciler(wake)
				}
			}
			stream.Stop()
		} else if ctx.Err() == nil {
			klog.V(2).Infof("mobility event watch %s unavailable: %v", name, err)
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func notifyReconciler(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}
