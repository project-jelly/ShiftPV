// Package provisioning reconnects capacity changes to waiting CSI requests.
package provisioning

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

const retryAnnotation = "shiftpv.io/capacity-retry"
const selectedNodeAnnotation = "volume.kubernetes.io/selected-node"
const maxWaiters = 4096
const waiterLifetime = 15 * time.Minute

type Repository interface {
	ReadyPools(context.Context) ([]volumeapi.Pool, error)
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	ListMoves(context.Context) ([]volumeapi.Move, error)
}

type waiter struct {
	namespace, name, uid, node, group string
	requested                         int64
	firstSeen, lastSeen               time.Time
	serial                            uint64
}

// Retries is a bounded, advisory index. It never grants capacity. Losing it on
// restart is safe: the provisioner's bounded retries register requests again.
// Resource changes and a fallback ticker wake it; Register never mutates PVCs.
// The server-side UID and resourceVersion of Update fence name reuse and races.
type Retries struct {
	Client  kubernetes.Interface
	Pools   Repository
	mu      sync.Mutex
	waiting map[string]waiter
	serial  uint64
	now     func() time.Time
}

func NewRetries(client kubernetes.Interface, pools Repository) *Retries {
	return &Retries{Client: client, Pools: pools, waiting: make(map[string]waiter), now: time.Now}
}

func (r *Retries) Register(namespace, name, uid, node, group string, requested int64) {
	if namespace == "" || name == "" || uid == "" || node == "" || requested <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.expire(now)
	previous, exists := r.waiting[uid]
	if !exists && len(r.waiting) >= maxWaiters {
		return
	}
	if group == "" {
		group = volumeapi.DefaultPoolGroup
	}
	first := now
	if exists {
		first = previous.firstSeen
	}
	r.serial++
	r.waiting[uid] = waiter{namespace, name, uid, node, group, requested, first, now, r.serial}
}

func (r *Retries) snapshot() []waiter {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire(r.now())
	result := make([]waiter, 0, len(r.waiting))
	for _, w := range r.waiting {
		result = append(result, w)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].firstSeen.Equal(result[j].firstSeen) {
			return result[i].uid < result[j].uid
		}
		return result[i].firstSeen.Before(result[j].firstSeen)
	})
	return result
}

func (r *Retries) expire(now time.Time) {
	for uid, w := range r.waiting {
		if now.Sub(w.lastSeen) >= waiterLifetime {
			delete(r.waiting, uid)
		}
	}
}

// Reconcile notifies only requests which now fit a live, valid logical ledger.
// Its per-Pool budget limits fan-out, but is only a hint: CreateVolume must
// re-check physical space and record its durable intent under the Pool lock.
func (r *Retries) Reconcile(ctx context.Context) error {
	waiting := r.snapshot()
	if len(waiting) == 0 {
		return nil
	}
	pools, err := r.Pools.ReadyPools(ctx)
	if err != nil {
		return err
	}
	volumes, err := r.Pools.ListVolumes(ctx)
	if err != nil {
		return err
	}
	moves, err := r.Pools.ListMoves(ctx)
	if err != nil {
		return err
	}
	free := make(map[string]int64, len(pools))
	for _, pool := range pools {
		limit, err := poolcapacity.LimitBytes(pool)
		if err != nil {
			return err
		}
		reserved, err := poolcapacity.ReservedBytesForPool(volumes, moves, pool.UID)
		if err != nil {
			return err
		}
		if reserved < limit {
			free[pool.UID] = limit - reserved
		}
	}
	var notifyErrors error
	for _, w := range waiting {
		if err := ctx.Err(); err != nil {
			return errors.Join(notifyErrors, err)
		}
		for _, pool := range pools {
			group := pool.PoolGroup
			if group == "" {
				group = volumeapi.DefaultPoolGroup
			}
			if pool.NodeName != w.node || group != w.group || w.requested > free[pool.UID] {
				continue
			}
			notified, err := r.notify(ctx, w)
			if err != nil {
				notifyErrors = errors.Join(notifyErrors, err)
				break
			}
			if notified {
				free[pool.UID] -= w.requested
			}
			break
		}
	}
	return notifyErrors
}

func (r *Retries) notify(ctx context.Context, w waiter) (bool, error) {
	pvc, err := r.Client.CoreV1().PersistentVolumeClaims(w.namespace).Get(ctx, w.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		r.take(w)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(pvc.UID) != w.uid || pvc.DeletionTimestamp != nil || pvc.Spec.VolumeName != "" || pvc.Status.Phase == corev1.ClaimBound || pvc.Annotations[selectedNodeAnnotation] != w.node {
		r.take(w)
		return false, nil
	}
	if !r.take(w) {
		return false, nil
	}
	desired := pvc.DeepCopy()
	if desired.Annotations == nil {
		desired.Annotations = make(map[string]string)
	}
	desired.Annotations[retryAnnotation] = fmt.Sprintf("%d-%d", r.now().UnixNano(), w.serial)
	_, err = r.Client.CoreV1().PersistentVolumeClaims(w.namespace).Update(ctx, desired, metav1.UpdateOptions{})
	if err != nil {
		r.mu.Lock()
		if _, exists := r.waiting[w.uid]; !exists && len(r.waiting) < maxWaiters {
			r.waiting[w.uid] = w
		}
		r.mu.Unlock()
		return false, err
	}
	klog.V(2).InfoS("Capacity retry requested", "namespace", w.namespace, "pvc", w.name, "node", w.node, "poolGroup", w.group)
	return true, nil
}

func (r *Retries) take(w waiter) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.waiting[w.uid]
	if !exists || current.serial != w.serial {
		return false
	}
	delete(r.waiting, w.uid)
	return true
}

// Run uses relisting informers so disconnected watches recover. A ticker
// provides a fallback for missed events; external-provisioner retries also
// remain active. No PVC watch is installed, preventing notification loops.
func (r *Retries) Run(ctx context.Context, client dynamic.Interface) {
	wake := make(chan struct{}, 1)
	notify := func(any) {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(client, 0)
	for _, resource := range []schema.GroupVersionResource{volumeapi.VolumeResource, volumeapi.MoveResource, volumeapi.PoolResource} {
		_, err := factory.ForResource(resource).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: notify, UpdateFunc: func(_, current any) { notify(current) }, DeleteFunc: notify,
		})
		if err != nil {
			klog.ErrorS(err, "Register capacity retry watch")
			return
		}
	}
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
		// Coalesce bursts of status updates before reading the live ledger.
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		select {
		case <-wake:
		default:
		}
		reconcileCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.Reconcile(reconcileCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			klog.V(2).InfoS("Capacity retry observation failed", "error", err)
		}
	}
}
