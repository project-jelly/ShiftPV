package provisioning

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type retryRepository struct {
	mu      sync.Mutex
	pools   []volumeapi.Pool
	volumes map[string]volumeapi.State
	moves   []volumeapi.Move
	err     error
}

func (r *retryRepository) ReadyPools(context.Context) ([]volumeapi.Pool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]volumeapi.Pool(nil), r.pools...), r.err
}
func (r *retryRepository) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]volumeapi.State)
	for k, v := range r.volumes {
		result[k] = v
	}
	return result, r.err
}
func (r *retryRepository) ListMoves(context.Context) ([]volumeapi.Move, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]volumeapi.Move(nil), r.moves...), r.err
}
func retryPVC(uid string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: uid, Namespace: "vmtest", UID: types.UID(uid), ResourceVersion: "1", Annotations: map[string]string{selectedNodeAnnotation: "node", "other": "keep"}}}
}
func retrySetup(t *testing.T, ids ...string) (*Retries, *retryRepository, *fake.Clientset) {
	t.Helper()
	objects := []runtime.Object{}
	for _, id := range ids {
		objects = append(objects, retryPVC(id))
	}
	client := fake.NewClientset(objects...)
	repo := &retryRepository{pools: []volumeapi.Pool{{Name: "pool", UID: "pool", NodeName: "node", CapacityLimit: "128Mi"}}, volumes: map[string]volumeapi.State{}}
	retries := NewRetries(client, repo)
	for _, id := range ids {
		retries.Register("vmtest", id, id, "node", "", 64<<20)
	}
	return retries, repo, client
}
func heldVolume(pool string, bytes int64) volumeapi.State {
	return volumeapi.State{OwnerNode: "node", CapacityBytes: bytes, CurrentCopy: &volume.CopyIdentity{PoolUID: pool, NodeName: "node"}}
}
func notified(t *testing.T, client *fake.Clientset, id string) bool {
	t.Helper()
	pvc, err := client.CoreV1().PersistentVolumeClaims("vmtest").Get(context.Background(), id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations["other"] != "keep" {
		t.Fatal("notification lost unrelated annotation")
	}
	return pvc.Annotations[retryAnnotation] != ""
}

func TestReservationRemainsChargedUntilVolumeDisappears(t *testing.T) {
	retries, repo, client := retrySetup(t, "scratch")
	state := heldVolume("pool", 128<<20)
	state.Phase = volumeapi.PhaseDeleting
	repo.volumes["old"] = state
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if notified(t, client, "scratch") {
		t.Fatal("Deleting volume released capacity prematurely")
	}
	delete(repo.volumes, "old")
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !notified(t, client, "scratch") || len(retries.snapshot()) != 0 {
		t.Fatal("released reservation did not notify and remove waiter")
	}
	before := len(client.Actions())
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != before {
		t.Fatal("notification repeated without registration")
	}
}

func TestMoveHoldAndInvalidAccountingDoNotWake(t *testing.T) {
	retries, repo, client := retrySetup(t, "scratch")
	state := heldVolume("source", 128<<20)
	state.ActiveMove = "move"
	repo.volumes["volume"] = state
	repo.moves = []volumeapi.Move{{Name: "move", Spec: volumeapi.MoveSpec{VolumeID: "volume"}, Status: volumeapi.MoveStatus{CapacityApproved: true, DestinationPoolUID: "pool", SourceCopy: state.CurrentCopy}}}
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if notified(t, client, "scratch") {
		t.Fatal("approved Move hold was ignored")
	}
	repo.volumes["broken"] = volumeapi.State{}
	if err := retries.Reconcile(context.Background()); err == nil {
		t.Fatal("invalid accounting was accepted")
	}
	if notified(t, client, "scratch") {
		t.Fatal("invalid ledger notified a waiter")
	}
}

func TestPoolGroupNodeAndFanout(t *testing.T) {
	retries, repo, client := retrySetup(t, "a", "b", "c")
	retries.Register("vmtest", "wrong-group", "wrong-group", "node", "slow", 64<<20)
	retries.Register("vmtest", "wrong-node", "wrong-node", "other", "", 64<<20)
	// One 128Mi capacity unit can wake two 64Mi requests, not all three.
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !notified(t, client, "a") || !notified(t, client, "b") || notified(t, client, "c") {
		t.Fatal("fanout exceeded the capacity hint")
	}
	if len(retries.snapshot()) != 3 {
		t.Fatal("unrelated requests were removed")
	}
	// Another independent Pool in the same node/group makes c eligible.
	repo.pools = append(repo.pools, volumeapi.Pool{Name: "second", UID: "second", NodeName: "node", CapacityLimit: "64Mi"})
	repo.volumes["first-full"] = heldVolume("pool", 128<<20)
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !notified(t, client, "c") {
		t.Fatal("another same-group Pool was not considered")
	}
}

func TestStaleClaimsAreDiscarded(t *testing.T) {
	for name, change := range map[string]func(*corev1.PersistentVolumeClaim){
		"uid":      func(p *corev1.PersistentVolumeClaim) { p.UID = "replacement" },
		"bound":    func(p *corev1.PersistentVolumeClaim) { p.Spec.VolumeName = "pv" },
		"phase":    func(p *corev1.PersistentVolumeClaim) { p.Status.Phase = corev1.ClaimBound },
		"deleting": func(p *corev1.PersistentVolumeClaim) { now := metav1.Now(); p.DeletionTimestamp = &now },
		"node":     func(p *corev1.PersistentVolumeClaim) { p.Annotations[selectedNodeAnnotation] = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			retries, _, client := retrySetup(t, "scratch")
			pvc := retryPVC("scratch")
			change(pvc)
			if _, err := client.CoreV1().PersistentVolumeClaims("vmtest").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := retries.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if notified(t, client, "scratch") || len(retries.snapshot()) != 0 {
				t.Fatal("stale claim was notified")
			}
		})
	}
	retries, _, client := retrySetup(t, "gone")
	_ = client.CoreV1().PersistentVolumeClaims("vmtest").Delete(context.Background(), "gone", metav1.DeleteOptions{})
	if err := retries.Reconcile(context.Background()); err != nil || len(retries.snapshot()) != 0 {
		t.Fatalf("missing claim: %v", err)
	}
}

func TestNotificationConflictPreservesRequestAndIdentity(t *testing.T) {
	retries, _, client := retrySetup(t, "scratch")
	client.PrependReactor("update", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		desired := action.(clienttesting.UpdateAction).GetObject().(*corev1.PersistentVolumeClaim)
		if desired.UID != "scratch" || desired.ResourceVersion != "1" {
			t.Fatal("Update lost identity/concurrency preconditions")
		}
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, "scratch", errors.New("changed"))
	})
	if err := retries.Reconcile(context.Background()); err == nil || len(retries.snapshot()) != 1 {
		t.Fatal("conflict lost waiter")
	}
}

func TestRegisterDuringNotificationIsNotLost(t *testing.T) {
	retries, _, client := retrySetup(t, "scratch")
	client.PrependReactor("update", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		retries.Register("vmtest", "scratch", "scratch", "node", "", 64<<20)
		return false, nil, nil
	})
	if err := retries.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(retries.snapshot()) != 1 {
		t.Fatal("concurrent retry registration was removed")
	}
}

func TestWaiterIndexIsBoundedAndExpires(t *testing.T) {
	retries, _, _ := retrySetup(t)
	now := time.Now()
	retries.now = func() time.Time { return now }
	retries.Register("", "bad", "bad", "node", "", 1)
	for i := 0; i < maxWaiters+1; i++ {
		id := fmt.Sprint(i)
		retries.Register("ns", id, id, "node", "", 1)
	}
	if len(retries.snapshot()) != maxWaiters {
		t.Fatal("waiter bound was not enforced")
	}
	now = now.Add(waiterLifetime)
	if len(retries.snapshot()) != 0 {
		t.Fatal("expired requests were retained")
	}
}

func TestInformerDeletionWakesWithoutCSIRequest(t *testing.T) {
	retries, repo, client := retrySetup(t, "scratch")
	repo.volumes["old"] = heldVolume("pool", 128<<20)
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": "old"}}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
	}, object)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { retries.Run(ctx, dynamicClient); close(done) }()
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		for _, action := range dynamicClient.Actions() {
			if action.GetVerb() == "watch" && action.GetResource() == volumeapi.VolumeResource {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if notified(t, client, "scratch") {
		t.Fatal("initial informer list ignored reservation")
	}
	repo.mu.Lock()
	delete(repo.volumes, "old")
	repo.mu.Unlock()
	if err := dynamicClient.Resource(volumeapi.VolumeResource).Delete(ctx, "old", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) { return notified(t, client, "scratch"), nil }); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop")
	}
}
