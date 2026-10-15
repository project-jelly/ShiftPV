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
	mu         sync.Mutex
	pools      []volumeapi.Pool
	volumes    map[string]volumeapi.State
	moves      []volumeapi.Move
	err        error
	readyReads int
}

func (r *retryRepository) ReadyPools(context.Context) ([]volumeapi.Pool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readyReads++
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

func TestPoolStatusRecoveryWakesObservationWaitWithoutCSIRequest(t *testing.T) {
	retries, repo, client := retrySetup(t)
	if _, err := client.CoreV1().PersistentVolumeClaims("vmtest").Create(context.Background(), retryPVC("scratch"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	healthy := repo.pools
	repo.pools = nil
	retries.RegisterObservation("vmtest", "scratch", "scratch", "node", "default", volumeapi.Pool{})
	pool := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": "pool", "uid": "pool", "resourceVersion": "1"}}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
	}, pool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { retries.Run(ctx, dynamicClient); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("retry watcher did not stop")
		}
	}()
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 3*time.Second, true, func(context.Context) (bool, error) {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.readyReads > 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if notified(t, client, "scratch") {
		t.Fatal("unready Pool notified a claim")
	}
	repo.mu.Lock()
	repo.pools = healthy
	// No spare quota: observation recovery must not double-charge a hold or
	// depend on the logical capacity fan-out used for new reservations.
	repo.volumes["full"] = heldVolume("pool", 128<<20)
	repo.mu.Unlock()
	pool.SetResourceVersion("2")
	pool.Object["status"] = map[string]any{"observedGeneration": int64(1)}
	if _, err := dynamicClient.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 3*time.Second, true, func(context.Context) (bool, error) {
		return notified(t, client, "scratch"), nil
	}); err != nil {
		t.Fatal("Pool status recovery did not wake the claim before the 30s fallback", err)
	}
}

func TestObservationWaitRetainsClaimIdentityGuards(t *testing.T) {
	for _, change := range []func(*corev1.PersistentVolumeClaim){
		func(p *corev1.PersistentVolumeClaim) { p.UID = "replacement" },
		func(p *corev1.PersistentVolumeClaim) { p.Annotations[selectedNodeAnnotation] = "other" },
		func(p *corev1.PersistentVolumeClaim) { p.Spec.VolumeName = "bound" },
	} {
		retries, _, client := retrySetup(t, "scratch")
		retries.RegisterObservation("vmtest", "scratch", "scratch", "node", "default", volumeapi.Pool{})
		pvc := retryPVC("scratch")
		change(pvc)
		if _, err := client.CoreV1().PersistentVolumeClaims("vmtest").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := retries.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if notified(t, client, "scratch") || len(retries.snapshot()) != 0 {
			t.Fatal("observation notification bypassed claim identity guards")
		}
	}
}

func TestFailedProbeWaitRequiresNewObservation(t *testing.T) {
	for _, recovery := range []string{"fresh probe", "clock moves backward", "new generation", "another pool"} {
		t.Run(recovery, func(t *testing.T) {
			retries, repo, client := retrySetup(t, "scratch")
			repo.pools[0].Generation = 3
			repo.pools[0].Status.LastProbeTime = metav1.NewTime(time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
			observed := repo.pools[0]
			retries.RegisterObservation("vmtest", "scratch", "scratch", "node", "default", observed)
			if err := retries.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if notified(t, client, "scratch") {
				t.Fatal("unchanged Ready snapshot caused an immediate failed-probe retry")
			}
			switch recovery {
			case "fresh probe":
				repo.pools[0].Status.LastProbeTime = metav1.NewTime(observed.Status.LastProbeTime.Add(time.Second))
			case "clock moves backward":
				repo.pools[0].Status.LastProbeTime = metav1.NewTime(observed.Status.LastProbeTime.Add(-time.Second))
			case "new generation":
				repo.pools[0].Generation++
			case "another pool":
				repo.pools = append(repo.pools, volumeapi.Pool{Name: "peer", UID: "peer", NodeName: "node", CapacityLimit: "128Mi"})
			}
			if err := retries.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !notified(t, client, "scratch") {
				t.Fatal("new successful observation did not notify the waiting claim")
			}
		})
	}
}
