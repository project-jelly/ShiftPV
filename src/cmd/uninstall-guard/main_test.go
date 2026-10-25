package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	uninstallcheck "github.com/project-jelly/ShiftPV/src/lifecycle/uninstall"
)

type emptyVolumeRepository struct{}

type emptyCleanupRepository struct{}

type mutableVolumeRepository struct {
	mu        sync.Mutex
	once      sync.Once
	listed    chan struct{}
	pools     []volumeapi.Pool
	removed   []string
	removeErr error
}

func (emptyCleanupRepository) List(context.Context) ([]cleanupapi.Cleanup, error) { return nil, nil }

func (emptyVolumeRepository) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return map[string]volumeapi.State{}, nil
}
func (emptyVolumeRepository) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return nil, nil
}
func (emptyVolumeRepository) ListPools(context.Context) ([]volumeapi.Pool, error) {
	return nil, nil
}
func (emptyVolumeRepository) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return nil, nil
}
func (emptyVolumeRepository) RemovePoolFinalizer(context.Context, string, string) error { return nil }

func (m *mutableVolumeRepository) ListVolumes(context.Context) (map[string]volumeapi.State, error) {
	return map[string]volumeapi.State{}, nil
}

func (m *mutableVolumeRepository) ListMoves(context.Context) ([]volumeapi.Move, error) {
	return nil, nil
}

func (m *mutableVolumeRepository) ListPools(context.Context) ([]volumeapi.Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := append([]volumeapi.Pool(nil), m.pools...)
	for index := range result {
		if result[index].Status.Inventory == nil {
			continue
		}
		inventory := *result[index].Status.Inventory
		inventory.Copies = append([]volumeapi.CopyObservation(nil), inventory.Copies...)
		result[index].Status.Inventory = &inventory
	}
	m.once.Do(func() {
		if m.listed != nil {
			close(m.listed)
		}
	})
	return result, nil
}

func (m *mutableVolumeRepository) ListPoolRegistrations(ctx context.Context) ([]volumeapi.Pool, error) {
	return m.ListPools(ctx)
}

func (m *mutableVolumeRepository) RemovePoolFinalizer(_ context.Context, name, uid string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, name+"/"+uid)
	return m.removeErr
}

func (m *mutableVolumeRepository) observeEmpty(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pools[0].Status.Inventory.ObservedAt = metav1.NewTime(at.UTC())
	m.pools[0].Status.Inventory.Copies = nil
}

func identityReleaseChecker() (*uninstallcheck.Checker, *fake.Clientset, *mutableVolumeRepository) {
	now := metav1.Now()
	repository := &mutableVolumeRepository{pools: []volumeapi.Pool{{
		Name: "lost", UID: "lost-uid", NodeName: "node", Generation: 1, DeletionTimestamp: &now,
		Status: volumeapi.PoolStatus{ObservedGeneration: 1,
			Inventory: &volumeapi.PoolInventory{ObservedAt: now, Valid: true},
			Conditions: []metav1.Condition{{Type: volumeapi.PoolConditionIdentityReleased, Status: metav1.ConditionFalse,
				ObservedGeneration: 1, Reason: "PoolStorageRestorationRequired"}},
		},
	}}}
	client := fake.NewClientset()
	checker := &uninstallcheck.Checker{Client: client, Volumes: repository, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}}
	return checker, client, repository
}

func TestCheckAfterQuiesceDoesNotInventBlockersBeforeFirstObservation(t *testing.T) {
	checker, client, _ := identityReleaseChecker()
	failure := errors.New("first API observation failed")
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, failure
	})
	_, err := checkAfterQuiesce(context.Background(), checker, time.Time{})
	if !errors.Is(err, failure) || strings.Contains(err.Error(), "last observed blockers") || strings.Contains(err.Error(), "PoolStorageRestorationRequired") {
		t.Fatalf("first failure fabricated a prior observation: %v", err)
	}
}

func TestCheckAfterQuiesceRetainsObservedBlockersWhenAPIFails(t *testing.T) {
	checker, client, _ := identityReleaseChecker()
	failure := errors.New("later API observation failed")
	calls := 0
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return false, nil, nil
		}
		return true, nil, failure
	})
	_, err := checkAfterQuiesce(context.Background(), checker, time.Time{})
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "last observed blockers:") || !strings.Contains(err.Error(), "ShiftPVPoolIdentity lost: PoolStorageRestorationRequired") {
		t.Fatalf("later API failure lost its prior observation: %v", err)
	}
}

func TestCheckAfterQuiesceRetainsObservedBlockersAtDeadline(t *testing.T) {
	checker, _, _ := identityReleaseChecker()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := checkAfterQuiesce(ctx, checker, time.Time{})
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "last observed blockers:") || !strings.Contains(err.Error(), "ShiftPVPoolIdentity lost: PoolStorageRestorationRequired") {
		t.Fatalf("deadline lost its prior observation: %v", err)
	}
}

func TestCheckAfterQuiesceAcceptsFreshReleaseAfterPriorBlocker(t *testing.T) {
	checker, client, repository := identityReleaseChecker()
	calls := 0
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 2 {
			repository.mu.Lock()
			repository.pools[0].Status.Conditions[0].Status = metav1.ConditionTrue
			repository.pools[0].Status.Conditions[0].Reason = "PoolIdentityReleased"
			repository.mu.Unlock()
		}
		return false, nil, nil
	})
	report, err := checkAfterQuiesce(context.Background(), checker, time.Time{})
	if err != nil || !report.Safe() || calls != 2 {
		t.Fatalf("prior blocker prevented a fresh safe observation: report=%+v calls=%d err=%v", report, calls, err)
	}
}

func TestRunCompletesQuiescedTeardown(t *testing.T) {
	client := uninstallClient()
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	checker := &uninstallcheck.Checker{Client: client, Volumes: emptyVolumeRepository{}, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	if err := run(ctx, checker, store, "shiftpv-lifecycle"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if granted, err := store.Granted(context.Background()); err != nil || !granted {
		t.Fatalf("Granted = %v, %v", granted, err)
	}
	if _, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "shiftpv-lifecycle", metav1.GetOptions{}); err == nil {
		t.Fatal("lifecycle validation still exists")
	}
}

func TestRunWaitsForEmptyPoolInventoryObservedAfterQuiesce(t *testing.T) {
	client := uninstallClient()
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	repository := &mutableVolumeRepository{listed: make(chan struct{}), pools: []volumeapi.Pool{{
		Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/var/lib/shiftpv", Generation: 1,
		Finalizers: []string{volumeapi.PoolProtectionFinalizer},
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{
			ObservedAt: metav1.NewTime(time.Now().Add(-time.Minute)), Valid: true,
			Copies: []volumeapi.CopyObservation{{Marker: "copy-before-quiesce.json", Present: true}},
		}},
	}}}
	checker := &uninstallcheck.Checker{
		Client: client, Volumes: repository, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system",
		Now: func() time.Time { return time.Now().Add(2 * time.Minute) },
	}
	go func() {
		<-repository.listed
		repository.observeEmpty(time.Now())
	}()

	if err := run(ctx, checker, store, "shiftpv-lifecycle"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if granted, err := store.Granted(context.Background()); err != nil || !granted {
		t.Fatalf("Granted = %v, %v", granted, err)
	}
	if len(repository.removed) != 1 || repository.removed[0] != "pool-a/pool-uid" {
		t.Fatalf("removed Pool protections = %v", repository.removed)
	}
}

func TestRunCancelsQuiesceWhenPoolProtectionReleaseFails(t *testing.T) {
	client := uninstallClient()
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	repository := &mutableVolumeRepository{removeErr: fmt.Errorf("injected finalizer release failure"), pools: []volumeapi.Pool{{
		Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/var/lib/shiftpv", Generation: 1,
		Finalizers: []string{volumeapi.PoolProtectionFinalizer},
		Status: volumeapi.PoolStatus{ObservedGeneration: 1, Inventory: &volumeapi.PoolInventory{
			ObservedAt: metav1.NewTime(time.Now().Add(time.Minute)), Valid: true,
		}},
	}}}
	checker := &uninstallcheck.Checker{
		Client: client, Volumes: repository, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system",
		Now: func() time.Time { return time.Now().Add(2 * time.Minute) },
	}

	err := run(ctx, checker, store, "shiftpv-lifecycle")
	if err == nil || !strings.Contains(err.Error(), "injected finalizer release failure") {
		t.Fatalf("run error = %v", err)
	}
	if _, quiescing, err := store.Quiescing(context.Background()); err != nil || quiescing {
		t.Fatalf("quiesce after finalizer release failure = %v, %v", quiescing, err)
	}
	if len(repository.removed) != 1 || repository.removed[0] != "pool-a/pool-uid" {
		t.Fatalf("removed Pool protections = %v", repository.removed)
	}
}

func TestRunCancelsQuiesceWhenDependenciesExist(t *testing.T) {
	client := uninstallClient()
	client.Tracker().Add(&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data"}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: uninstallcheck.DriverName, VolumeHandle: "volume"}},
	}})
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	checker := &uninstallcheck.Checker{Client: client, Volumes: emptyVolumeRepository{}, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	err := run(ctx, checker, store, "shiftpv-lifecycle")
	if err == nil || !strings.Contains(err.Error(), "PersistentVolume pv-data") {
		t.Fatalf("run error = %v", err)
	}
	if _, quiescing, err := store.Quiescing(context.Background()); err != nil || quiescing {
		t.Fatalf("quiesce after failed run = %v, %v", quiescing, err)
	}
}

func TestRunCancelsQuiesceWhenValidationRemovalFails(t *testing.T) {
	client := uninstallClient()
	client.PrependReactor("delete", "validatingwebhookconfigurations", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("injected validation delete failure")
	})
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	checker := &uninstallcheck.Checker{Client: client, Volumes: emptyVolumeRepository{}, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	err := run(ctx, checker, store, "shiftpv-lifecycle")
	if err == nil || !strings.Contains(err.Error(), "injected validation delete failure") {
		t.Fatalf("run error = %v", err)
	}
	if _, quiescing, err := store.Quiescing(context.Background()); err != nil || quiescing {
		t.Fatalf("quiesce after validation failure = %v, %v", quiescing, err)
	}
	if _, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "shiftpv-lifecycle", metav1.GetOptions{}); err != nil {
		t.Fatalf("validation configuration was lost: %v", err)
	}
}

func TestRunWithRetryCompletesAfterBlockerIsRemoved(t *testing.T) {
	client := uninstallClient()
	blocker := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data"}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: uninstallcheck.DriverName, VolumeHandle: "volume"}},
	}}
	if err := client.Tracker().Add(blocker); err != nil {
		t.Fatal(err)
	}
	checks := 0
	client.PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
		checks++
		if checks != 1 {
			return false, nil, nil
		}
		// The first snapshot must contain the blocker. Remove it for the next
		// attempt instead of racing a 20ms sleep against the 200ms ACK poll.
		err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), "", blocker.Name)
		return true, &corev1.PersistentVolumeList{Items: []corev1.PersistentVolume{*blocker.DeepCopy()}}, err
	})
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = gate.Run(ctx) }()
	checker := &uninstallcheck.Checker{Client: client, Volumes: emptyVolumeRepository{}, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}

	if err := runWithRetry(ctx, checker, store, "shiftpv-lifecycle", time.Second, time.Millisecond); err != nil {
		t.Fatalf("runWithRetry: %v", err)
	}
	if checks < 2 {
		t.Fatal("retry did not observe both blocked and clear snapshots")
	}
	if granted, err := store.Granted(context.Background()); err != nil || !granted {
		t.Fatalf("Granted = %v, %v", granted, err)
	}
}

func TestRunWithRetryValidatesDurationsAndStops(t *testing.T) {
	client := uninstallClient()
	store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
	checker := &uninstallcheck.Checker{Client: client, Volumes: emptyVolumeRepository{}, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system"}
	if err := runWithRetry(context.Background(), checker, store, "shiftpv-lifecycle", 0, time.Millisecond); err == nil {
		t.Fatal("zero attempt timeout was accepted")
	}
	if err := runWithRetry(context.Background(), checker, store, "shiftpv-lifecycle", time.Second, 0); err == nil {
		t.Fatal("zero retry interval was accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runWithRetry(ctx, checker, store, "shiftpv-lifecycle", time.Second, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "retry interrupted") {
		t.Fatalf("canceled retry error = %v", err)
	}
}

func uninstallClient() *fake.Clientset {
	driver := &storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: uninstallcheck.DriverName, UID: types.UID("driver-uid")}}
	validation := &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{
		Name: "shiftpv-lifecycle", UID: types.UID("validation-uid"),
		Labels: map[string]string{
			"app.kubernetes.io/name": "shiftpv", "app.kubernetes.io/managed-by": "shiftpv-controller", "app.kubernetes.io/component": "lifecycle-admission",
		},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: storagev1.SchemeGroupVersion.String(), Kind: "CSIDriver", Name: driver.Name, UID: driver.UID}},
	}}
	return fake.NewSimpleClientset(driver, validation)
}

// The quiesce, checker, protection release, webhook teardown and permit execute
// together. An applied grant with a lost reply must remain retryable without
// releasing either the ordinary or Terminating retired Pool tombstone.
func TestRunPreservesRetiredPoolAcrossGrantResponseLoss(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprint("terminating=", terminating), func(t *testing.T) {
			client := uninstallClient()
			store := &uninstallcheck.PermitStore{Client: client, Namespace: "shiftpv-system", Name: "shiftpv-uninstall-permit", CSIDriver: uninstallcheck.DriverName}
			gate := &uninstallcheck.QuiesceGate{Store: store, Interval: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			pool := volumeapi.Pool{Name: "lost", UID: "lost-uid", NodeName: "node", MountPath: "/mnt/lost", RetirementApproval: "lost-uid", Finalizers: []string{volumeapi.PoolProtectionFinalizer}, Status: volumeapi.PoolStatus{RegistrationApproved: true}}
			if terminating {
				at := metav1.Now()
				pool.DeletionTimestamp = &at
			}
			repository := &mutableVolumeRepository{pools: []volumeapi.Pool{pool}}
			checker := &uninstallcheck.Checker{Client: client, Volumes: repository, Cleanups: emptyCleanupRepository{}, StorageClassNames: []string{"shiftpv"}, Namespace: "shiftpv-system", PreserveRetiredPoolUIDs: []string{pool.UID}}
			var lost atomic.Bool
			client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				state := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if state.Data["state"] != "granted" || !lost.CompareAndSwap(false, true) {
					return false, nil, nil
				}
				if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), state.DeepCopy(), state.Namespace); err != nil {
					return true, nil, err
				}
				return true, nil, fmt.Errorf("injected applied grant response loss")
			})
			go func() { _ = gate.Run(ctx) }()
			if err := run(ctx, checker, store, "shiftpv-lifecycle"); err == nil || !strings.Contains(err.Error(), "injected applied grant response loss") {
				t.Fatalf("first run=%v", err)
			}
			if err := run(ctx, checker, store, "shiftpv-lifecycle"); err != nil {
				t.Fatalf("retry=%v", err)
			}
			if granted, err := store.Granted(context.Background()); err != nil || !granted {
				t.Fatalf("grant=%v,%v", granted, err)
			}
			repository.mu.Lock()
			defer repository.mu.Unlock()
			if len(repository.removed) != 0 || len(repository.pools) != 1 || repository.pools[0].UID != pool.UID || len(repository.pools[0].Finalizers) != 1 || repository.pools[0].Finalizers[0] != volumeapi.PoolProtectionFinalizer {
				t.Fatalf("retired Pool tombstone changed: removed=%v pools=%+v", repository.removed, repository.pools)
			}
		})
	}
}
