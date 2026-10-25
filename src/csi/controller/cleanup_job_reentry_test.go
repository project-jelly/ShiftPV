package controller

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestDeleteReentryRequiresCompatibleWriterBeforeConsumingLegacyHistory(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		for _, executor := range []string{"running Job", "absent Job with owned Pod", "absent Job with relabeled owned Pod", "absent Job with orphaned receipt Pod"} {
			t.Run(phase+"/"+executor, func(t *testing.T) {
				ctx := context.Background()
				service, kube, journal, job := newDeleteJobReceiptFixture(t)
				if phase == cleanupapi.PhaseConfirmingAbsence {
					// Model the older controller's premature fence. Its scan already
					// proves absence, but executor settlement is still outstanding.
					var err error
					journal, _, err = service.Cleanups.ReconcileAbsence(ctx, journal)
					if err != nil || journal.Status.Phase != phase || journal.Status.AbsenceProof == nil {
						t.Fatalf("prepare historical fence: %+v %v", journal, err)
					}
				}
				if executor != "running Job" {
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "residual-cleanup", Namespace: service.Namespace,
						UID: "cleanup-pod-uid", Labels: job.Spec.Template.Labels,
						OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}}}
					if executor == "absent Job with relabeled owned Pod" || executor == "absent Job with orphaned receipt Pod" {
						pod.Labels = nil // Mutable labels cannot prove this executor is gone.
					}
					if executor == "absent Job with orphaned receipt Pod" {
						pod.OwnerReferences = nil // The journal still names the exact Pod UID.
					}
					if _, err := kube.CoreV1().Pods(service.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
					if err := kube.BatchV1().Jobs(service.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				beforePool := deleteJobPool(t, service, journal)
				kube.ClearActions()
				request := &csi.DeleteVolumeRequest{VolumeId: journal.Spec.Target.VolumeID}
				if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
					t.Fatalf("unsettled executor must remain retryable: %v", err)
				}
				current, err := service.Cleanups.Get(ctx, journal.Spec.Authority)
				if err != nil || !reflect.DeepEqual(current.Status, journal.Status) {
					t.Fatalf("unsettled executor changed journal/fence: before=%+v after=%+v err=%v", journal.Status, current.Status, err)
				}
				state, err := service.Volumes.Get(ctx, request.VolumeId)
				if err != nil || state.UID != journal.Spec.Target.VolumeUID || state.Phase != volumeapi.PhaseDeleting || state.CapacityBytes != 64<<20 {
					t.Fatalf("executor wait released parent/capacity: %+v %v", state, err)
				}
				if after := deleteJobPool(t, service, journal); !reflect.DeepEqual(beforePool.Object, after.Object) {
					t.Fatal("executor wait requested another absence scan")
				}
				assertNoDeleteJobWrites(t, kube)
				if executor == "running Job" {
					job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
					if _, err := kube.BatchV1().Jobs(service.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
				} else if err := kube.CoreV1().Pods(service.Namespace).Delete(ctx, "residual-cleanup", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				// Process completion alone cannot authorize this candidate to
				// advance the old journal. The compatible writer closes it first.
				if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
					t.Fatalf("candidate adopted unsettled legacy journal: %v", err)
				}
				journal = settleDeleteLegacyHistory(t, service, kube, journal)
				kube.ClearActions()
				// This method is the real deletion settlement path; retain the
				// parent briefly so the completed proof can be inspected.
				if err := service.settleVolumeCleanup(ctx, request.VolumeId, state); err != nil {
					t.Fatalf("settled executor did not converge: %v", err)
				}
				settled, err := service.Cleanups.Get(ctx, journal.Spec.Authority)
				if err != nil || settled.Status.Phase != cleanupapi.PhaseCompleted ||
					!reflect.DeepEqual(settled.Status.Executor, journal.Status.Executor) || !reflect.DeepEqual(settled.Status.Receipt, journal.Status.Receipt) {
					t.Fatalf("settlement changed executor/receipt: %+v %v", settled, err)
				}
				if phase == cleanupapi.PhaseConfirmingAbsence {
					proof, original := settled.Status.AbsenceProof, journal.Status.AbsenceProof
					if proof.RequestID != original.RequestID || proof.RequiredGeneration != original.RequiredGeneration ||
						!reflect.DeepEqual(beforePool.Object, deleteJobPool(t, service, journal).Object) {
						t.Fatal("historical post-receipt fence was replaced")
					}
				}
				if _, err := service.DeleteVolume(ctx, request); err != nil {
					t.Fatal(err)
				}
				if _, err := service.Volumes.Get(ctx, request.VolumeId); !apierrors.IsNotFound(err) {
					t.Fatalf("settled parent remains: %v", err)
				}
				assertNoDeleteJobWrites(t, kube)
			})
		}
	}
}

func TestCompletedLegacyDeleteKeepsParentWhileReceiptPodRemains(t *testing.T) {
	for _, missingJob := range []bool{false, true} {
		name := "completed Job with live receipt Pod"
		if missingJob {
			name = "absent Job with live receipt Pod"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			service, kube, journal, job := newDeleteJobReceiptFixture(t)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if _, err := kube.BatchV1().Jobs(service.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			journal = settleDeleteLegacyHistory(t, service, kube, journal)
			// Completed evidence alone cannot rule out an executor still alive
			// after a legacy writer's observation. Even a relabelled orphan is
			// found by the exact receipt-writing Pod UID recorded in the journal.
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-receipt-pod", Namespace: service.Namespace, UID: types.UID(journal.Status.Executor.PodUID)}, Spec: corev1.PodSpec{NodeName: journal.Spec.Target.NodeName, Containers: []corev1.Container{{Name: "helper", Image: "helper"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			if _, err := kube.CoreV1().Pods(service.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if missingJob {
				if err := kube.BatchV1().Jobs(service.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			beforePool := deleteJobPool(t, service, journal)
			api := service.Cleanups.Client.(*dynamicfake.FakeDynamicClient)
			kube.ClearActions()
			api.ClearActions()
			request := &csi.DeleteVolumeRequest{VolumeId: journal.Spec.Target.VolumeID}
			if _, err := service.DeleteVolume(ctx, request); status.Code(err) != codes.Unavailable {
				t.Fatalf("live legacy executor released completed parent: %v", err)
			}
			state, err := service.Volumes.Get(ctx, request.VolumeId)
			if err != nil || state.UID != journal.Spec.Target.VolumeUID || state.Phase != volumeapi.PhaseDeleting || state.CapacityBytes != 64<<20 || !slices.Contains(state.Finalizers, volumeapi.VolumeProtectionFinalizer) {
				t.Fatalf("live executor lost parent/hold/protection: %+v %v", state, err)
			}
			current, err := service.Cleanups.Get(ctx, journal.Spec.Authority)
			if err != nil || !reflect.DeepEqual(current, journal) {
				t.Fatalf("candidate changed completed legacy proof: before=%+v after=%+v err=%v", journal, current, err)
			}
			if !reflect.DeepEqual(beforePool, deleteJobPool(t, service, journal)) {
				t.Fatal("live executor observation opened another absence fence")
			}
			for _, action := range api.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatalf("live executor allowed parent delete: %#v", action)
				}
			}
			assertNoExecutorWrites(t, kube)
			if err := kube.CoreV1().Pods(service.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil {
				t.Fatal(err)
			}
			kube.ClearActions()
			if _, err := service.DeleteVolume(ctx, request); err != nil {
				t.Fatalf("exact executor absence did not permit settled parent removal: %v", err)
			}
			if _, err := service.Volumes.Get(ctx, request.VolumeId); !apierrors.IsNotFound(err) {
				t.Fatalf("settled parent remained: %v", err)
			}
			if !reflect.DeepEqual(beforePool, deleteJobPool(t, service, journal)) {
				t.Fatal("completed history requested a new absence fence")
			}
			assertNoExecutorWrites(t, kube)
		})
	}
}

func settleDeleteLegacyHistory(t *testing.T, service *Service, kube *fake.Clientset, journal cleanupapi.Cleanup) cleanupapi.Cleanup {
	t.Helper()
	ctx := context.Background()
	// Seed the previous writer's absence settlement after its Job has finished.
	for range 2 {
		var err error
		journal, _, err = service.Cleanups.ReconcileAbsence(ctx, journal)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanupapi.ValidateSettled(journal); err != nil {
		t.Fatal(err)
	}
	return journal
}

func assertNoExecutorWrites(t *testing.T, kube *fake.Clientset) {
	t.Helper()
	for _, action := range kube.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("candidate mutated legacy executor: %#v", action)
		}
	}
}

func newDeleteJobReceiptFixture(t *testing.T) (*Service, *fake.Clientset, cleanupapi.Cleanup, *batchv1.Job) {
	t.Helper()
	ctx := context.Background()
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
	parent := cleanupParentVolume(copy.VolumeID, copy)
	identity, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&copy)
	parent.Object["status"] = map[string]any{"phase": volumeapi.PhaseReady, "ownerNode": copy.NodeName, "currentCopy": identity}
	pool := cleanupPool(copy, true)
	pool.Object["spec"].(map[string]any)["mountPath"] = "/mnt/pool"
	pool.Object["spec"].(map[string]any)["capacity"] = map[string]any{"limit": "1Gi"}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(pool.Object, now, "status", "lastProbeTime")
	conditions := []any{}
	for _, kind := range []string{volumeapi.PoolConditionAccessible, volumeapi.PoolConditionWritable, volumeapi.PoolConditionCapacityReadable} {
		conditions = append(conditions, map[string]any{"type": kind, "status": "True", "reason": "ProbeSucceeded", "observedGeneration": pool.GetGeneration(), "lastTransitionTime": now})
	}
	_ = unstructured.SetNestedSlice(pool.Object, conditions, "status", "conditions")
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), cleanupListKinds, parent, pool)
	// A requested scan returns a newer generation and complete empty inventory.
	api.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			object.SetGeneration(object.GetGeneration() + 1)
			_ = unstructured.SetNestedField(object.Object, object.GetGeneration(), "status", "observedGeneration")
			_ = unstructured.SetNestedSlice(object.Object, []any{}, "status", "inventory", "copies")
		}
		return false, nil, nil
	})
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		action.(ktesting.CreateAction).GetObject().(*batchv1.Job).UID = types.UID("cleanup-job-uid")
		return false, nil, nil
	})
	registry := &volumeapi.Registry{Client: api}
	store := &cleanupapi.Store{Client: api}
	helper := &helperpod.Runner{Client: kube, Namespace: "shiftpv-system", Pools: registry, Image: "helper", Timeout: 10 * time.Millisecond}
	operator := &nodeexecutor.Client{Volumes: registry, Inspector: &helperpod.Inspector{Client: helper.Client, Namespace: helper.Namespace}, Timeout: helper.Timeout}
	service := &Service{Client: kube, Namespace: helper.Namespace, Operator: &fakeDirectoryOperator{}, Volumes: registry,
		Cleanups: store, CleanupOperator: operator, CleanupAbsenceWait: time.Second}
	state, err := registry.BeginDelete(ctx, copy.VolumeID, copy.VolumeUID, copy)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := store.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: copy, Reason: "VolumeDelete",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: copy.VolumeID, UID: copy.VolumeUID}})
	if err != nil {
		t.Fatal(err)
	}
	seedLegacyCleanupJob(t, store, kube, journal, helper.Namespace)
	journal, err = store.Get(ctx, journal.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	next := journal.Status
	executor := *next.Executor
	executor.PodUID = "cleanup-pod-uid"
	next.Executor = &executor
	if err := store.UpdateStatus(ctx, journal, next); err != nil {
		t.Fatal(err)
	}
	journal, err = store.Get(ctx, journal.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	next = journal.Status
	next.Phase = cleanupapi.PhaseVerifying
	next.Receipt = &cleanupapi.Receipt{OperationID: journal.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: now, Retired: true, Purged: true, LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	if err := store.UpdateStatus(ctx, journal, next); err != nil {
		t.Fatal(err)
	}
	journal, err = store.Get(ctx, journal.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	job, err := kube.BatchV1().Jobs(service.Namespace).Get(ctx, executor.JobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return service, kube, journal, job
}

func deleteJobPool(t *testing.T, service *Service, journal cleanupapi.Cleanup) *unstructured.Unstructured {
	t.Helper()
	pool, err := service.Cleanups.Client.Resource(volumeapi.PoolResource).Get(context.Background(), journal.Spec.Target.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func assertNoDeleteJobWrites(t *testing.T, kube *fake.Clientset) {
	t.Helper()
	for _, action := range kube.Actions() {
		if action.GetResource().Resource == "jobs" && action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("receipt re-entry changed the executor: %s Job", action.GetVerb())
		}
	}
}
