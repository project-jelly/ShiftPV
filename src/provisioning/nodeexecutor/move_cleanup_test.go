package nodeexecutor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type moveClientFixture struct {
	client  *Client
	node    *Node
	dynamic *dynamicfake.FakeDynamicClient
	kube    *kubefake.Clientset
	store   *cleanupapi.Store
	cleanup cleanupapi.Cleanup
}

func newMoveClientFixture(t *testing.T, reason string) moveClientFixture {
	t.Helper()
	client, node, dynamic, kube, state := fixture(t)
	ctx := context.Background()
	copy := *state.CurrentCopy
	op := volumeapi.MoveCleanupOperationID("move-uid")
	sourceNode := "worker-a"
	if reason == "MoveRollback" {
		copy.CopyID, copy.Role = "destination-move-uid", volume.RoleIncoming
		op = volumeapi.MoveRollbackOperationID("move-uid")
		sourceNode = "worker-b"
	}
	parent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": testID, "sourceNode": sourceNode},
	}}
	if _, err := dynamic.Resource(volumeapi.MoveResource).Create(ctx, parent, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	volumeObject, err := dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(volumeObject.Object, "move-a", "status", "activeMove")
	if _, err := dynamic.Resource(volumeapi.VolumeResource).Update(ctx, volumeObject, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	store := &cleanupapi.Store{Client: dynamic}
	cleanup, err := store.Ensure(ctx, cleanupapi.Spec{OperationID: op, Target: copy, Reason: reason,
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a", UID: "move-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	f := moveClientFixture{client: client, node: node, dynamic: dynamic, kube: kube, store: store, cleanup: cleanup}
	f.changePod(t, func(pod *corev1.Pod) { pod.Annotations[volumeapi.NodeMoveCleanupAnnotation] = "v1" })
	return f
}

func (f moveClientFixture) changePod(t *testing.T, change func(*corev1.Pod)) {
	t.Helper()
	pod, err := f.kube.CoreV1().Pods("shiftpv").Get(context.Background(), "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change(pod)
	if _, err := f.kube.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func (f moveClientFixture) current(t *testing.T) cleanupapi.Cleanup {
	t.Helper()
	current, err := f.store.Get(context.Background(), f.cleanup.Spec.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func (f moveClientFixture) assertNoJobs(t *testing.T) {
	t.Helper()
	jobs, err := f.kube.BatchV1().Jobs("shiftpv").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("unexpected helper fallback: jobs=%+v err=%v", jobs, err)
	}
}

func (f moveClientFixture) assignJobUIDs() {
	f.kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = "job-uid"
		return true, job, f.kube.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace)
	})
}

type moveClientJournal struct {
	cleanupapi.Journal
	write func(context.Context, cleanupapi.Cleanup, cleanupapi.Status) error
}

func (j moveClientJournal) UpdateStatus(ctx context.Context, current cleanupapi.Cleanup, next cleanupapi.Status) error {
	return j.write(ctx, current, next)
}

// Use the real helper to create its exact deterministic suspended Job, but lose
// the following binding write. The resident-only client must report this old
// responsibility without adopting, starting or deleting its Job.
func (f moveClientFixture) acceptedJob(t *testing.T) *batchv1.Job {
	t.Helper()
	f.assignJobUIDs()
	journal := moveClientJournal{Journal: f.store, write: func(context.Context, cleanupapi.Cleanup, cleanupapi.Status) error {
		return apierrors.NewTimeoutError("binding response unavailable", 1)
	}}
	legacy := &helperpod.Runner{Client: f.kube, Namespace: "shiftpv", Pools: f.client.Volumes, Image: "helper:test", Timeout: 10 * time.Millisecond}
	if _, err := legacy.Reclaim(context.Background(), f.cleanup, journal); err == nil {
		t.Fatal("expected interrupted Job binding")
	}
	job, err := f.kube.BatchV1().Jobs("shiftpv").Get(context.Background(), f.cleanup.Name+"-effect", metav1.GetOptions{})
	if err != nil || job.Spec.Suspend == nil || !*job.Spec.Suspend || f.current(t).Status.Executor != nil {
		t.Fatalf("accepted suspended Job setup: job=%+v err=%v", job, err)
	}
	return job
}

// This is input produced by the previous writer, never a resident-client
// execution path. Its exact binding remains immutable during compatibility observation.
func (f moveClientFixture) boundLegacyJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job := f.acceptedJob(t)
	current := f.current(t)
	next := current.Status
	next.Phase = cleanupapi.PhaseRunning
	next.Executor = &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), PodUID: "helper-pod-uid", NodeName: current.Spec.Target.NodeName}
	if err := f.store.UpdateStatus(context.Background(), current, next); err != nil {
		t.Fatal(err)
	}
	running := false
	job.Spec.Suspend = &running
	if err := f.kube.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace); err != nil {
		t.Fatal(err)
	}
	return job
}

func cleanupReceipt(current cleanupapi.Cleanup) cleanupapi.Status {
	next := current.Status
	executor := *next.Executor
	if executor.Kind == "" {
		executor.PodUID = "helper-pod-uid"
	}
	next.Executor = &executor
	next.Phase = cleanupapi.PhaseVerifying
	next.Receipt = &cleanupapi.Receipt{OperationID: current.Spec.OperationID, ExecutorUID: executor.ExecutionUID(),
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: testDigest}
	return next
}

func TestMoveCleanupCapabilityIsSeparateAndExact(t *testing.T) {
	for _, test := range []struct {
		name     string
		change   func(*corev1.Pod)
		ordinary bool
	}{
		{"old effects capability", func(p *corev1.Pod) { delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation) }, true},
		{"unknown move capability", func(p *corev1.Pod) { p.Annotations[volumeapi.NodeMoveCleanupAnnotation] = "v2" }, true},
		{"missing effects capability", func(p *corev1.Pod) { delete(p.Annotations, volumeapi.NodeEffectsAnnotation) }, false},
		{"foreign DaemonSet", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "foreign" }, false},
		{"not ready", func(p *corev1.Pod) { p.Status.Conditions = nil }, false},
		{"terminating", func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: time.Now()} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			f.changePod(t, test.change)
			found, err := f.client.Discovery.FindMoveCleanup(context.Background(), "worker-a")
			if err != nil || found != nil {
				t.Fatalf("Move discovery=%+v err=%v", found, err)
			}
			ordinary, err := f.client.Discovery.Find(context.Background(), "worker-a")
			if err != nil || (ordinary != nil) != test.ordinary {
				t.Fatalf("ordinary discovery=%+v err=%v", ordinary, err)
			}
		})
	}
}

func TestMoveCleanupBindsBothReasonsToNode(t *testing.T) {
	for _, reason := range []string{"MoveSource", "MoveRollback"} {
		t.Run(reason, func(t *testing.T) {
			f := newMoveClientFixture(t, reason)
			current, err := f.client.Reclaim(context.Background(), f.cleanup, f.store)
			if !errors.Is(err, context.DeadlineExceeded) || current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode || current.Status.Receipt != nil {
				t.Fatalf("Node binding=%+v err=%v", current, err)
			}
			request := cleanupRequest(current)
			if request.VolumeName != testID || request.VolumeUid != "volume-uid" || request.OperationId != f.cleanup.Spec.OperationID {
				t.Fatalf("RPC targets Move instead of Volume: %+v", request)
			}
			f.assertNoJobs(t)
		})
	}
}

func TestMoveCleanupMissingCapabilityOrUnboundJobNeverSelectsLegacy(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "old Node", true: "accepted suspended Job"}[accepted], func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			var job *batchv1.Job
			if accepted {
				job = f.acceptedJob(t)
			} else {
				f.changePod(t, func(p *corev1.Pod) { delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation) })
			}
			before := f.current(t)
			f.kube.ClearActions()
			_, err := f.client.Reclaim(context.Background(), f.cleanup, f.store)
			var retryable interface{ Retryable() bool }
			if err == nil || !errors.As(err, &retryable) || !retryable.Retryable() || accepted && !errors.Is(err, ErrLegacyExecution) {
				t.Fatalf("unavailable resident execution not retained for retry: %v", err)
			}
			if !reflect.DeepEqual(before, f.current(t)) {
				t.Fatal("blocked selection changed journal or intent")
			}
			for _, action := range f.kube.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("blocked selection mutated executor: %#v", action)
				}
			}
			if job != nil {
				stored, err := f.kube.BatchV1().Jobs(job.Namespace).Get(context.Background(), job.Name, metav1.GetOptions{})
				if err != nil || !reflect.DeepEqual(stored, job) {
					t.Fatalf("accepted suspended Job was adopted or changed: %+v %v", stored, err)
				}
			} else {
				f.assertNoJobs(t)
			}
		})
	}
}

func TestMoveCleanupNodeBindingNeverFallsBack(t *testing.T) {
	for _, state := range []string{"NotReady", "lost capability", "replacement old Node", "replacement capable Node"} {
		t.Run(state, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			ctx := context.Background()
			if err := f.client.bindCleanup(ctx, f.cleanup, f.store); err != nil {
				t.Fatal(err)
			}
			old := f.current(t)
			f.changePod(t, func(p *corev1.Pod) {
				switch state {
				case "NotReady":
					p.Status.Conditions = nil
				case "lost capability":
					delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation)
				default:
					p.UID = "replacement-uid"
					if state == "replacement old Node" {
						delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation)
					}
				}
			})
			if _, err := f.client.Reclaim(ctx, old, f.store); err == nil {
				t.Fatal("unproved effect completed")
			}
			current := f.current(t)
			wantUID := "pod-uid"
			if state == "replacement capable Node" {
				wantUID = "replacement-uid"
			}
			if current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode || current.Status.Executor.PodUID != wantUID || current.Status.Phase != cleanupapi.PhaseRunning {
				t.Fatalf("backend/identity changed unexpectedly: %+v", current)
			}
			if state == "replacement capable Node" {
				if err := f.store.UpdateStatus(ctx, old, cleanupReceipt(old)); !errors.Is(err, cleanupapi.ErrConflict) {
					t.Fatalf("old executor receipt accepted: %v", err)
				}
			}
			f.assertNoJobs(t)
		})
	}
}

func TestMoveCleanupBindingResponseLossRequiresReobservation(t *testing.T) {
	f := newMoveClientFixture(t, "MoveRollback")
	ctx := context.Background()
	writes := 0
	journal := moveClientJournal{Journal: f.store, write: func(ctx context.Context, current cleanupapi.Cleanup, next cleanupapi.Status) error {
		writes++
		if err := f.store.UpdateStatus(ctx, current, next); err != nil {
			return err
		}
		return apierrors.NewTimeoutError("Node binding stored; response lost", 1)
	}}
	if _, err := f.client.Reclaim(ctx, f.cleanup, journal); !apierrors.IsTimeout(err) {
		t.Fatalf("lost write=%v", err)
	}
	bound := f.current(t)
	if writes != 1 || bound.Status.Executor == nil || bound.Status.Executor.Kind != cleanupapi.ExecutorNode || bound.Status.Receipt != nil {
		t.Fatalf("binding not durable: %+v writes=%d", bound, writes)
	}
	if _, err := f.client.Reclaim(ctx, f.cleanup, f.store); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry=%v", err)
	}
	if !reflect.DeepEqual(bound, f.current(t)) {
		t.Fatal("retry changed stable binding")
	}
	f.assertNoJobs(t)
}

func TestMoveCleanupFreshObservationRejectsChangedJournal(t *testing.T) {
	for _, change := range []string{"spec", "parent UID", "needs review", "receipt"} {
		t.Run(change, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			reply := &cleanupRPCReply{f: f}
			attachCleanupRPC(t, f, reply)
			ctx := context.Background()
			journal := moveClientJournal{Journal: f.store, write: func(ctx context.Context, current cleanupapi.Cleanup, next cleanupapi.Status) error {
				if err := f.store.UpdateStatus(ctx, current, next); err != nil {
					return err
				}
				stored := f.current(t)
				if change == "receipt" {
					return f.store.UpdateStatus(ctx, stored, cleanupReceipt(stored))
				}
				if change == "needs review" {
					next = stored.Status
					next.Phase = cleanupapi.PhaseNeedsReview
					return f.store.UpdateStatus(ctx, stored, next)
				}
				parent, err := f.dynamic.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if change == "spec" {
					_ = unstructured.SetNestedField(parent.Object, "changed-operation", "status", "cleanup", "spec", "operationID")
				} else {
					parent.SetUID("replacement-move")
				}
				_, err = f.dynamic.Resource(volumeapi.MoveResource).Update(ctx, parent, metav1.UpdateOptions{})
				return err
			}}
			current, err := f.client.Reclaim(ctx, f.cleanup, journal)
			if change == "receipt" {
				if err != nil || current.Status.Phase != cleanupapi.PhaseVerifying {
					t.Fatalf("receipt not adopted: %+v %v", current, err)
				}
			} else if !errors.Is(err, cleanupapi.ErrConflict) {
				t.Fatalf("changed journal accepted: %+v %v", current, err)
			}
			if reply.calls.Load() != 0 {
				t.Fatal("post-binding journal change reached the executor")
			}
			f.assertNoJobs(t)
		})
	}
}

func TestMoveCleanupFreshLegacyWinnerIsNotInvokedAsNode(t *testing.T) {
	f := newMoveClientFixture(t, "MoveSource")
	ctx := context.Background()
	job := f.acceptedJob(t)
	if err := f.kube.BatchV1().Jobs("shiftpv").Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	f.changePod(t, func(p *corev1.Pod) { delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation) })
	f.kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if err := f.kube.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job.DeepCopy(), job.Namespace); err != nil {
			return true, nil, err
		}
		err := f.store.UpdateStatus(ctx, f.cleanup, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &cleanupapi.Executor{JobName: job.Name, JobUID: string(job.UID), NodeName: "worker-a"}})
		return false, nil, err
	})
	if _, err := f.client.Reclaim(ctx, f.cleanup, f.store); err == nil {
		t.Fatal("concurrent helper winner did not block")
	}
	if _, err := f.client.Reclaim(ctx, f.cleanup, f.store); !errors.Is(err, ErrLegacyExecution) {
		t.Fatalf("fresh legacy binding not reported: %v", err)
	}
	current := f.current(t)
	if current.Status.Executor == nil || current.Status.Executor.Kind != "" || current.Status.Executor.JobUID != string(job.UID) || current.Status.Phase != cleanupapi.PhaseRunning {
		t.Fatalf("legacy winner overwritten: %+v", current)
	}
}

func TestMoveCleanupLegacyReceiptCannotAdvanceAfterExecutorTermination(t *testing.T) {
	f := newMoveClientFixture(t, "MoveSource")
	ctx := context.Background()
	job := f.boundLegacyJob(t)
	current := f.current(t)
	if err := f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
		t.Fatal(err)
	}
	verified := f.current(t)
	f.kube.ClearActions()
	for _, terminal := range []bool{false, true} {
		if terminal {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if err := f.kube.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace); err != nil {
				t.Fatal(err)
			}
		}
		result, err := f.client.Reclaim(ctx, f.cleanup, f.store)
		if !errors.Is(err, ErrLegacyExecution) || !reflect.DeepEqual(result, verified) || !reflect.DeepEqual(verified, f.current(t)) {
			t.Fatalf("candidate settled old receipt (terminal=%v): %+v %v", terminal, result, err)
		}
	}
	for _, action := range f.kube.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("legacy observation wrote executor: %#v", action)
		}
	}
}
