package helperpod

import (
	"context"
	"errors"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestCleanupRunnerPreservesNodeWinner(t *testing.T) {
	for _, boundary := range []string{"before call", "after Job create", "after accepted create response loss", "before Job binding"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			store, stale := moveCleanupBackendFixture(t)
			client := fake.NewClientset()
			runner := cleanupBackendRunner(client, stale)
			var winner cleanupapi.Cleanup
			bindNode := func() error {
				if err := store.UpdateStatus(ctx, stale, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: cleanupNodeExecutor(stale)}); err != nil {
					return err
				}
				var err error
				winner, err = store.Get(ctx, stale.Spec.Authority)
				return err
			}
			if boundary == "before call" {
				if err := bindNode(); err != nil {
					t.Fatal(err)
				}
			}
			creates := 0
			client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				creates++
				job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				job.UID = "job-uid"
				if err := client.Tracker().Add(job); err != nil {
					return true, nil, err
				}
				if boundary != "before Job binding" {
					if err := bindNode(); err != nil {
						return true, nil, err
					}
				}
				if boundary == "after accepted create response loss" {
					return true, nil, apierrors.NewTimeoutError("accepted suspended Job, response lost", 1)
				}
				return true, job, nil
			})
			journal := cleanupBackendJournal{Journal: store}
			if boundary == "before Job binding" {
				journal.write = func(ctx context.Context, expected cleanupapi.Cleanup, next cleanupapi.Status) error {
					if err := bindNode(); err != nil {
						return err
					}
					return store.UpdateStatus(ctx, expected, next)
				}
			}
			if _, err := runner.Reclaim(ctx, stale, journal); !errors.Is(err, cleanupapi.ErrConflict) {
				t.Fatalf("Node winner was not returned as a backend conflict: %v", err)
			}
			current, err := store.Get(ctx, stale.Spec.Authority)
			if err != nil || !reflect.DeepEqual(current, winner) {
				t.Fatalf("helper changed Node journal: current=%+v winner=%+v err=%v", current, winner, err)
			}
			if _, err := runner.Reclaim(ctx, current, store); !errors.Is(err, cleanupapi.ErrConflict) {
				t.Fatalf("fresh Node binding entered helper execution: %v", err)
			}
			wantCreates := 1
			if boundary == "before call" {
				wantCreates = 0
			}
			jobs, err := client.BatchV1().Jobs(runner.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != wantCreates || creates != wantCreates {
				t.Fatalf("unexpected helper creation: creates=%d jobs=%+v err=%v", creates, jobs, err)
			}
			for _, job := range jobs.Items {
				owner := metav1.GetControllerOf(&job)
				if job.UID != "job-uid" || job.Spec.Suspend == nil || !*job.Spec.Suspend || owner == nil ||
					owner.APIVersion != "shiftpv.io/v1alpha1" || owner.Kind != "ShiftPVMove" || owner.Name != stale.Spec.Authority.Name || string(owner.UID) != stale.UID {
					t.Fatalf("loser Job is not suspended and bound to its Move for owner GC: %+v", job)
				}
			}
			for _, action := range client.Actions() {
				if (action.GetVerb() == "update" || action.GetVerb() == "patch") && action.GetResource().Resource == "jobs" ||
					action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
					t.Fatalf("Node winner allowed helper execution action: %+v", action)
				}
			}
		})
	}
}

func TestCleanupRunnerPreservesJobWinner(t *testing.T) {
	ctx := context.Background()
	store, stale := moveCleanupBackendFixture(t)
	client := fake.NewClientset()
	runner := cleanupBackendRunner(client, stale)
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = "job-uid"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
		return true, job, client.Tracker().Add(job)
	})
	writeReceiptOnFirstJobGet(client, store, stale)
	var nodeBindErr error
	journal := cleanupBackendJournal{Journal: store, write: func(ctx context.Context, expected cleanupapi.Cleanup, next cleanupapi.Status) error {
		if err := store.UpdateStatus(ctx, expected, next); err != nil {
			return err
		}
		nodeBindErr = store.UpdateStatus(ctx, stale, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: cleanupNodeExecutor(stale)})
		return nil
	}}
	result, err := runner.Reclaim(ctx, stale, journal)
	if err != nil || !errors.Is(nodeBindErr, cleanupapi.ErrConflict) || result.Status.Phase != cleanupapi.PhaseVerifying ||
		result.Status.Executor == nil || result.Status.Executor.Kind != "" || result.Status.Executor.JobUID != "job-uid" ||
		result.Status.Receipt == nil || result.Status.Receipt.ExecutorUID != "job-uid" {
		t.Fatalf("Job winner did not retain backend and receipt: result=%+v bindErr=%v err=%v", result, nodeBindErr, err)
	}
	job, err := client.BatchV1().Jobs(runner.Namespace).Get(ctx, stale.Name+"-effect", metav1.GetOptions{})
	if err != nil || job.Spec.Suspend == nil || *job.Spec.Suspend {
		t.Fatalf("bound Job did not start: job=%+v err=%v", job, err)
	}
}

type cleanupBackendJournal struct {
	cleanupapi.Journal
	write func(context.Context, cleanupapi.Cleanup, cleanupapi.Status) error
}

func (j cleanupBackendJournal) UpdateStatus(ctx context.Context, expected cleanupapi.Cleanup, next cleanupapi.Status) error {
	if j.write != nil {
		return j.write(ctx, expected, next)
	}
	return j.Journal.UpdateStatus(ctx, expected, next)
}

func moveCleanupBackendFixture(t *testing.T) (*cleanupapi.Store, cleanupapi.Cleanup) {
	t.Helper()
	store, initial := cleanupFixture(t)
	parent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "generation": int64(1), "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec":     map[string]any{"volumeID": initial.Spec.Target.VolumeID, "sourceNode": initial.Spec.Target.NodeName},
	}}
	if _, err := store.Client.Resource(volumeapi.MoveResource).Create(context.Background(), parent, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	cleanup, err := store.Ensure(context.Background(), cleanupapi.Spec{OperationID: volumeapi.MoveCleanupOperationID("move-uid"),
		Target: initial.Spec.Target, Reason: "MoveSource", Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a", UID: "move-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	return store, cleanup
}

func cleanupNodeExecutor(cleanup cleanupapi.Cleanup) *cleanupapi.Executor {
	return &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: "shiftpv-system", PodName: "node-a", PodUID: "node-pod-uid", NodeName: cleanup.Spec.Target.NodeName}
}

func cleanupBackendRunner(client *fake.Clientset, cleanup cleanupapi.Cleanup) Runner {
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: readyCleanupPool(volumeapi.Pool{Name: cleanup.Spec.Target.PoolName, UID: cleanup.Spec.Target.PoolUID,
		NodeName: cleanup.Spec.Target.NodeName, MountPath: "/mnt/shiftpv"})}
	return runner
}
