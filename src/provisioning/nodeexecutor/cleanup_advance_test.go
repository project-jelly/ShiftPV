package nodeexecutor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestCleanupAdvanceNodeWaitsForWatchReceiptWithoutRPC(t *testing.T) {
	for _, reason := range []string{"MoveSource", "MoveRollback"} {
		t.Run(reason, func(t *testing.T) {
			f := newMoveClientFixture(t, reason)
			reply := &cleanupRPCReply{f: f}
			attachCleanupRPC(t, f, reply)
			ctx := context.Background()
			current, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, f.store)
			if err != nil || ready || current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode || current.Status.Phase != cleanupapi.PhaseRunning {
				t.Fatalf("Node advance did not hand off to watch: %+v ready=%v err=%v", current, ready, err)
			}
			again, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, f.store)
			if err != nil || ready || !reflect.DeepEqual(again, current) || reply.calls.Load() != 0 {
				t.Fatalf("pending advance invoked RPC or changed binding: %+v ready=%v err=%v calls=%d", again, ready, err, reply.calls.Load())
			}
			if err := f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
				t.Fatal(err)
			}
			durable := f.current(t)
			observed, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, f.store)
			if err != nil || !ready || !reflect.DeepEqual(observed, durable) || reply.calls.Load() != 0 {
				t.Fatalf("exact durable Node receipt not adopted: %+v ready=%v err=%v calls=%d", observed, ready, err, reply.calls.Load())
			}
			f.assertNoJobs(t)
		})
	}
}

func TestCleanupAdvanceBindingResponseLossDoesNotReplayOrFallback(t *testing.T) {
	f := newMoveClientFixture(t, "MoveRollback")
	ctx := context.Background()
	writes := 0
	journal := moveClientJournal{Journal: f.store, write: func(ctx context.Context, current cleanupapi.Cleanup, next cleanupapi.Status) error {
		writes++
		if err := f.store.UpdateStatus(ctx, current, next); err != nil {
			return err
		}
		return apierrors.NewTimeoutError("binding saved, response lost", 1)
	}}
	if _, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, journal); ready || !apierrors.IsTimeout(err) {
		t.Fatalf("lost response did not stop advance: ready=%v err=%v", ready, err)
	}
	bound := f.current(t)
	observed, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, journal)
	if err != nil || ready || writes != 1 || !reflect.DeepEqual(observed, bound) {
		t.Fatalf("retry rewrote binding: %+v ready=%v writes=%d err=%v", observed, ready, writes, err)
	}
	f.changePod(t, func(p *corev1.Pod) {
		p.UID = "old-capability-replacement"
		delete(p.Annotations, volumeapi.NodeMoveCleanupAnnotation)
	})
	if _, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, f.store); ready || err == nil {
		t.Fatalf("Node-bound work fell back for unsupported replacement: ready=%v err=%v", ready, err)
	}
	if !reflect.DeepEqual(bound, f.current(t)) {
		t.Fatal("unsupported replacement changed the durable binding")
	}
	f.assertNoJobs(t)
}

func TestCleanupAdvancePreservesLegacyReceiptAndFenceAsCompatibilityBlocker(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		t.Run(phase, func(t *testing.T) {
			f := newMoveClientFixture(t, "MoveSource")
			ctx := context.Background()
			job := f.boundLegacyJob(t)
			current := f.current(t)
			if err := f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
				t.Fatal(err)
			}
			current = f.current(t)
			if phase == cleanupapi.PhaseConfirmingAbsence {
				next := current.Status
				next.Phase = phase
				next.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "old-fence", PoolName: current.Spec.Target.PoolName, PoolUID: current.Spec.Target.PoolUID, RequiredGeneration: 2}
				if err := f.store.UpdateStatus(ctx, current, next); err != nil {
					t.Fatal(err)
				}
				current = f.current(t)
			}
			f.kube.ClearActions()
			for _, terminal := range []bool{false, true} {
				if terminal {
					job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
					if err := f.kube.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace); err != nil {
						t.Fatal(err)
					}
				}
				observed, ready, err := f.client.AdvanceReclaim(ctx, f.cleanup, f.store)
				if !errors.Is(err, ErrLegacyExecution) || ready || !reflect.DeepEqual(observed, current) || !reflect.DeepEqual(f.current(t), current) {
					t.Fatalf("candidate advanced old receipt/fence (Job terminal=%v): current=%+v ready=%v error=%v", terminal, observed, ready, err)
				}
			}
			for _, action := range f.kube.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("compatibility observation mutated executor: %#v", action)
				}
			}
		})
	}
}
