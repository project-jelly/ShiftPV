package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/nodeexecutor"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRollbackCleanupPendingPreservesRecoveryAndCapacity(t *testing.T) {
	for _, phase := range []string{cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
			kube := r.Client.(*fake.Clientset)
			assignJobUIDs(kube)
			move := repo.moves[0]
			spec := rollbackCleanupSpec(move, incoming)
			job := terminalVerifyingJob(t, r, kube, spec)
			before := seedCleanupObservationPhase(t, r, spec, phase)
			state := repo.volumes[move.Spec.VolumeID]
			for range 2 {
				if err := r.reconcilePersistedMove(ctx, repo.moves[0]); !errors.Is(err, nodeexecutor.ErrLegacyExecution) {
					t.Fatalf("unsettled legacy work was not reported: %v", err)
				}
				current, err := r.Cleanups.Get(ctx, spec.Authority)
				if err != nil || !reflect.DeepEqual(current, before) || !sameMoveAuthority(move.Status, repo.moves[0].Status) || repo.moves[0].UID != move.UID || repo.moves[0].Spec != move.Spec || !reflect.DeepEqual(repo.volumes[move.Spec.VolumeID], state) {
					t.Fatalf("pending rollback changed journal, hold or authority: cleanup=%+v move=%+v err=%v", current, repo.moves[0], err)
				}
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if _, err := kube.BatchV1().Jobs(job.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			settleLegacyCleanupFixture(t, r, kube, spec)
			if err := r.reconcilePersistedMove(ctx, repo.moves[0]); err != nil {
				t.Fatal(err)
			}
			current, err := r.Cleanups.Get(ctx, spec.Authority)
			if err != nil || current.Status.Phase != cleanupapi.PhaseCompleted || current.Status.AbsenceProof == nil || repo.moves[0].Status.CapacityApproved || repo.moves[0].Status.CapacityReason != recoveryCapacitySettled {
				t.Fatalf("settled rollback did not confirm absence before releasing capacity: cleanup=%+v move=%+v err=%v", current, repo.moves[0], err)
			}
			if repo.volumes[move.Spec.VolumeID].Phase != volumeapi.PhaseBlocked || repo.volumes[move.Spec.VolumeID].ActiveMove != move.Name {
				t.Fatal("cleanup settlement skipped the separate source restoration boundary")
			}
		})
	}
}
