package controller

import (
	"context"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestRetiredClaimSettlesMoveWithoutRestoringService(t *testing.T) {
	for _, owner := range []string{"source", "destination"} {
		for _, reuse := range []bool{false, true} {
			name := owner + "/absent"
			if reuse {
				name = owner + "/reused-name"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				r, repo, client := recoveryFixture(t, owner)
				for index := range repo.pools {
					repo.pools[index].Generation = 1
					repo.pools[index].Finalizers = []string{volumeapi.PoolProtectionFinalizer}
				}
				move := repo.moves[0]
				move.Spec.Recovery = "" // No operator ResumeOwner request.
				move.Status.Phase = "Copying"
				state := repo.volumes[move.Spec.VolumeID]
				state.Phase = volumeapi.PhaseMoving
				if owner == "destination" {
					move.Status.Phase, state.Phase = "WaitingForDestinationPublish", volumeapi.PhaseReady
				}
				state.PublishedNodes = nil
				repo.moves[0], repo.volumes[move.Spec.VolumeID] = move, state
				original := state
				if err := client.CoreV1().Pods("workload").Delete(ctx, "consumer", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				claim, err := client.CoreV1().PersistentVolumeClaims("workload").Get(ctx, "claim", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				var replacement *corev1.PersistentVolumeClaim
				if reuse {
					replacement = claim.DeepCopy()
					replacement.UID, replacement.ResourceVersion = "new-request-uid", ""
					replacement.Spec.VolumeName = "new-pv"
					if _, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
				}
				// Settlement does not need to schedule a new workload on its owner.
				node, _ := client.CoreV1().Nodes().Get(ctx, owner, metav1.GetOptions{})
				node.Spec.Unschedulable = true
				if _, err := client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				var lastErr error
				for cycle := 0; cycle < 32; cycle++ {
					restarted := newTestReconciler(client, repo, withCleanupsFrom(r), func(next *Reconciler) {
						next.ServiceAccountName = r.ServiceAccountName
					})
					lastErr = restarted.ReconcileAll(ctx)
					finishRecoveryJobs(t, client, batchv1.JobComplete)
					// A scanner acknowledges only the generation already requested by
					// the preceding controller pass, including across every restart.
					pool := &repo.pools[1]
					pool.Status.ObservedGeneration = pool.Generation
					pool.Status.Conditions = testBackingConditions(pool.Generation)
					pool.Status.LastProbeTime = metav1.Now()
					inventory := &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.Now()}
					if owner == "destination" {
						inventory.Copies = []volumeapi.CopyObservation{{Identity: original.CurrentCopy, Present: true}}
					}
					pool.Status.Inventory = inventory
					live := repo.volumes[move.Spec.VolumeID]
					if live.UID != original.UID || live.OwnerNode != owner || !reflect.DeepEqual(live.CurrentCopy, original.CurrentCopy) || live.CapacityBytes != original.CapacityBytes || len(live.PublishedNodes) != 0 {
						t.Fatalf("settlement changed authority/accounting or invented publication: %+v", live)
					}
				}
				live, settled := repo.volumes[move.Spec.VolumeID], repo.moves[0]
				if live.Phase != volumeapi.PhaseReady || live.ActiveMove != "" || !volumeapi.MoveCleanupSettled(settled) || len(settled.Finalizers) != 0 {
					t.Fatalf("retired workload never settled: state=%+v move=%+v lastErr=%v", live, settled.Status, lastErr)
				}
				for _, pool := range repo.pools {
					reserved, err := poolcapacity.ReservedBytesForPool(repo.volumes, repo.moves, pool.UID)
					want := int64(0)
					if pool.NodeName == owner {
						want = original.CapacityBytes
					}
					if err != nil || reserved != want {
						t.Fatalf("retained copy or temporary hold accounting: pool=%s got=%d want=%d err=%v", pool.Name, reserved, want, err)
					}
				}
				if reuse {
					current, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
					if err != nil || !reflect.DeepEqual(current, replacement) {
						t.Fatalf("old operation touched the new claim: %+v %v", current, err)
					}
				}
			})
		}
	}
}
