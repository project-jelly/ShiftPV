package helperpod

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestInspectorMoveNamesPreserveHistoricalDeterministicNames(t *testing.T) {
	jobs, pods := inspectorMoveNames(inspectorHistoryMove())
	if !reflect.DeepEqual(jobs, map[string]bool{
		"shiftpv-move-eec045147f75-copy": true, "shiftpv-move-eec045147f75-promote": true,
		"shiftpv-move-cefc1eeba7ab-verify": true, "shiftpv-move-cefc1eeba7ab-retire": true,
	}) || !reflect.DeepEqual(pods, map[string]bool{
		"shiftpv-move-eec045147f75-source": true, "shiftpv-move-eec045147f75-placement": true,
		"shiftpv-usage-4a0bcab9dfdcff85cf765f571e0bc980": true,
	}) {
		t.Fatalf("historical names changed: jobs=%v pods=%v", jobs, pods)
	}
}

func TestInspectorMovesSettledUsesOneCompleteSnapshot(t *testing.T) {
	var moves []volumeapi.Move
	var objects []runtime.Object
	for index := range 32 {
		move := inspectorHistoryMove()
		move.Name, move.UID = fmt.Sprintf("move-%d", index), fmt.Sprintf("move-uid-%d", index)
		job := inspectorHistoryJob(move, inspectorMoveBase(move.Name)+"-copy", fmt.Sprintf("job-uid-%d", index))
		pod := inspectorHistoryPod(move, fmt.Sprintf("job-pod-%d", index), fmt.Sprintf("pod-uid-%d", index))
		pod.OwnerReferences = []metav1.OwnerReference{inspectorJobOwner(job.Name, string(job.UID))}
		moves, objects = append(moves, move), append(objects, job, pod)
	}
	// Unrelated live workloads and helper-shaped metadata grant no ownership.
	objects = append(objects, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: inspectorNamespace, Name: "unrelated-job", UID: "unrelated-job-uid"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: inspectorNamespace, Name: "unrelated-pod", UID: "unrelated-pod-uid", Labels: map[string]string{"app.kubernetes.io/name": "shiftpv"}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	client := fake.NewClientset(objects...)
	if err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), moves, nil); err != nil {
		t.Fatal(err)
	}
	assertInspectorMoveSnapshotReads(t, client)
	client.ClearActions()
	if err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), nil, nil); err != nil || len(client.Actions()) != 0 {
		t.Fatalf("empty Move history performed observation: actions=%+v err=%v", client.Actions(), err)
	}
}

func TestInspectorMovesSettledChecksEveryHistoricalExecutorRole(t *testing.T) {
	for _, role := range []string{"copy", "promote", "recorded copy", "recorded promotion", "recovery verify", "old recovery retire", "source rsync", "source usage", "placement", "unknown owned Job", "unknown owned Pod"} {
		for _, live := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/live=%v", role, live), func(t *testing.T) {
				move := inspectorHistoryMove()
				name, isPod := inspectorHistoryRole(&move, role)
				var object runtime.Object
				if isPod {
					pod := inspectorHistoryPod(move, name, "pod-uid")
					if live {
						pod.Status.Phase = corev1.PodRunning
					}
					object = pod
				} else {
					job := inspectorHistoryJob(move, name, "job-uid")
					if live {
						job.Status.Conditions = nil
					}
					object = job
				}
				client := fake.NewClientset(object)
				err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{move}, nil)
				if (err != nil) != live {
					t.Fatalf("historical %s quiescence: live=%v err=%v", role, live, err)
				}
				assertInspectorMoveSnapshotReads(t, client)
			})
		}
	}
}

func TestInspectorMovesSettledFindsRelabeledOrphanJobPods(t *testing.T) {
	for _, role := range []string{"copy", "promote", "recorded copy", "recorded promotion", "recovery verify", "old recovery retire"} {
		for _, live := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/live=%v", role, live), func(t *testing.T) {
				move := inspectorHistoryMove()
				name, _ := inspectorHistoryRole(&move, role)
				pod := inspectorHistoryPod(move, "orphaned-pod", "orphan-pod-uid")
				pod.OwnerReferences = []metav1.OwnerReference{inspectorJobOwner(name, "absent-job-uid")}
				if live {
					pod.Status.Phase = corev1.PodRunning
				}
				client := fake.NewClientset(pod)
				err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{move}, nil)
				if (err != nil) != live {
					t.Fatalf("orphan owner-name responsibility lost: live=%v err=%v", live, err)
				}
				assertInspectorMoveSnapshotReads(t, client)
			})
		}
	}
}

func TestInspectorMovesSettledRejectsExecutorIdentityAmbiguity(t *testing.T) {
	for _, change := range []string{"Job owner missing", "Job parent UID", "Job parent name", "Job parent kind", "Job UID missing", "Job UID duplicated", "Job controllers ambiguous", "named Pod owner missing", "named Pod foreign owner", "Pod Job owner UID", "Pod Job owner name", "Pod Job UID belongs to another name", "Pod Job UID wrong kind", "Pod UID duplicated", "placement Pod UID"} {
		t.Run(change, func(t *testing.T) {
			move := inspectorHistoryMove()
			base := inspectorMoveBase(move.Name)
			job := inspectorHistoryJob(move, base+"-copy", "job-uid")
			pod := inspectorHistoryPod(move, "job-pod", "pod-uid")
			pod.OwnerReferences = []metav1.OwnerReference{inspectorJobOwner(job.Name, string(job.UID))}
			objects := []runtime.Object{job, pod}
			switch change {
			case "Job owner missing":
				job.OwnerReferences = nil
			case "Job parent UID":
				job.OwnerReferences[0].UID = "foreign-move-uid"
			case "Job parent name":
				job.OwnerReferences[0].Name = "foreign-move"
			case "Job parent kind":
				job.OwnerReferences[0].Kind = "ShiftPVVolume"
			case "Job UID missing":
				job.UID = ""
			case "Job UID duplicated":
				objects = append(objects, inspectorHistoryJob(move, base+"-promote", string(job.UID)))
			case "Job controllers ambiguous":
				job.OwnerReferences = append(job.OwnerReferences, inspectorMoveOwnerReference(move))
			case "named Pod owner missing":
				pod.Name, pod.OwnerReferences = base+"-source", nil
			case "named Pod foreign owner":
				pod.Name = base + "-source"
				pod.OwnerReferences = []metav1.OwnerReference{inspectorMoveOwnerReference(move)}
				pod.OwnerReferences[0].UID = "foreign-move-uid"
			case "Pod Job owner UID":
				pod.OwnerReferences[0].UID = "replacement-job-uid"
			case "Pod Job owner name":
				pod.OwnerReferences[0].Name = "different-job"
			case "Pod Job UID belongs to another name":
				pod.OwnerReferences[0].Name = base + "-promote"
			case "Pod Job UID wrong kind":
				pod.OwnerReferences[0].Kind = "Pod"
			case "Pod UID duplicated":
				second := pod.DeepCopy()
				second.Name = "second-pod"
				objects = append(objects, second)
			case "placement Pod UID":
				move.Status.PlacementPodUID = "recorded-placement-uid"
				pod.Name = base + "-placement"
				pod.OwnerReferences = []metav1.OwnerReference{inspectorMoveOwnerReference(move)}
			}
			client := fake.NewClientset(objects...)
			if err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{move}, nil); !errors.Is(err, cleanupapi.ErrConflict) {
				t.Fatalf("ambiguous identity accepted: %v", err)
			}
			assertInspectorMoveSnapshotReads(t, client)
		})
	}
}

func TestInspectorMovesSettledRequiresExplicitTermination(t *testing.T) {
	for _, missing := range []string{"Job deleting", "Job active", "Job Failed despite Complete", "Pod deleting", "main status", "init status", "ephemeral status", "running ephemeral"} {
		t.Run(missing, func(t *testing.T) {
			move := inspectorHistoryMove()
			job := inspectorHistoryJob(move, inspectorMoveBase(move.Name)+"-copy", "job-uid")
			pod := inspectorHistoryPod(move, "job-pod", "pod-uid")
			pod.OwnerReferences = []metav1.OwnerReference{inspectorJobOwner(job.Name, string(job.UID))}
			switch missing {
			case "Job deleting":
				stamp := metav1.Now()
				job.DeletionTimestamp = &stamp
			case "Job active":
				job.Status.Active = 1
			case "Job Failed despite Complete":
				job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
			case "Pod deleting":
				stamp := metav1.Now()
				pod.DeletionTimestamp = &stamp
			case "main status":
				pod.Status.ContainerStatuses = nil
			case "init status":
				pod.Status.InitContainerStatuses = nil
			case "ephemeral status":
				pod.Status.EphemeralContainerStatuses = nil
			case "running ephemeral":
				pod.Status.EphemeralContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			}
			client := fake.NewClientset(job, pod)
			if err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{move}, nil); err == nil {
				t.Fatal("incomplete process termination accepted")
			}
			assertInspectorMoveSnapshotReads(t, client)
		})
	}
}

func TestInspectorMovesSettledPreservesLegacyCleanupAbsenceContract(t *testing.T) {
	for _, situation := range []string{"retained complete", "orphan receipt terminal", "absent Job and Pods", "absent Job with terminal owned Pod", "absent Job with terminal receipt Pod", "replacement Job UID", "native shared Pod"} {
		t.Run(situation, func(t *testing.T) {
			move := inspectorHistoryMove()
			journal := inspectorMoveJournal(t, move)
			job, pod := inspectorExecutorObjects(journal)
			objects := []runtime.Object{job, pod}
			blocked, conflict := false, false
			switch situation {
			case "orphan receipt terminal":
				pod.OwnerReferences = nil
			case "absent Job and Pods":
				objects = nil
			case "absent Job with terminal owned Pod":
				pod.UID, objects, blocked = "older-retry-uid", []runtime.Object{pod}, true
			case "absent Job with terminal receipt Pod":
				pod.OwnerReferences, objects, blocked = nil, []runtime.Object{pod}, true
			case "replacement Job UID":
				job.UID, conflict = "replacement-job-uid", true
			case "native shared Pod":
				journal.Status.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: inspectorNamespace, PodName: "resident-node", PodUID: "resident-pod-uid", NodeName: move.Spec.SourceNode}
				journal.Status.Receipt.ExecutorUID = journal.Status.Executor.PodUID
				objects = []runtime.Object{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "resident-node", Namespace: inspectorNamespace, UID: "resident-pod-uid"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}}
			}
			client := fake.NewClientset(objects...)
			err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{move}, map[string]cleanupapi.Cleanup{move.UID: journal})
			if (err != nil) != (blocked || conflict) || errors.Is(err, cleanupapi.ErrConflict) != conflict {
				t.Fatalf("cleanup history changed semantics: blocked=%v conflict=%v err=%v", blocked, conflict, err)
			}
			assertInspectorMoveSnapshotReads(t, client)
		})
	}
}

func TestInspectorMovesSettledRejectsInvalidInputsBeforeObservation(t *testing.T) {
	for _, problem := range []string{"Move UID", "Move name", "Volume identity", "Source node", "recorded name", "duplicate identity", "journal authority", "journal proof"} {
		t.Run(problem, func(t *testing.T) {
			move := inspectorHistoryMove()
			moves := []volumeapi.Move{move}
			journals := map[string]cleanupapi.Cleanup{}
			switch problem {
			case "Move UID":
				moves[0].UID = ""
			case "Move name":
				moves[0].Name = "../move"
			case "Volume identity":
				moves[0].Spec.VolumeID = "invalid"
			case "Source node":
				moves[0].Spec.SourceNode = ""
			case "recorded name":
				moves[0].Status.CopyJobName = "../foreign"
			case "duplicate identity":
				moves = append(moves, move)
			case "journal authority":
				journal := inspectorMoveJournal(t, move)
				journal.Spec.Authority.Name = "another-move"
				journals[move.UID] = journal
			case "journal proof":
				journal := inspectorMoveJournal(t, move)
				journal.Status.AbsenceProof.Complete = false
				journals[move.UID] = journal
			}
			client := fake.NewClientset()
			if err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), moves, journals); !errors.Is(err, cleanupapi.ErrConflict) || len(client.Actions()) != 0 {
				t.Fatalf("invalid history allowed observation: actions=%+v err=%v", client.Actions(), err)
			}
		})
	}
	var nilInspector *Inspector
	if err := nilInspector.MovesSettled(context.Background(), nil, nil); err == nil {
		t.Fatal("empty history bypassed inspector configuration")
	}
}

func TestInspectorMovesSettledFailsClosedOnIncompleteOrFailedLists(t *testing.T) {
	for _, resource := range []string{"jobs", "pods"} {
		for _, failure := range []string{"API error", "continue token", "remaining count"} {
			t.Run(resource+"/"+failure, func(t *testing.T) {
				client := fake.NewClientset()
				cause := errors.New("API LIST unavailable")
				client.PrependReactor("list", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
					if failure == "API error" {
						return true, nil, cause
					}
					meta := metav1.ListMeta{Continue: "unread-page"}
					if failure == "remaining count" {
						remaining := int64(1)
						meta.Continue, meta.RemainingItemCount = "", &remaining
					}
					if resource == "jobs" {
						return true, &batchv1.JobList{ListMeta: meta}, nil
					}
					return true, &corev1.PodList{ListMeta: meta}, nil
				})
				err := (&Inspector{Client: client, Namespace: inspectorNamespace}).MovesSettled(context.Background(), []volumeapi.Move{inspectorHistoryMove()}, nil)
				if err == nil || failure == "API error" && !errors.Is(err, cause) {
					t.Fatalf("incomplete observation accepted or cause lost: %v", err)
				}
				assertInspectorReads(t, client)
			})
		}
	}
}

func inspectorHistoryMove() volumeapi.Move {
	return volumeapi.Move{Name: "move-a", UID: "move-uid", Spec: volumeapi.MoveSpec{ProtocolVersion: volumeapi.MoveProtocolPreparedV1,
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", SourceNode: "worker-a"}, Status: volumeapi.MoveStatus{Phase: "Cancelled"}}
}

func inspectorHistoryRole(move *volumeapi.Move, role string) (string, bool) {
	base, recovery := inspectorMoveBase(move.Name), inspectorMoveBase(move.Name+"-recovery-"+move.UID)
	switch role {
	case "recorded copy":
		move.Status.CopyJobName = "recorded-copy"
		return move.Status.CopyJobName, false
	case "recorded promotion":
		move.Status.PromotionJobName = "recorded-promote"
		return move.Status.PromotionJobName, false
	case "recovery verify":
		return recovery + "-verify", false
	case "old recovery retire":
		return recovery + "-retire", false
	case "source rsync":
		return base + "-source", true
	case "source usage":
		return sourceUsagePodName(move.UID), true
	case "placement":
		return base + "-placement", true
	case "unknown owned Job":
		return "old-job", false
	case "unknown owned Pod":
		return "old-pod", true
	default:
		return base + "-" + role, false
	}
}

func inspectorMoveOwnerReference(move volumeapi.Move) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name, UID: types.UID(move.UID), Controller: &controller}
}

func inspectorJobOwner(name, uid string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: name, UID: types.UID(uid), Controller: &controller}
}

func inspectorHistoryJob(move volumeapi.Move, name, uid string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: inspectorNamespace, UID: types.UID(uid), OwnerReferences: []metav1.OwnerReference{inspectorMoveOwnerReference(move)}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
}

func inspectorHistoryPod(move volumeapi.Move, name, uid string) *corev1.Pod {
	terminated := func(name string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: inspectorNamespace, UID: types.UID(uid), OwnerReferences: []metav1.OwnerReference{inspectorMoveOwnerReference(move)}},
		Spec: corev1.PodSpec{NodeName: move.Spec.SourceNode, Containers: []corev1.Container{{Name: "operation"}}, InitContainers: []corev1.Container{{Name: "init"}},
			EphemeralContainers: []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{terminated("operation")},
			InitContainerStatuses: []corev1.ContainerStatus{terminated("init")}, EphemeralContainerStatuses: []corev1.ContainerStatus{terminated("debug")}}}
}

func inspectorMoveJournal(t *testing.T, move volumeapi.Move) cleanupapi.Cleanup {
	t.Helper()
	current := inspectorCompletedCleanup(t)
	current.UID, current.Spec.Reason = move.UID, "MoveSource"
	current.Spec.Authority = cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}
	current.Spec.OperationID = volumeapi.MoveCleanupOperationID(move.UID)
	current.Status.Receipt.OperationID = current.Spec.OperationID
	if err := cleanupapi.ValidateSettled(current); err != nil {
		t.Fatal(err)
	}
	return current
}

func assertInspectorMoveSnapshotReads(t *testing.T, client *fake.Clientset) {
	t.Helper()
	assertInspectorReads(t, client)
	actions := client.Actions()
	if len(actions) != 2 || actions[0].GetVerb() != "list" || actions[0].GetResource().Resource != "jobs" || actions[1].GetVerb() != "list" || actions[1].GetResource().Resource != "pods" {
		t.Fatalf("Move history did not use exactly two shared LISTs: %+v", actions)
	}
}
