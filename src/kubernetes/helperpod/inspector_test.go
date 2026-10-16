package helperpod

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const inspectorNamespace = "shiftpv-system"

func TestInspectorPresencePreservesAnyAcceptedExecution(t *testing.T) {
	cleanup := inspectorCompletedCleanup(t)
	creation := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "shiftpv-create-3b70b23c42bc20fc404337f322e57ce0", Namespace: inspectorNamespace, UID: "old-creation-uid",
	}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: cleanup.Name + "-effect", Namespace: inspectorNamespace, UID: "unbound-job-uid"}}
	client := fake.NewClientset(creation, job)
	inspector := &Inspector{Client: client, Namespace: inspectorNamespace}
	if exists, err := inspector.CreationExists(context.Background(), cleanup.Spec.Target); err != nil || !exists {
		t.Fatalf("accepted creation Pod disappeared: exists=%v err=%v", exists, err)
	}
	if exists, err := inspector.CleanupExists(context.Background(), cleanup); err != nil || !exists {
		t.Fatalf("unbound cleanup Job disappeared: exists=%v err=%v", exists, err)
	}
	assertInspectorReads(t, client)
	if len(client.Actions()) != 2 || client.Actions()[0].(k8stesting.GetAction).GetName() != creation.Name ||
		client.Actions()[1].(k8stesting.GetAction).GetName() != job.Name {
		t.Fatalf("legacy deterministic names changed: %+v", client.Actions())
	}
	storedCreation, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), inspectorNamespace, creation.Name)
	if err != nil || !reflect.DeepEqual(storedCreation, creation) {
		t.Fatalf("creation changed: object=%+v err=%v", storedCreation, err)
	}
	storedJob, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), inspectorNamespace, job.Name)
	if err != nil || !reflect.DeepEqual(storedJob, job) {
		t.Fatalf("cleanup Job changed: object=%+v err=%v", storedJob, err)
	}
}

func TestInspectorPresenceAbsenceAndConfiguration(t *testing.T) {
	cleanup := inspectorCompletedCleanup(t)
	client := fake.NewClientset()
	inspector := &Inspector{Client: client, Namespace: inspectorNamespace}
	for _, check := range []func() (bool, error){
		func() (bool, error) { return inspector.CreationExists(context.Background(), cleanup.Spec.Target) },
		func() (bool, error) { return inspector.CleanupExists(context.Background(), cleanup) },
	} {
		if exists, err := check(); err != nil || exists {
			t.Fatalf("missing legacy executor: exists=%v err=%v", exists, err)
		}
	}
	assertInspectorReads(t, client)
	for _, invalid := range []*Inspector{nil, {}, {Client: client}, {Namespace: inspectorNamespace}, {Client: client, Namespace: "../wrong"}} {
		if exists, err := invalid.CreationExists(context.Background(), cleanup.Spec.Target); err == nil || exists {
			t.Fatalf("invalid creation inspector allowed absence: exists=%v err=%v", exists, err)
		}
		if exists, err := invalid.CleanupExists(context.Background(), cleanup); err == nil || exists {
			t.Fatalf("invalid cleanup inspector allowed absence: exists=%v err=%v", exists, err)
		}
	}
	client.ClearActions()
	identity := cleanup.Spec.Target
	identity.CopyID = ""
	if exists, err := inspector.CreationExists(context.Background(), identity); err == nil || exists {
		t.Fatalf("invalid copy identity allowed absence: exists=%v err=%v", exists, err)
	}
	cleanup.UID = "replacement-parent"
	if exists, err := inspector.CleanupExists(context.Background(), cleanup); !errors.Is(err, cleanupapi.ErrConflict) || exists {
		t.Fatalf("invalid cleanup authority allowed absence: exists=%v err=%v", exists, err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("invalid input reached Kubernetes: %+v", client.Actions())
	}
}

func TestInspectorCompletedCleanupAllowsOnlyQuiescentHistory(t *testing.T) {
	for _, test := range []struct {
		name    string
		edit    func(*batchv1.Job, *corev1.Pod)
		settled bool
	}{
		{name: "retained terminal history", settled: true},
		{name: "old image and removed labels", settled: true, edit: func(job *batchv1.Job, pod *corev1.Pod) {
			job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "old-binary", Image: "old-release:old-tag"}}
			job.Labels, job.Spec.Template.Labels, pod.Labels = nil, nil, nil
		}},
		{name: "failed prior Pod is explicitly terminated", settled: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodFailed
			pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
		}},
		{name: "active Job", edit: func(job *batchv1.Job, _ *corev1.Pod) { job.Status.Active = 1 }},
		{name: "terminating Job", edit: func(job *batchv1.Job, _ *corev1.Pod) { stamp := metav1.Now(); job.DeletionTimestamp = &stamp }},
		{name: "incomplete Job", edit: func(job *batchv1.Job, _ *corev1.Pod) { job.Status.Conditions = nil }},
		{name: "failed wins over complete", edit: func(job *batchv1.Job, _ *corev1.Pod) {
			job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
		}},
		{name: "running Pod despite JobComplete", edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Status.Phase = corev1.PodRunning }},
		{name: "terminating Pod", edit: func(_ *batchv1.Job, pod *corev1.Pod) { stamp := metav1.Now(); pod.DeletionTimestamp = &stamp }},
		{name: "main status absent", edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Status.ContainerStatuses = nil }},
		{name: "main process running", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{name: "contradictory process state", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Status.ContainerStatuses[0].State.Running = &corev1.ContainerStateRunning{}
		}},
		{name: "init status absent", edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Status.InitContainerStatuses = nil }},
		{name: "init process running", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Status.InitContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{name: "ephemeral status absent", edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Status.EphemeralContainerStatuses = nil }},
		{name: "ephemeral process running", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Status.EphemeralContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{name: "unknown container status", edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Status.ContainerStatuses[0].Name = "different-container" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := inspectorCompletedCleanup(t)
			job, pod := inspectorExecutorObjects(cleanup)
			if test.edit != nil {
				test.edit(job, pod)
			}
			client := fake.NewClientset(job, pod)
			inspector := &Inspector{Client: client, Namespace: inspectorNamespace}
			before, err := json.Marshal(cleanup)
			if err != nil {
				t.Fatal(err)
			}
			settled, err := inspector.CompletedCleanupSettled(context.Background(), cleanup)
			if err != nil || settled != test.settled {
				t.Fatalf("settled=%v want=%v err=%v", settled, test.settled, err)
			}
			after, err := json.Marshal(cleanup)
			if err != nil || string(before) != string(after) {
				t.Fatalf("historical cleanup was changed: err=%v", err)
			}
			assertInspectorReads(t, client)
		})
	}
}

func TestInspectorCompletedCleanupMatchesExactUIDsWithoutLabels(t *testing.T) {
	for _, test := range []struct {
		name     string
		edit     func(*batchv1.Job, *corev1.Pod)
		settled  bool
		conflict bool
	}{
		{name: "Job-owned relabeled live retry", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.UID, pod.Labels, pod.Status.Phase = "retry-pod-uid", nil, corev1.PodRunning
		}},
		{name: "orphan receipt Pod still live", edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.OwnerReferences, pod.Labels, pod.Status.Phase = nil, nil, corev1.PodRunning
		}},
		{name: "orphan receipt Pod terminated", settled: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.OwnerReferences, pod.Labels = nil, nil }},
		{name: "same Pod name different UID and Job UID ignored", settled: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.UID, pod.OwnerReferences[0].UID, pod.Status.Phase = "replacement-pod-uid", "replacement-job-uid", corev1.PodRunning
		}},
		{name: "unrelated namespace ignored", settled: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) {
			pod.Namespace, pod.Status.Phase = "workloads", corev1.PodRunning
		}},
		{name: "same Job name different UID", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.UID = "replacement-job-uid" }},
		{name: "parent same name different UID", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.OwnerReferences[0].UID = "replacement-parent-uid" }},
		{name: "parent name changed", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.OwnerReferences[0].Name = "different-parent" }},
		{name: "parent kind changed", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.OwnerReferences[0].Kind = "ShiftPVMove" }},
		{name: "Job owner removed", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.OwnerReferences = nil }},
		{name: "Job node changed", conflict: true, edit: func(job *batchv1.Job, _ *corev1.Pod) { job.Spec.Template.Spec.NodeName = "worker-b" }},
		{name: "receipt Pod node changed", conflict: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.Spec.NodeName = "worker-b" }},
		{name: "receipt Pod rebound to different Job", conflict: true, edit: func(_ *batchv1.Job, pod *corev1.Pod) { pod.OwnerReferences[0].UID = "replacement-job-uid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := inspectorCompletedCleanup(t)
			job, pod := inspectorExecutorObjects(cleanup)
			test.edit(job, pod)
			client := fake.NewClientset(job, pod)
			settled, err := (&Inspector{Client: client, Namespace: inspectorNamespace}).CompletedCleanupSettled(context.Background(), cleanup)
			if settled != test.settled || errors.Is(err, cleanupapi.ErrConflict) != test.conflict || err != nil && !test.conflict {
				t.Fatalf("settled=%v want=%v conflict=%v err=%v", settled, test.settled, test.conflict, err)
			}
			assertInspectorReads(t, client)
		})
	}
}

func TestInspectorCompletedCleanupAfterJobDisappears(t *testing.T) {
	for _, test := range []struct {
		name    string
		pod     string
		settled bool
	}{
		{name: "Job and Pods absent", settled: true},
		{name: "terminal owned Pod remains", pod: "owned"},
		{name: "orphan receipt Pod remains", pod: "receipt"},
		{name: "replacement Pod unrelated to exact executor", pod: "replacement", settled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := inspectorCompletedCleanup(t)
			_, pod := inspectorExecutorObjects(cleanup)
			var objects []runtime.Object
			switch test.pod {
			case "owned":
				pod.UID = "prior-retry-uid"
			case "receipt":
				pod.OwnerReferences = nil
			case "replacement":
				pod.UID, pod.OwnerReferences[0].UID = "replacement-pod-uid", "replacement-job-uid"
			}
			if test.pod != "" {
				objects = append(objects, pod)
			}
			client := fake.NewClientset(objects...)
			settled, err := (&Inspector{Client: client, Namespace: inspectorNamespace}).CompletedCleanupSettled(context.Background(), cleanup)
			if err != nil || settled != test.settled {
				t.Fatalf("settled=%v want=%v err=%v", settled, test.settled, err)
			}
			assertInspectorReads(t, client)
		})
	}
}

func TestInspectorRejectsDamagedProofBeforeReadingExecutors(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*cleanupapi.Cleanup)
	}{
		{"unfinished cleanup", func(c *cleanupapi.Cleanup) { c.Status.Phase = cleanupapi.PhaseConfirmingAbsence }},
		{"missing receipt", func(c *cleanupapi.Cleanup) { c.Status.Receipt = nil }},
		{"incomplete absence", func(c *cleanupapi.Cleanup) { c.Status.AbsenceProof.Complete = false }},
		{"missing receipt Pod UID", func(c *cleanupapi.Cleanup) { c.Status.Executor.PodUID = "" }},
		{"invalid Job binding", func(c *cleanupapi.Cleanup) { c.Status.Executor.JobUID = "" }},
		{"resident Node binding", func(c *cleanupapi.Cleanup) {
			c.Status.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: inspectorNamespace, PodName: "resident-node", PodUID: "resident-pod-uid", NodeName: c.Spec.Target.NodeName}
			c.Status.Receipt.ExecutorUID = "resident-pod-uid"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := inspectorCompletedCleanup(t)
			test.edit(&cleanup)
			client := fake.NewClientset()
			settled, err := (&Inspector{Client: client, Namespace: inspectorNamespace}).CompletedCleanupSettled(context.Background(), cleanup)
			if settled || !errors.Is(err, cleanupapi.ErrConflict) || len(client.Actions()) != 0 {
				t.Fatalf("damaged proof reached executor observation: settled=%v actions=%+v err=%v", settled, client.Actions(), err)
			}
		})
	}
}

func TestInspectorAPIFailuresPreserveCause(t *testing.T) {
	for _, test := range []struct {
		name, verb, resource string
		check                func(*Inspector, cleanupapi.Cleanup) (bool, error)
	}{
		{"creation GET", "get", "pods", func(i *Inspector, c cleanupapi.Cleanup) (bool, error) {
			return i.CreationExists(context.Background(), c.Spec.Target)
		}},
		{"cleanup presence GET", "get", "jobs", func(i *Inspector, c cleanupapi.Cleanup) (bool, error) {
			return i.CleanupExists(context.Background(), c)
		}},
		{"completed Job GET", "get", "jobs", func(i *Inspector, c cleanupapi.Cleanup) (bool, error) {
			return i.CompletedCleanupSettled(context.Background(), c)
		}},
		{"completed Pods LIST", "list", "pods", func(i *Inspector, c cleanupapi.Cleanup) (bool, error) {
			return i.CompletedCleanupSettled(context.Background(), c)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanup := inspectorCompletedCleanup(t)
			job, _ := inspectorExecutorObjects(cleanup)
			client := fake.NewClientset(job)
			cause := errors.New("API observation unavailable")
			client.PrependReactor(test.verb, test.resource, func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, cause })
			settled, err := test.check(&Inspector{Client: client, Namespace: inspectorNamespace}, cleanup)
			if settled || !errors.Is(err, cause) {
				t.Fatalf("API failure lost cause or allowed progress: settled=%v err=%v", settled, err)
			}
			assertInspectorReads(t, client)
		})
	}
}

func inspectorCompletedCleanup(t *testing.T) cleanupapi.Cleanup {
	t.Helper()
	const now = "2026-10-16T00:00:00Z"
	cleanup := cleanupapi.Cleanup{
		Name: "shiftpv-cleanup-history", UID: "volume-uid",
		Spec: cleanupapi.Spec{
			OperationID: "delete-volume-uid", Reason: "VolumeDelete",
			Target: volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
				VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing},
			Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: "shiftpv-0123456789abcdef0123456789abcdef", UID: "volume-uid"},
		},
		Status: cleanupapi.Status{
			Phase: cleanupapi.PhaseCompleted, SettledAt: now,
			Executor: &cleanupapi.Executor{JobName: "shiftpv-cleanup-history-effect", JobUID: "job-uid", PodUID: "receipt-pod-uid", NodeName: "worker-a"},
			Receipt:  &cleanupapi.Receipt{OperationID: "delete-volume-uid", ExecutorUID: "job-uid", ObservedAt: now, Retired: true, Purged: true, LocalReceiptDigest: strings.Repeat("a", 64)},
			AbsenceProof: &cleanupapi.AbsenceProof{RequestID: "scan-request", PoolName: "pool-a", PoolUID: "pool-uid",
				RequiredGeneration: 4, ObservedGeneration: 5, Valid: true, Complete: true, Absent: true, ConfirmedAt: now},
		},
	}
	if err := cleanupapi.ValidateSettled(cleanup); err != nil {
		t.Fatalf("invalid completed fixture: %v", err)
	}
	return cleanup
}

func inspectorExecutorObjects(cleanup cleanupapi.Cleanup) (*batchv1.Job, *corev1.Pod) {
	controller := true
	executor, authority := cleanup.Status.Executor, cleanup.Spec.Authority
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: executor.JobName, Namespace: inspectorNamespace, UID: types.UID(executor.JobUID),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: authority.Kind, Name: authority.Name, UID: types.UID(authority.UID), Controller: &controller}}},
		Spec:   batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{NodeName: executor.NodeName}}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}},
	}
	terminated := func(name string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cleanup-receipt", Namespace: inspectorNamespace, UID: types.UID(executor.PodUID),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: executor.JobName, UID: types.UID(executor.JobUID), Controller: &controller}}},
		Spec: corev1.PodSpec{NodeName: executor.NodeName, Containers: []corev1.Container{{Name: "cleanup"}},
			InitContainers: []corev1.Container{{Name: "init"}}, EphemeralContainers: []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{terminated("cleanup")},
			InitContainerStatuses: []corev1.ContainerStatus{terminated("init")}, EphemeralContainerStatuses: []corev1.ContainerStatus{terminated("debug")}},
	}
	return job, pod
}

func assertInspectorReads(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" || action.GetNamespace() != inspectorNamespace {
			t.Fatalf("inspector issued a mutation or escaped installation namespace: %+v", action)
		}
		if action.GetResource().Resource != "jobs" && action.GetResource().Resource != "pods" {
			t.Fatalf("inspector read unrelated infrastructure: %+v", action)
		}
		if list, ok := action.(k8stesting.ListAction); ok && (!list.GetListRestrictions().Labels.Empty() || !list.GetListRestrictions().Fields.Empty()) {
			t.Fatalf("executor Pod observation depends on mutable selectors: %+v", action)
		}
	}
}
