package upgrade

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	testID        = "shiftpv-0123456789abcdef0123456789abcdef"
	testNamespace = "shiftpv-system"
	testTime      = "2026-10-16T00:00:00Z"
)

type fixture struct {
	checker *Checker
	dynamic *dynamicfake.FakeDynamicClient
	kube    *kubefake.Clientset
}

func newFixture(t *testing.T, objects ...runtime.Object) *fixture {
	t.Helper()
	dynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList",
	}, objects...)
	controller := true
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "shiftpv-node", UID: "ds-uid"},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "node"}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "node-a", UID: "resident-pod-uid",
		Annotations:     map[string]string{volumeapi.NodeEffectsAnnotation: "v1", volumeapi.NodeMoveCleanupAnnotation: "v1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: &controller}}},
		Spec:   corev1.PodSpec{NodeName: "worker-a", ServiceAccountName: "node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	kube := kubefake.NewClientset(ds, pod)
	return &fixture{checker: &Checker{Client: kube, Volumes: &volumeapi.Registry{Client: dynamic}, Cleanups: &cleanupapi.Store{Client: dynamic}, Namespace: testNamespace, NodeDaemonSet: ds.Name}, dynamic: dynamic, kube: kube}
}

func readyVolume(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: testID,
		VolumeUID: "volume-uid", NodeName: "worker-a", Role: volume.RoleServing, CopyID: "initial-volume-uid"}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": testID, "uid": "volume-uid", "finalizers": []any{volumeapi.VolumeProtectionFinalizer}},
		"spec":     map[string]any{"requestName": "old-claim", "initialNode": "worker-a", "capacityBytes": int64(1024)},
		"status":   map[string]any{"phase": volumeapi.PhaseReady, "ownerNode": "worker-a", "currentCopy": encode(t, &copy), "publishedNodes": []any{"worker-a"}},
	}}
}

func registeredPool(name, node string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": name, "uid": name + "-uid"},
		"spec":   map[string]any{"nodeName": node, "mountPath": "/mnt/pool", "capacity": map[string]any{"limit": "1Gi"}},
		"status": map[string]any{"registrationApproved": true},
	}}
}

func encode(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertReadOnly(t *testing.T, f *fixture) {
	t.Helper()
	for _, action := range append(f.dynamic.Actions(), f.kube.Actions()...) {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("checker wrote API: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
		if action.GetResource().Resource == "jobs" && action.GetNamespace() != testNamespace {
			t.Fatalf("cluster Job permission required: %#v", action)
		}
	}
}

func check(t *testing.T, f *fixture, safe bool) Report {
	t.Helper()
	report, err := f.checker.Check(context.Background())
	if err != nil || !report.Complete || report.Safe() != safe {
		t.Fatalf("safe=%v: report=%+v error=%v", safe, report, err)
	}
	assertReadOnly(t, f)
	return report
}

func TestReadyWithoutPVOrReceiptIsReadOnlyAndSafe(t *testing.T) {
	for _, creation := range []string{"legacy", "native", "moved"} {
		t.Run(creation, func(t *testing.T) {
			object := readyVolume(t)
			if creation != "legacy" {
				executor := volumeapi.NodeExecutor{Namespace: testNamespace, PodName: "original-creator", PodUID: "creator-pod-uid", NodeName: "worker-a"}
				receipt := volumeapi.CreationReceipt{OperationID: "create-volume-uid", ExecutorUID: executor.PodUID, ObservedAt: testTime, LocalReceiptDigest: strings.Repeat("a", 64)}
				_ = unstructured.SetNestedField(object.Object, "create-volume-uid", "status", "creationOperationID")
				_ = unstructured.SetNestedMap(object.Object, encode(t, &executor), "status", "creationExecutor")
				_ = unstructured.SetNestedMap(object.Object, encode(t, &receipt), "status", "creationReceipt")
			}
			if creation == "moved" {
				_ = unstructured.SetNestedField(object.Object, "worker-b", "status", "ownerNode")
				_ = unstructured.SetNestedField(object.Object, "worker-b", "status", "currentCopy", "nodeName")
				_ = unstructured.SetNestedSlice(object.Object, []any{"worker-b"}, "status", "publishedNodes")
			}
			before := object.DeepCopy()
			f := newFixture(t, object)
			if creation == "moved" {
				pod, err := f.kube.CoreV1().Pods(testNamespace).Get(context.Background(), "node-a", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.Spec.NodeName = "worker-b"
				if _, err := f.kube.CoreV1().Pods(testNamespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				f.kube.ClearActions()
			}
			check(t, f, true)
			after, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(context.Background(), testID, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("Ready data or reservation mutated: before=%v after=%v error=%v", before, after, err)
			}
			for _, action := range f.kube.Actions() {
				if action.GetResource().Resource == "persistentvolumes" || action.GetResource().Resource == "persistentvolumeclaims" {
					t.Fatalf("legacy Ready required PV/PVC: %#v", action)
				}
			}
		})
	}
}

func TestUnsettledVolumesBlockWithoutChangingData(t *testing.T) {
	for _, test := range []struct {
		name   string
		edit   func(*unstructured.Unstructured)
		reason string
	}{
		{"active Move", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, "move-a", "status", "activeMove")
		}, "ActiveMove"},
		{"pending creation", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, "Pending", "status", "phase")
		}, "VolumeNotReady"},
		{"deletion", func(o *unstructured.Unstructured) { now := metav1.Now(); o.SetDeletionTimestamp(&now) }, "DeletionOrCleanupUnsettled"},
		{"delete operation", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, "delete-volume-uid", "status", "deletionOperationID")
		}, "DeletionOrCleanupUnsettled"},
		{"copy UID", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, "other-volume-uid", "status", "currentCopy", "volumeUID")
		}, "VolumeIdentityInvalid"},
		{"foreign publication", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, []any{"worker-b"}, "status", "publishedNodes")
		}, "PublicationOwnerMismatch"},
		{"receipt without executor", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedMap(o.Object, map[string]any{"operationID": "create-volume-uid"}, "status", "creationReceipt")
		}, "CreationEvidenceInvalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := readyVolume(t)
			test.edit(object)
			f := newFixture(t, object)
			report := check(t, f, false)
			if len(report.Blockers) != 1 || report.Blockers[0].Reason != test.reason {
				t.Fatalf("unexpected blocker: %+v", report)
			}
		})
	}
}

func TestCapabilityScopeIncludesApprovedPoolsAndReadyOwners(t *testing.T) {
	for _, test := range []struct {
		name  string
		pool  *unstructured.Unstructured
		ready bool
		safe  bool
	}{
		{"approved unavailable node", registeredPool("pool-b", "worker-b"), false, false},
		{"approved NoSpace and no inventory", registeredPool("pool-a", "worker-a"), false, true},
		{"unapproved unrelated", registeredPool("unapproved", "worker-b"), false, true},
		{"invalid unrelated", registeredPool("invalid", "worker-b"), false, true},
		{"deleting unrelated", registeredPool("deleting", "worker-b"), false, true},
		{"Ready owner despite deleting pool", registeredPool("deleting-owner", "worker-a"), true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			switch test.name {
			case "unapproved unrelated":
				_ = unstructured.SetNestedField(test.pool.Object, false, "status", "registrationApproved")
			case "invalid unrelated":
				_ = unstructured.SetNestedField(test.pool.Object, "relative/path", "spec", "mountPath")
			case "deleting unrelated", "Ready owner despite deleting pool":
				now := metav1.Now()
				test.pool.SetDeletionTimestamp(&now)
			case "approved NoSpace and no inventory":
				_ = unstructured.SetNestedSlice(test.pool.Object, []any{map[string]any{"type": "Ready", "status": "False", "reason": "NoSpace"}}, "status", "conditions")
			}
			objects := []runtime.Object{test.pool}
			if test.ready {
				objects = append(objects, readyVolume(t))
			}
			f := newFixture(t, objects...)
			check(t, f, test.safe)
		})
	}
}

func TestCapabilityRequiresExactReadyResidentIdentity(t *testing.T) {
	for _, name := range []string{"old effects", "old cleanup", "replacement DaemonSet", "duplicate executor", "terminating"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, readyVolume(t))
			pod, _ := f.kube.CoreV1().Pods(testNamespace).Get(context.Background(), "node-a", metav1.GetOptions{})
			switch name {
			case "old effects":
				delete(pod.Annotations, volumeapi.NodeEffectsAnnotation)
			case "old cleanup":
				delete(pod.Annotations, volumeapi.NodeMoveCleanupAnnotation)
			case "replacement DaemonSet":
				pod.OwnerReferences[0].UID = "old-ds-uid"
			case "terminating":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			case "duplicate executor":
				pod.Name = "node-duplicate"
				pod.UID = "other-pod-uid"
			}
			if name == "duplicate executor" {
				_, _ = f.kube.CoreV1().Pods(testNamespace).Create(context.Background(), pod, metav1.CreateOptions{})
			} else {
				_, _ = f.kube.CoreV1().Pods(testNamespace).Update(context.Background(), pod, metav1.UpdateOptions{})
			}
			f.kube.ClearActions()
			report, err := f.checker.Check(context.Background())
			if report.Safe() || (name == "duplicate executor" && (err == nil || report.Complete)) || (name != "duplicate executor" && (err != nil || !report.Complete || len(report.Blockers) != 1)) {
				t.Fatalf("untrusted capability accepted: report=%+v error=%v", report, err)
			}
			assertReadOnly(t, f)
		})
	}
}

func TestObservationFailureNeverApprovesUpgrade(t *testing.T) {
	if (Report{}).Safe() {
		t.Fatal("zero report approved upgrade")
	}
	for _, resource := range []string{"shiftpvvolumes", "shiftpvmoves", "shiftpvpools", "jobs", "pods", "daemonsets"} {
		t.Run(resource, func(t *testing.T) {
			f := newFixture(t, readyVolume(t))
			fail := func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("injected observation failure")
			}
			if strings.HasPrefix(resource, "shiftpv") {
				f.dynamic.PrependReactor("list", resource, fail)
			} else {
				verb := "list"
				if resource == "daemonsets" {
					verb = "get"
				}
				f.kube.PrependReactor(verb, resource, fail)
			}
			report, err := f.checker.Check(context.Background())
			if err == nil || report.Complete || report.Safe() {
				t.Fatalf("failed observation approved: %+v %v", report, err)
			}
			assertReadOnly(t, f)
		})
	}
	f := newFixture(t, readyVolume(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if report, err := f.checker.Check(ctx); err == nil || report.Safe() || report.Complete {
		t.Fatalf("cancelled observation approved: %+v %v", report, err)
	}
	bad := readyVolume(t)
	_ = unstructured.SetNestedField(bad.Object, "not-an-object", "status", "currentCopy")
	f = newFixture(t, bad)
	if report, err := f.checker.Check(context.Background()); err == nil || report.Complete || report.Safe() {
		t.Fatalf("corrupt identity approved: %+v %v", report, err)
	}
}

func parentOwner(uid string) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVVolume", Name: testID, UID: types.UID(uid)}}
}

func executorJob() *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "cleanup-job", UID: "job-uid", OwnerReferences: parentOwner("volume-uid")}}
}

func executorPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "cleanup-pod", UID: "cleanup-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "cleanup-job", UID: "job-uid"}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "helper"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
}

func TestExactUnlabelledExecutorsBlockAndForeignUIDDoesNot(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*batchv1.Job, *corev1.Pod)
		safe bool
	}{
		{"exact owner and Job Pod UID", func(*batchv1.Job, *corev1.Pod) {}, false},
		{"same name different owner UID", func(j *batchv1.Job, _ *corev1.Pod) { j.OwnerReferences = parentOwner("replacement-volume") }, true},
		{"same name different Job UID", func(j *batchv1.Job, p *corev1.Pod) {
			j.OwnerReferences = nil
			p.OwnerReferences[0].UID = "replacement-job"
		}, true},
		{"unrelated namespace fake owner", func(j *batchv1.Job, p *corev1.Pod) { j.Namespace = "workloads"; p.Namespace = "workloads" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, readyVolume(t))
			job, pod := executorJob(), executorPod()
			test.edit(job, pod)
			if err := f.kube.Tracker().Add(job); err != nil {
				t.Fatal(err)
			}
			if err := f.kube.Tracker().Add(pod); err != nil {
				t.Fatal(err)
			}
			report := check(t, f, test.safe)
			if !test.safe && len(report.Blockers) != 2 {
				t.Fatalf("label-free Job and Pod not both observed: %+v", report)
			}
		})
	}
}

func TestTerminalHistoryExecutorsRequireFullQuiescence(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*batchv1.Job, *corev1.Pod)
		safe bool
	}{
		{"complete and all processes terminated", func(*batchv1.Job, *corev1.Pod) {}, true},
		{"active Job", func(j *batchv1.Job, _ *corev1.Pod) { j.Status.Active = 1 }, false},
		{"terminating Job", func(j *batchv1.Job, _ *corev1.Pod) { now := metav1.Now(); j.DeletionTimestamp = &now }, false},
		{"failed retryable Job", func(j *batchv1.Job, _ *corev1.Pod) {
			j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
		}, false},
		{"contradictory Job completion", func(j *batchv1.Job, _ *corev1.Pod) {
			j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
		}, false},
		{"terminal Pod missing exit status", func(_ *batchv1.Job, p *corev1.Pod) { p.Status.ContainerStatuses = nil }, false},
		{"running Pod after Job complete", func(_ *batchv1.Job, p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }, false},
		{"terminating terminal Pod", func(_ *batchv1.Job, p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }, false},
		{"running init sidecar", func(_ *batchv1.Job, p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "sidecar"}}
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		}, false},
		{"running ephemeral process", func(_ *batchv1.Job, p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
			p.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{Name: "debug", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		}, false},
		{"wrong ephemeral identity", func(_ *batchv1.Job, p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
			p.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{Name: "other-debug", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
		}, false},
		{"contradictory process state", func(_ *batchv1.Job, p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Running = &corev1.ContainerStateRunning{}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, readyVolume(t))
			job, pod := executorJob(), executorPod()
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			pod.Status.Phase = corev1.PodSucceeded
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "helper", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
			test.edit(job, pod)
			_ = f.kube.Tracker().Add(job)
			_ = f.kube.Tracker().Add(pod)
			check(t, f, test.safe)
		})
	}
}

// Settled cleanup is decoded through the real Store, not an invented phase-only
// repository. Exact receipt executor IDs still discover helpers stripped of all
// labels and owner references.
func TestRecordedExecutorUIDsAndIndependentCleanupEvidence(t *testing.T) {
	for _, test := range []struct {
		name     string
		executor string
		wantKind string
		corrupt  bool
	}{
		{"closed retained history", "", "", false},
		{"closed retained executors", "terminal", "", false},
		{"unlabelled live recorded Pod", "pod", "Pod", false},
		{"unlabelled live recorded Job", "job", "Job", false},
		{"same names different UIDs", "replacement", "", false},
		{"receipt mismatch", "", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			move, journal := settledMove(t)
			if test.corrupt {
				journal.Status.Receipt.ExecutorUID = "replacement-executor"
				_ = unstructured.SetNestedMap(move.Object, encode(t, &journal.Status), "status", "cleanup", "status")
			}
			f := newFixture(t, move)
			switch test.executor {
			case "pod":
				pod := executorPod()
				pod.OwnerReferences = nil
				_ = f.kube.Tracker().Add(pod)
			case "job":
				job := executorJob()
				job.OwnerReferences = nil
				_ = f.kube.Tracker().Add(job)
			case "terminal":
				job, pod := executorJob(), executorPod()
				job.OwnerReferences = nil
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
				pod.Status.Phase = corev1.PodSucceeded
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "helper", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
				_ = f.kube.Tracker().Add(job)
				_ = f.kube.Tracker().Add(pod)
			case "replacement":
				job, pod := executorJob(), executorPod()
				job.UID, job.OwnerReferences = "replacement-job", nil
				pod.UID, pod.OwnerReferences = "replacement-pod", nil
				_ = f.kube.Tracker().Add(job)
				_ = f.kube.Tracker().Add(pod)
			}
			report := check(t, f, test.wantKind == "" && !test.corrupt)
			if test.wantKind != "" && (len(report.Blockers) != 1 || report.Blockers[0].Kind != test.wantKind) {
				t.Fatalf("exact recorded executor UID missed: %+v", report)
			}
		})
	}
}

func TestUnsettledHistoryCannotUseCompletedPhaseAlone(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*unstructured.Unstructured)
	}{
		{"active Move", func(m *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(m.Object, "Copying", "status", "phase")
		}},
		{"NeedsReview", func(m *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(m.Object, cleanupapi.PhaseNeedsReview, "status", "cleanup", "status", "phase")
		}},
		{"missing absence proof", func(m *unstructured.Unstructured) {
			unstructured.RemoveNestedField(m.Object, "status", "cleanup", "status", "absenceProof")
		}},
		{"replacement cleanup parent", func(m *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(m.Object, "replacement-move", "status", "cleanup", "spec", "authority", "uid")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			move, _ := settledMove(t)
			test.edit(move)
			f := newFixture(t, move)
			report, err := f.checker.Check(context.Background())
			if report.Safe() || (err == nil && (!report.Complete || len(report.Blockers) == 0)) {
				t.Fatalf("unsettled or damaged history approved: report=%+v error=%v", report, err)
			}
			assertReadOnly(t, f)
		})
	}
}

func TestPreparedNoArtifactRecoveryPreflightPreservesHistory(t *testing.T) {
	for _, scenario := range []string{
		"settled", "legacy", "unknown protocol", "no rollback fence", "missing source plan",
		"missing binding plan", "unreleased hold", "wrong recovery owner", "unfinished recovery",
		"cleanup without receipt", "live executor",
	} {
		t.Run(scenario, func(t *testing.T) {
			object := readyVolume(t)
			move := preparedNoArtifactMove(t, object)
			switch scenario {
			case "legacy":
				unstructured.RemoveNestedField(move.Object, "spec", "protocolVersion")
			case "unknown protocol":
				_ = unstructured.SetNestedField(move.Object, "future-v2", "spec", "protocolVersion")
			case "no rollback fence":
				_ = unstructured.SetNestedField(move.Object, int64(0), "status", "rollbackRequiredGeneration")
			case "missing source plan":
				unstructured.RemoveNestedField(move.Object, "status", "sourceCopy")
			case "missing binding plan":
				unstructured.RemoveNestedField(move.Object, "status", "persistentVolumeClaimUID")
			case "unreleased hold":
				_ = unstructured.SetNestedField(move.Object, true, "status", "capacityApproved")
			case "wrong recovery owner":
				_ = unstructured.SetNestedField(move.Object, "worker-b", "status", "recoveryOwner")
			case "unfinished recovery":
				_ = unstructured.SetNestedField(move.Object, "Retiring", "status", "recoveryPhase")
			case "cleanup without receipt":
				incoming, _, _ := unstructured.NestedMap(move.Object, "status", "incomingCopy")
				var target volume.CopyIdentity
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(incoming, &target); err != nil {
					t.Fatal(err)
				}
				spec := cleanupapi.Spec{OperationID: volumeapi.MoveRollbackOperationID(string(move.GetUID())), Target: target, Reason: "MoveRollback",
					Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.GetName(), UID: string(move.GetUID())}}
				_ = unstructured.SetNestedMap(move.Object, map[string]any{"spec": encode(t, &spec), "status": map[string]any{"phase": cleanupapi.PhaseCompleted}}, "status", "cleanup")
			}
			f := newFixture(t, object, move)
			if scenario == "live executor" {
				controller := true
				job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "copy-job", Namespace: testNamespace, UID: "copy-job-uid",
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.GetName(), UID: move.GetUID(), Controller: &controller}}}}
				if err := f.kube.Tracker().Add(job); err != nil {
					t.Fatal(err)
				}
			}
			beforeMove, beforeVolume := move.DeepCopy(), object.DeepCopy()
			report, err := f.checker.Check(context.Background())
			if scenario == "settled" {
				if err != nil || !report.Complete || !report.Safe() {
					t.Fatalf("prepared no-purge settlement blocked upgrade: %+v %v", report, err)
				}
			} else if report.Safe() || err == nil && (!report.Complete || len(report.Blockers) == 0) {
				t.Fatalf("unsafe recovery approved: %+v %v", report, err)
			}
			afterMove, getErr := f.dynamic.Resource(volumeapi.MoveResource).Get(context.Background(), move.GetName(), metav1.GetOptions{})
			if getErr != nil || !reflect.DeepEqual(beforeMove.Object, afterMove.Object) {
				t.Fatalf("inspection mutated recovery history: %v", getErr)
			}
			afterVolume, getErr := f.dynamic.Resource(volumeapi.VolumeResource).Get(context.Background(), testID, metav1.GetOptions{})
			if getErr != nil || !reflect.DeepEqual(beforeVolume.Object, afterVolume.Object) {
				t.Fatalf("inspection mutated source authority: %v", getErr)
			}
			assertReadOnly(t, f)
		})
	}
}

func preparedNoArtifactMove(t *testing.T, ready *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	sourceMap, _, err := unstructured.NestedMap(ready.Object, "status", "currentCopy")
	if err != nil {
		t.Fatal(err)
	}
	var source volume.CopyIdentity
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(sourceMap, &source); err != nil {
		t.Fatal(err)
	}
	incoming, destination := volumeapi.MoveTransactionCopies("move-uid", testID, volumeapi.MoveDestinationAnchor{
		InstallationID: source.InstallationID, PoolName: "pool-b", PoolUID: "pool-b-uid", NodeName: "worker-b", VolumeUID: source.VolumeUID,
	})
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove",
		"metadata": map[string]any{"name": "move-a", "uid": "move-uid", "resourceVersion": "8"},
		"spec":     map[string]any{"protocolVersion": volumeapi.MoveProtocolPreparedV1, "volumeID": testID, "sourceNode": source.NodeName, "recovery": "ResumeOwner"},
		"status": map[string]any{
			"phase": "Blocked", "recoveryPhase": "Recovered", "recoveryOwner": source.NodeName,
			"capacityApproved": false, "capacityReason": "RecoverySettled", "rollbackRequiredGeneration": int64(3), "sourceBytes": int64(10),
			"persistentVolumeName": "pv-a", "persistentVolumeUID": "pv-uid", "persistentVolumeClaimNamespace": "workloads", "persistentVolumeClaimName": "claim-a", "persistentVolumeClaimUID": "claim-uid",
			"consumerName": "consumer-a", "consumerUID": "consumer-uid", "placementPodUID": "placement-uid",
			"destinationNode": destination.NodeName, "destinationPoolUID": destination.PoolUID,
			"sourceCopy": sourceMap, "incomingCopy": encode(t, &incoming), "destinationCopy": encode(t, &destination),
			"copyJobName": "copy-job", "copyOperationID": volumeapi.MoveCopyOperationID("move-uid"), "promotionOperationID": volumeapi.MovePromotionOperationID("move-uid"),
		},
	}}
}

func settledMove(t *testing.T) (*unstructured.Unstructured, cleanupapi.Cleanup) {
	t.Helper()
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-a-uid", VolumeID: testID, VolumeUID: "volume-uid", NodeName: "worker-a", Role: volume.RoleServing, CopyID: "original-copy"}
	incoming, destination := volumeapi.MoveTransactionCopies("move-uid", testID, volumeapi.MoveDestinationAnchor{InstallationID: "installation", PoolName: "pool-b", PoolUID: "pool-b-uid", NodeName: "worker-b", VolumeUID: "volume-uid"})
	move := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": "move-a", "uid": "move-uid", "finalizers": []any{volumeapi.MoveProtectionFinalizer}},
		"spec": map[string]any{"volumeID": testID, "sourceNode": "worker-a"},
		"status": map[string]any{"phase": "Succeeded", "destinationNode": "worker-b", "destinationPoolUID": "pool-b-uid", "sourceBytes": int64(10), "capacityApproved": true,
			"sourceCopy": encode(t, &source), "incomingCopy": encode(t, &incoming), "destinationCopy": encode(t, &destination),
			"copyOperationID": volumeapi.MoveCopyOperationID("move-uid"), "promotionOperationID": volumeapi.MovePromotionOperationID("move-uid")},
	}}
	f := newFixture(t, move)
	store := f.checker.Cleanups.(*cleanupapi.Store)
	ctx := context.Background()
	current, err := store.Ensure(ctx, cleanupapi.Spec{OperationID: volumeapi.MoveCleanupOperationID("move-uid"), Target: source, Reason: "MoveSource", Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: "move-a", UID: "move-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	status := cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: &cleanupapi.Executor{NodeName: "worker-a", JobName: "cleanup-job", JobUID: "job-uid", PodUID: "cleanup-pod-uid"}}
	write := func() {
		t.Helper()
		if err := store.UpdateStatus(ctx, current, status); err != nil {
			t.Fatal(err)
		}
		current, err = store.Get(ctx, current.Spec.Authority)
		if err != nil {
			t.Fatal(err)
		}
	}
	write()
	status.Phase = cleanupapi.PhaseVerifying
	status.Receipt = &cleanupapi.Receipt{OperationID: current.Spec.OperationID, ExecutorUID: "job-uid", ObservedAt: testTime, Retired: true, Purged: true, LocalReceiptDigest: strings.Repeat("a", 64)}
	write()
	status.Phase = cleanupapi.PhaseConfirmingAbsence
	status.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "scan-request", PoolName: "pool-a", PoolUID: "pool-a-uid", RequiredGeneration: 4}
	write()
	status.Phase = cleanupapi.PhaseCompleted
	status.SettledAt = testTime
	status.AbsenceProof.ObservedGeneration = 5
	status.AbsenceProof.Valid = true
	status.AbsenceProof.Complete = true
	status.AbsenceProof.Absent = true
	status.AbsenceProof.ConfirmedAt = testTime
	write()
	move, err = f.dynamic.Resource(volumeapi.MoveResource).Get(ctx, "move-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	move.SetFinalizers(nil)
	return move, current
}
