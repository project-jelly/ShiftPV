package helperpod

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func usageFixture(t *testing.T) (*Runner, *fake.Clientset, volumeapi.Move, volume.CopyIdentity, volumeapi.Pool) {
	t.Helper()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "usage-pod-uid"
		return true, pod, client.Tracker().Add(pod)
	})
	copy := creationIdentity()
	move := volumeapi.Move{Name: "move-a", UID: "move-uid", Finalizers: []string{volumeapi.MoveProtectionFinalizer},
		Spec: volumeapi.MoveSpec{VolumeID: copy.VolumeID, SourceNode: copy.NodeName}, Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity"}}
	pool := volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName,
		Generation: 7, MountPath: "/mnt/pool-a", PoolGroup: "default", CapacityLimit: "128Mi"}
	runner := validRunner(client)
	runner.Pools = fakePoolResolver{pool: pool}
	return &runner, client, move, copy, pool
}

func usagePod(t *testing.T, r *Runner, move volumeapi.Move) *corev1.Pod {
	t.Helper()
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(context.Background(), sourceUsagePodName(move.UID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func storeUsagePod(t *testing.T, client *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
		t.Fatal(err)
	}
}

func TestSourceUsageRetainsOnePodAcrossShortCallsRestartAndScanEpoch(t *testing.T) {
	runner, client, move, copy, pool := usageFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if bytes, ready, err := runner.AdvanceSourceUsage(ctx, move, copy); err != nil || ready || bytes != 0 {
		t.Fatalf("initial pending=%d %v %v", bytes, ready, err)
	}
	original := usagePod(t, runner, move)
	if original.Spec.Containers[0].VolumeMounts[0].ReadOnly != true || original.OwnerReferences[0].UID != "move-uid" ||
		original.Labels[SourceUsagePurposeLabel] != "true" {
		t.Fatalf("measurement identity/readonly shape missing: %+v", original)
	}
	pool.Generation += 5
	restarted := *runner
	restarted.Pools = fakePoolResolver{pool: pool}
	for i := 0; i < 3; i++ {
		if bytes, ready, err := restarted.AdvanceSourceUsage(ctx, move, copy); err != nil || ready || bytes != 0 {
			t.Fatalf("scan-only retry=%d %v %v", bytes, ready, err)
		}
	}
	if got := usagePod(t, runner, move); !reflect.DeepEqual(got, original) {
		t.Fatalf("scan refresh changed measurement Pod: %+v", got)
	}
	completed := original.DeepCopy()
	completed.Status.Phase = corev1.PodSucceeded
	completed.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "operation", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "12345\n"}}}}
	storeUsagePod(t, client, completed)
	for i := 0; i < 2; i++ {
		bytes, ready, err := restarted.AdvanceSourceUsage(ctx, move, copy)
		if err != nil || !ready || bytes != 12345 {
			t.Fatalf("durable result lost across retry: %d %v %v", bytes, ready, err)
		}
	}
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			creates++
		}
		if action.GetVerb() == "delete" {
			t.Fatal("result deleted before SourceBytes was recorded")
		}
	}
	if creates != 1 {
		t.Fatalf("measurement incarnations=%d", creates)
	}
	move.Status.SourceBytes, move.Status.SourceCopy = 12345, &copy
	restarted.Pools, restarted.Image = nil, ""
	if done, err := restarted.FinalizeSourceUsage(ctx, move); err != nil || !done {
		t.Fatalf("finalization depended on Pool/image: done=%v err=%v", done, err)
	}
}

func TestSourceUsageAcceptedCreateResponseLossReusesExactPod(t *testing.T) {
	runner, client, move, copy, _ := usageFixture(t)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "accepted-uid"
		if err := client.Tracker().Add(pod); err != nil {
			return true, nil, err
		}
		return true, nil, apierrors.NewTimeoutError("accepted response lost", 1)
	})
	for i := 0; i < 2; i++ {
		if _, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil || ready {
			t.Fatalf("accepted running Pod not recovered: ready=%v err=%v", ready, err)
		}
	}
	if pod := usagePod(t, runner, move); pod.UID != "accepted-uid" {
		t.Fatal("Pod identity changed")
	}
}

func TestSourceUsageRetiresOldConfigurationOrProfileBeforeRetry(t *testing.T) {
	for _, change := range []string{"group", "limit", "path", "image", "resources", "staleness", "timeout"} {
		t.Run(change, func(t *testing.T) {
			runner, client, move, copy, pool := usageFixture(t)
			if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
				t.Fatal(err)
			}
			before := usagePod(t, runner, move)
			switch change {
			case "group":
				pool.PoolGroup = "other"
			case "limit":
				pool.CapacityLimit = "256Mi"
			case "path":
				pool.MountPath = "/mnt/reconfigured"
			case "image":
				runner.Image = "new:version"
			case "resources":
				runner.Resources.Claims = []corev1.ResourceClaim{{Name: "changed-profile"}}
			case "staleness":
				runner.PoolReadinessStaleAfter = time.Hour
			case "timeout":
				runner.Timeout = time.Minute
			}
			runner.Pools = fakePoolResolver{pool: pool}
			client.ClearActions()
			if _, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil || ready {
				t.Fatalf("old profile not retired: ready=%v err=%v", ready, err)
			}
			deleted := false
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" {
					t.Fatal("replacement created before separate absence observation")
				}
				if action.GetVerb() == "delete" {
					deleted = true
					p := action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions
					if p == nil || p.UID == nil || *p.UID != before.UID {
						t.Fatal("deletion lacks exact Pod UID")
					}
				}
			}
			if !deleted {
				t.Fatal("old Pod not retired")
			}
		})
	}
}

func TestSourceUsageRejectsForeignOrUnsafeStoredPodWithoutDeleting(t *testing.T) {
	for _, change := range []string{"owner UID", "copy", "snapshot envFrom", "snapshot annotation", "command", "writable", "duplicate writable mount", "hostpath", "sidecar", "privileged", "deadline missing", "deadline zero"} {
		t.Run(change, func(t *testing.T) {
			runner, client, move, copy, _ := usageFixture(t)
			if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
				t.Fatal(err)
			}
			pod := usagePod(t, runner, move)
			switch change {
			case "owner UID":
				pod.OwnerReferences[0].UID = "other-move"
			case "copy":
				move.Spec.VolumeID = "shiftpv-abcdef0123456789abcdef0123456789"
			case "snapshot envFrom":
				pod.Spec.Containers[0].Env[0].ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
			case "snapshot annotation":
				pod.Annotations = map[string]string{sourceUsageSnapshotEnv: pod.Spec.Containers[0].Env[0].Value}
				pod.Spec.Containers[0].Env = nil
			case "command":
				pod.Spec.Containers[0].Command[1] = "create"
			case "writable":
				pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = false
			case "duplicate writable mount":
				pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "pool", MountPath: "/writable"})
			case "hostpath":
				pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "foreign", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
			case "sidecar":
				pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar"})
			case "privileged":
				pod.Spec.Containers[0].SecurityContext.Privileged = boolPtr(true)
			case "deadline missing":
				pod.Spec.ActiveDeadlineSeconds = nil
			case "deadline zero":
				pod.Spec.ActiveDeadlineSeconds = int64Ptr(0)
			}
			storeUsagePod(t, client, pod)
			client.ClearActions()
			if done, err := runner.FinalizeSourceUsage(context.Background(), move); err == nil || done {
				t.Fatalf("unsafe/foreign Pod accepted: done=%v err=%v", done, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("unsafe/foreign Pod was deleted")
				}
			}
		})
	}
}

func TestSourceUsageFailedPodWithoutTerminationIsRetired(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing status", true: "waiting container"}[waiting], func(t *testing.T) {
			runner, client, move, copy, _ := usageFixture(t)
			if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
				t.Fatal(err)
			}
			pod := usagePod(t, runner, move)
			pod.Status.Phase, pod.Status.Reason = corev1.PodFailed, "DeadlineExceeded"
			if waiting {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "operation", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}
			}
			storeUsagePod(t, client, pod)
			client.ClearActions()
			if _, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); ready || !isRetryable(err) {
				t.Fatalf("failed measurement accepted: ready=%v err=%v", ready, err)
			}
			if _, err := client.CoreV1().Pods(runner.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("failed measurement was not retired: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" {
					t.Fatal("measurement was recreated in the retirement pass")
				}
				if action.GetVerb() == "delete" {
					pre := action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions
					if pre == nil || pre.UID == nil || *pre.UID != pod.UID {
						t.Fatal("failed measurement deletion lacked exact UID")
					}
				}
			}
			if _, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil || ready {
				t.Fatalf("failed measurement did not allow a later retry: ready=%v err=%v", ready, err)
			}
		})
	}
}

func TestSourceUsageDeadlineIndependentOfReconcileBudget(t *testing.T) {
	runner, _, move, copy, _ := usageFixture(t)
	runner.Timeout = 1500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ready, err := runner.AdvanceSourceUsage(ctx, move, copy); err != nil || ready {
		t.Fatalf("initial measurement: ready=%v err=%v", ready, err)
	}
	pod := usagePod(t, runner, move)
	if pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds != 2 {
		t.Fatalf("helper lifetime was not the rounded execution budget: %+v", pod.Spec.ActiveDeadlineSeconds)
	}
	runner.Pools, runner.Image, runner.Timeout = nil, "", 0
	if done, err := runner.FinalizeSourceUsage(context.Background(), move); err != nil || !done {
		t.Fatalf("retirement depended on current helper deadline: done=%v err=%v", done, err)
	}
}

func TestSourceUsageOnlyAdoptsNamedSuccessfulPositiveResult(t *testing.T) {
	for _, test := range []struct {
		name, message, container string
		phase                    corev1.PodPhase
		exitCode                 int32
	}{
		{name: "empty", phase: corev1.PodSucceeded, container: "operation"},
		{name: "zero", message: "0", phase: corev1.PodSucceeded, container: "operation"},
		{name: "negative", message: "-1", phase: corev1.PodSucceeded, container: "operation"},
		{name: "overflow", message: "9223372036854775808", phase: corev1.PodSucceeded, container: "operation"},
		{name: "wrong container", message: "123", phase: corev1.PodSucceeded, container: "other"},
		{name: "failed phase", message: "123", phase: corev1.PodFailed, container: "operation"},
		{name: "failed exit", message: "123", phase: corev1.PodSucceeded, container: "operation", exitCode: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner, client, move, copy, _ := usageFixture(t)
			if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
				t.Fatal(err)
			}
			pod := usagePod(t, runner, move)
			pod.Status.Phase = test.phase
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: test.container,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: test.exitCode, Message: test.message}}}}
			storeUsagePod(t, client, pod)
			if bytes, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); bytes != 0 || ready || !isRetryable(err) {
				t.Fatalf("invalid terminal result accepted: bytes=%d ready=%v err=%v", bytes, ready, err)
			}
			if _, err := client.CoreV1().Pods(runner.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("invalid terminal result was not retired: %v", err)
			}
		})
	}
}

func TestSourceUsageFinalizationWaitsForExactTerminatingPod(t *testing.T) {
	runner, client, move, copy, _ := usageFixture(t)
	if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
		t.Fatal(err)
	}
	pod := usagePod(t, runner, move)
	now := metav1.Now()
	pod.DeletionTimestamp, pod.Finalizers = &now, []string{"test.example/retain"}
	storeUsagePod(t, client, pod)
	runner.Pools, runner.Image, runner.Timeout = nil, "", 0
	client.ClearActions()
	for i := 0; i < 2; i++ {
		if done, err := runner.FinalizeSourceUsage(context.Background(), move); err != nil || done {
			t.Fatalf("terminating Pod treated as absent: done=%v err=%v", done, err)
		}
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("terminating Pod was mutated again: %+v", action)
		}
	}
	if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name); err != nil {
		t.Fatal(err)
	}
	if done, err := runner.FinalizeSourceUsage(context.Background(), move); err != nil || !done {
		t.Fatalf("exact absence did not finish retirement: done=%v err=%v", done, err)
	}
}

func TestSourceUsageFinalizeConfirmsExactAbsenceAfterResponseLoss(t *testing.T) {
	for _, outcome := range []string{"retained", "deleted response lost", "replacement"} {
		t.Run(outcome, func(t *testing.T) {
			runner, client, move, copy, _ := usageFixture(t)
			if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
				t.Fatal(err)
			}
			pod := usagePod(t, runner, move)
			client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				pre := action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions
				if pre == nil || pre.UID == nil || *pre.UID != pod.UID {
					t.Fatal("missing exact delete precondition")
				}
				if outcome != "retained" {
					if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name); err != nil {
						return true, nil, err
					}
				}
				if outcome == "replacement" {
					replacement := pod.DeepCopy()
					replacement.UID = "replacement-uid"
					if err := client.Tracker().Add(replacement); err != nil {
						return true, nil, err
					}
				}
				return true, nil, apierrors.NewTimeoutError("delete response lost", 1)
			})
			runner.Pools, runner.Image = nil, ""
			done, err := runner.FinalizeSourceUsage(context.Background(), move)
			if outcome == "replacement" {
				if err == nil || done {
					t.Fatalf("replacement counted as absent: %v %v", done, err)
				}
			} else if err != nil || done != (outcome == "deleted response lost") {
				t.Fatalf("absence not confirmed: done=%v err=%v", done, err)
			}
		})
	}
}

func TestSourceUsageIncompleteObservationNeverDeletes(t *testing.T) {
	runner, client, move, copy, _ := usageFixture(t)
	if _, _, err := runner.AdvanceSourceUsage(context.Background(), move, copy); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, context.DeadlineExceeded })
	if _, ready, err := runner.AdvanceSourceUsage(context.Background(), move, copy); ready || !errors.Is(err, context.DeadlineExceeded) || !isRetryable(err) {
		t.Fatalf("deadline changed into progress: ready=%v err=%v", ready, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" || action.GetVerb() == "create" {
			t.Fatal("incomplete observation recreated the measurement")
		}
	}
}
