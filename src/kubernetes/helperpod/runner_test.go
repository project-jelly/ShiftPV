package helperpod

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const testVolumeID = "shiftpv-0123456789abcdef0123456789abcdef"

type fakePoolResolver struct {
	pool    volumeapi.Pool
	nodeErr error
}

func (f fakePoolResolver) PoolForNode(context.Context, string) (volumeapi.Pool, error) {
	return f.pool, f.nodeErr
}

func (f fakePoolResolver) PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error) {
	return f.pool, nil
}

func TestClassifyKubernetesAPIErrorMarksTransientFailuresRetryable(t *testing.T) {
	transient := apierrors.NewTimeoutError("timeout", 1)
	if !isRetryable(classifyKubernetesAPIError(transient)) {
		t.Fatal("Kubernetes timeout was not retryable")
	}
	permanent := errors.New("invalid response")
	if got := classifyKubernetesAPIError(permanent); !errors.Is(got, permanent) || isRetryable(got) {
		t.Fatalf("permanent error changed classification: %v", got)
	}
}

func TestCreateCopyRunsIdentityAwareHelper(t *testing.T) {
	client, created := clientWithPodPhase(t, corev1.PodSucceeded, "")
	runner := validRunner(client)
	runner.ServiceAccountName = "shiftpv-controller"
	runner.Pools = fakePoolResolver{pool: volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/shiftpv-a"},
		nodeErr: errors.New("node has multiple Pools")}
	identity := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: testVolumeID, VolumeUID: "volume-uid", CopyID: "copy-id",
		NodeName: "worker-a", Role: volume.RoleServing,
	}
	if err := runner.CreateCopy(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if created.pod.Spec.ServiceAccountName != "shiftpv-controller" {
		t.Fatalf("serviceAccountName=%q", created.pod.Spec.ServiceAccountName)
	}
	if got := created.pod.Spec.Volumes[0].HostPath.Path; got != "/mnt/shiftpv-a" {
		t.Fatalf("creation Pool path = %q", got)
	}
	command := created.pod.Spec.Containers[0].Command
	joined := strings.Join(command, "\x00")
	for _, expected := range []string{
		"/shiftpv-volume-helper", "create", "--operation-id=create-volume-uid", "--installation-id=installation", "--pool-name=pool-a",
		"--pool-uid=pool-uid", "--volume-id=" + testVolumeID, "--volume-uid=volume-uid",
		"--copy-id=copy-id", "--node-name=worker-a",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("identity argument %q missing from %#v", expected, command)
		}
	}
}

func TestStatFSReturnsRegisteredPoolFilesystemCapacity(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "100 25 4096 12\n")
	runner := validRunner(client)

	stats, err := runner.StatFS(context.Background(), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalBytes != 409600 || stats.AvailableBytes != 102400 || stats.AvailableInodes != 12 {
		t.Fatalf("stats = %+v", stats)
	}
	want := []string{"sh", "-c", "stat -f -c '%b %a %S %d' /pool > /dev/termination-log"}
	if got := created.pod.Spec.Containers[0].Command; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("command = %#v", got)
	}
}

func TestStatFSForPoolUsesExactPoolPathOnSharedNode(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "100 25 4096 12\n")
	runner := validRunner(client)
	pool := volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-a", MountPath: "/mnt/pool-b"}
	runner.Pools = fakePoolResolver{pool: pool, nodeErr: errors.New("node has multiple Pools")}
	if _, err := runner.StatFSForPool(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if got := created.pod.Spec.Volumes[0].HostPath.Path; got != pool.MountPath {
		t.Fatalf("statfs helper used %q, want %q", got, pool.MountPath)
	}
}

func TestStatFSRejectsMissingHelperResult(t *testing.T) {
	client, _ := clientWithPodPhase(t, corev1.PodSucceeded, "")
	runner := validRunner(client)

	if _, err := runner.StatFS(context.Background(), "worker-a"); err == nil || !isRetryable(err) {
		t.Fatalf("expected retryable result error, got %v", err)
	}
}

func TestVolumeUsageReturnsQuiescedDirectoryBytes(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "12345\n")
	runner := validRunner(client)

	bytes, err := runner.VolumeUsage(context.Background(), "worker-a", testVolumeID)
	if err != nil || bytes != 12345 {
		t.Fatalf("usage = %d, err = %v", bytes, err)
	}
	command := created.pod.Spec.Containers[0].Command
	if len(command) != 5 || command[4] != "/pool/volumes/"+testVolumeID {
		t.Fatalf("command = %#v", command)
	}
}

func TestVolumeUsageForCopyUsesExactPoolOnSharedNode(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "12345\n")
	runner := validRunner(client)
	pool := volumeapi.Pool{Name: "pool-b", UID: "pool-b-uid", NodeName: "worker-a", MountPath: "/mnt/pool-b"}
	runner.Pools = fakePoolResolver{pool: pool, nodeErr: errors.New("node has multiple Pools")}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: pool.Name, PoolUID: pool.UID,
		VolumeID: testVolumeID, VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: pool.NodeName, Role: volume.RoleServing}
	bytes, err := runner.VolumeUsageForCopy(context.Background(), copy)
	if err != nil || bytes != 12345 {
		t.Fatalf("usage=%d err=%v", bytes, err)
	}
	if got := created.pod.Spec.Volumes[0].HostPath.Path; got != pool.MountPath {
		t.Fatalf("usage helper used %q, want %q", got, pool.MountPath)
	}
}

func TestRunForResultRejectsReplacementPod(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "100 25 4096 12\n")
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		replacement := created.pod.DeepCopy()
		replacement.UID = "replacement-uid"
		return true, replacement, nil
	})
	runner := validRunner(client)

	if _, err := runner.StatFS(context.Background(), "worker-a"); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replacement helper accepted: %v", err)
	}
}

func TestRunForResultRejectsChangedExecutionSpec(t *testing.T) {
	client, created := clientWithPodTerminationMessage(t, "100 25 4096 12\n")
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		changed := created.pod.DeepCopy()
		changed.Spec.Containers[0].Command = []string{"sh", "-c", "printf forged > /dev/termination-log"}
		return true, changed, nil
	})
	runner := validRunner(client)

	if _, err := runner.StatFS(context.Background(), "worker-a"); err == nil || !strings.Contains(err.Error(), "command changed") {
		t.Fatalf("changed helper accepted: %v", err)
	}
}

func TestRunForResultDeletesOnlyCreatedUID(t *testing.T) {
	client, _ := clientWithPodTerminationMessage(t, "100 25 4096 12\n")
	var deletedUID *types.UID
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions != nil {
			deletedUID = options.Preconditions.UID
		}
		return true, nil, nil
	})
	runner := validRunner(client)

	if _, err := runner.StatFS(context.Background(), "worker-a"); err != nil {
		t.Fatal(err)
	}
	if deletedUID == nil || *deletedUID != types.UID("helper-uid") {
		t.Fatalf("helper delete UID precondition = %v", deletedUID)
	}
}

func TestSameResultPodRejectsChangedSecurityBoundary(t *testing.T) {
	runner := validRunner(fake.NewClientset())
	expected := runner.helperPod("worker-a", testVolumeID, "/mnt/shiftpv", []string{"sh", "-c", "true"})
	expected.Name, expected.Namespace, expected.UID = "helper-1", runner.Namespace, "helper-uid"

	tests := map[string]func(*corev1.Pod){
		"node": func(pod *corev1.Pod) { pod.Spec.NodeName = "worker-b" },
		"host path": func(pod *corev1.Pod) {
			pod.Spec.Volumes[0].HostPath.Path = "/other"
		},
		"security context": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].SecurityContext.RunAsUser = int64Ptr(1000)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			current := expected.DeepCopy()
			mutate(current)
			if err := sameResultPod(expected, current, expected.UID); err == nil {
				t.Fatal("changed helper accepted")
			}
		})
	}
}

func TestRunRejectsIncompleteConfiguration(t *testing.T) {
	tests := map[string]Runner{
		"relative pool": {
			Client: fake.NewClientset(), Namespace: "shiftpv-system", Pools: fakePoolResolver{pool: volumeapi.Pool{MountPath: "relative"}}, Image: "busybox", Timeout: time.Second,
		},
		"missing client": {
			Namespace: "shiftpv-system", Pools: fakePoolResolver{pool: volumeapi.Pool{MountPath: "/mnt/shiftpv"}}, Image: "busybox", Timeout: time.Second,
		},
		"missing namespace": {
			Client: fake.NewClientset(), Pools: fakePoolResolver{pool: volumeapi.Pool{MountPath: "/mnt/shiftpv"}}, Image: "busybox", Timeout: time.Second,
		},
		"missing image": {
			Client: fake.NewClientset(), Namespace: "shiftpv-system", Pools: fakePoolResolver{pool: volumeapi.Pool{MountPath: "/mnt/shiftpv"}}, Timeout: time.Second,
		},
		"invalid timeout": {
			Client: fake.NewClientset(), Namespace: "shiftpv-system", Pools: fakePoolResolver{pool: volumeapi.Pool{MountPath: "/mnt/shiftpv"}}, Image: "busybox",
		},
		"missing pool registry": {
			Client: fake.NewClientset(), Namespace: "shiftpv-system", Image: "busybox", Timeout: time.Second,
		},
	}

	for name, runner := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := runner.StatFS(context.Background(), "worker-a"); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
	valid := validRunner(fake.NewClientset())
	if _, err := valid.StatFS(context.Background(), ""); err == nil {
		t.Fatal("expected missing node error")
	}
	if _, err := valid.VolumeUsage(context.Background(), "worker-a", "../escape"); err == nil {
		t.Fatal("expected unsafe volume ID error")
	}
}

type createdPod struct {
	pod *corev1.Pod
}

func isRetryable(err error) bool {
	var retryable interface{ Retryable() bool }
	return errors.As(err, &retryable) && retryable.Retryable()
}

func clientWithPodPhase(t *testing.T, phase corev1.PodPhase, message string) (*fake.Clientset, *createdPod) {
	t.Helper()
	client := fake.NewClientset()
	captured := &createdPod{}
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		pod := create.GetObject().(*corev1.Pod).DeepCopy()
		if pod.Name == "" {
			pod.Name = "helper-1"
		}
		pod.Namespace = action.GetNamespace()
		pod.UID = "helper-uid"
		pod.Status.Phase = phase
		pod.Status.Message = message
		if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
			exitCode := int32(0)
			if phase == corev1.PodFailed {
				exitCode = 1
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "operation", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode}},
			}}
		}
		captured.pod = pod.DeepCopy()
		if err := client.Tracker().Add(pod); err != nil {
			return true, nil, err
		}
		return true, pod, nil
	})
	return client, captured
}

func clientWithPodTerminationMessage(t *testing.T, message string) (*fake.Clientset, *createdPod) {
	t.Helper()
	client := fake.NewClientset()
	captured := &createdPod{}
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		pod := create.GetObject().(*corev1.Pod).DeepCopy()
		if pod.Name == "" {
			pod.Name = "helper-1"
		}
		pod.Namespace = action.GetNamespace()
		pod.UID = "helper-uid"
		pod.Status.Phase = corev1.PodSucceeded
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "operation",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0,
				Message:  message,
			}},
		}}
		captured.pod = pod.DeepCopy()
		if err := client.Tracker().Add(pod); err != nil {
			return true, nil, err
		}
		return true, pod, nil
	})
	return client, captured
}

func validRunner(client *fake.Clientset) Runner {
	return Runner{
		Client:    client,
		Namespace: "shiftpv-system",
		Pools:     fakePoolResolver{pool: volumeapi.Pool{NodeName: "worker-a", MountPath: "/mnt/shiftpv"}},
		Image:     "busybox:1.37",
		Timeout:   time.Second,
	}
}
