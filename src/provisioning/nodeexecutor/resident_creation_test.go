package nodeexecutor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func TestResidentCreationUnavailablePreservesIntentUntilCapabilityRecovers(t *testing.T) {
	for _, unavailable := range []string{"absent capability", "unknown capability", "NotReady", "missing Pod"} {
		t.Run(unavailable, func(t *testing.T) {
			client, node, api, kube, initial := fixture(t)
			ctx := context.Background()
			pod, err := kube.CoreV1().Pods(node.Identity.Namespace).Get(ctx, node.Identity.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			available := pod.DeepCopy()
			switch unavailable {
			case "absent capability":
				delete(pod.Annotations, volumeapi.NodeEffectsAnnotation)
			case "unknown capability":
				pod.Annotations[volumeapi.NodeEffectsAnnotation] = "v2"
			case "NotReady":
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
			}
			if unavailable == "missing Pod" {
				err = kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			} else {
				_, err = kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			api.ClearActions()
			kube.ClearActions()
			creationErr := client.CreateCopy(ctx, *initial.CurrentCopy)
			var retryable interface{ Retryable() bool }
			if creationErr == nil || !errors.As(creationErr, &retryable) || !retryable.Retryable() || errors.Is(creationErr, ErrLegacyExecution) {
				t.Fatalf("unavailable resident executor did not retry: %v", creationErr)
			}
			after, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("unavailable executor changed intent or reservation: before=%+v after=%+v err=%v", before, after, err)
			}
			assertResidentCreationReads(t, api.Actions())
			assertResidentCreationReads(t, kube.Actions())
			if unavailable == "missing Pod" {
				_, err = kube.CoreV1().Pods(available.Namespace).Create(ctx, available, metav1.CreateOptions{})
			} else {
				_, err = kube.CoreV1().Pods(available.Namespace).Update(ctx, available, metav1.UpdateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			kube.ClearActions()
			if err := client.CreateCopy(ctx, *initial.CurrentCopy); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("restored capability did not bind and wait on original creation: %v", err)
			}
			bound, err := client.Volumes.Get(ctx, testID)
			if err != nil || bound.Phase != volumeapi.PhaseNodeCreating || bound.CreationExecutor == nil || *bound.CreationExecutor != node.Identity || bound.CreationReceipt != nil {
				t.Fatalf("restored executor did not retain exact Node binding: state=%+v err=%v", bound, err)
			}
			assertResidentCreationIdentity(t, initial, bound)
			if err := client.CreateCopy(ctx, *initial.CurrentCopy); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("watch retry changed backend: %v", err)
			}
			again, err := client.Volumes.Get(ctx, testID)
			if err != nil || !reflect.DeepEqual(bound, again) {
				t.Fatalf("watch timeout changed binding or reservation: before=%+v after=%+v err=%v", bound, again, err)
			}
			assertResidentCreationReads(t, kube.Actions())
		})
	}
}

func TestResidentCreationReadyLegacyHistoryDoesNotReplay(t *testing.T) {
	for _, helperPresent := range []bool{false, true} {
		t.Run(fmt.Sprintf("helperPresent=%v", helperPresent), func(t *testing.T) {
			client, node, api, kube, initial := fixture(t)
			ctx := context.Background()
			if err := client.Volumes.CompleteCreate(ctx, testID, initial.UID, *initial.CurrentCopy); err != nil {
				t.Fatal(err)
			}
			ready, err := client.Volumes.Get(ctx, testID)
			if err != nil || ready.Phase != volumeapi.PhaseReady || ready.CreationExecutor != nil || ready.CreationReceipt != nil {
				t.Fatalf("legacy Ready fixture: state=%+v err=%v", ready, err)
			}
			probe := &residentCreationRPC{registry: client.Volumes, expected: ready, executor: node.Identity}
			attachCleanupRPC(t, moveClientFixture{client: client, node: node, dynamic: api, kube: kube}, probe)
			if helperPresent {
				sum := sha256.Sum256([]byte(ready.CreationOperationID + "\x00" + ready.CurrentCopy.CopyID))
				_, err := kube.CoreV1().Pods(node.Identity.Namespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("shiftpv-create-%x", sum[:16]), Namespace: node.Identity.Namespace, UID: "legacy-create-pod-uid",
				}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			api.ClearActions()
			kube.ClearActions()
			for _, call := range []func(context.Context) error{
				func(ctx context.Context) error { return client.CreateCopy(ctx, *ready.CurrentCopy) },
				func(ctx context.Context) error { return client.FinalizeCreate(ctx, *ready.CurrentCopy) },
			} {
				if err := call(ctx); helperPresent && !errors.Is(err, ErrLegacyExecution) || !helperPresent && err != nil {
					t.Fatalf("legacy Ready reentry: helper=%v err=%v", helperPresent, err)
				}
			}
			after, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(before, after) || probe.calls.Load() != 0 {
				t.Fatalf("Ready history replayed or changed: calls=%d before=%+v after=%+v err=%v", probe.calls.Load(), before, after, err)
			}
			assertResidentCreationReads(t, api.Actions())
			assertResidentCreationReads(t, kube.Actions())
		})
	}
}

func TestResidentCreationAcceptedBindingResponseLossDoesNotDispatch(t *testing.T) {
	client, node, api, kube, initial := fixture(t)
	ctx := context.Background()
	probe := &residentCreationRPC{registry: client.Volumes, expected: initial, executor: node.Identity}
	attachCleanupRPC(t, moveClientFixture{client: client, node: node, dynamic: api, kube: kube}, probe)
	bindings := 0
	lost := false
	api.PrependReactor("update", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		_, bound, _ := unstructured.NestedMap(object.Object, "status", "creationExecutor")
		_, receipted, _ := unstructured.NestedMap(object.Object, "status", "creationReceipt")
		if !bound || receipted {
			return false, nil, nil
		}
		bindings++
		if lost {
			return false, nil, nil
		}
		lost = true
		if err := api.Tracker().Update(volumeapi.VolumeResource, object.DeepCopy(), ""); err != nil {
			return true, nil, err
		}
		return true, nil, apierrors.NewTimeoutError("creation binding accepted, response lost", 1)
	})
	api.ClearActions()
	kube.ClearActions()
	if err := client.CreateCopy(ctx, *initial.CurrentCopy); !apierrors.IsTimeout(err) || probe.calls.Load() != 0 || bindings != 1 {
		t.Fatalf("effect followed ambiguous creation binding: calls=%d bindings=%d err=%v", probe.calls.Load(), bindings, err)
	}
	bound, err := client.Volumes.Get(ctx, testID)
	if err != nil || bound.CreationExecutor == nil || *bound.CreationExecutor != node.Identity || bound.CreationReceipt != nil || bound.Phase != volumeapi.PhaseNodeCreating {
		t.Fatalf("accepted binding was lost: state=%+v err=%v", bound, err)
	}
	assertResidentCreationIdentity(t, initial, bound)
	assertResidentCreationReads(t, kube.Actions())
	if err := client.CreateCopy(ctx, *initial.CurrentCopy); err != nil {
		t.Fatalf("fresh retry did not reuse accepted binding: %v", err)
	}
	settled, err := client.Volumes.Get(ctx, testID)
	if err != nil || !volumeapi.ValidCreationReceipt(settled) || probe.calls.Load() != 1 || bindings != 1 || !reflect.DeepEqual(settled.CreationExecutor, bound.CreationExecutor) {
		t.Fatalf("retry rebound or replayed creation: state=%+v calls=%d bindings=%d err=%v", settled, probe.calls.Load(), bindings, err)
	}
	assertResidentCreationIdentity(t, initial, settled)
	if err := client.CreateCopy(ctx, *initial.CurrentCopy); err != nil || probe.calls.Load() != 1 || bindings != 1 {
		t.Fatalf("stored receipt did not end dispatch: calls=%d bindings=%d err=%v", probe.calls.Load(), bindings, err)
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" && !(action.GetVerb() == "create" && action.GetResource().Resource == "tokenreviews") {
			t.Fatalf("creation retry mutated a helper: %+v", action)
		}
	}
}

type residentCreationRPC struct {
	registry *volumeapi.Registry
	expected volumeapi.State
	executor volumeapi.NodeExecutor
	calls    atomic.Int32
}

func (p *residentCreationRPC) Execute(ctx context.Context, request *protocol.EffectRequest, operation protocol.Operation) error {
	p.calls.Add(1)
	if operation != protocol.Operation_CREATE || request.VolumeName != p.expected.CurrentCopy.VolumeID || request.VolumeUid != p.expected.UID ||
		request.OperationId != p.expected.CreationOperationID || request.ExecutorUid != p.executor.PodUID || request.NodeName != p.executor.NodeName {
		return fmt.Errorf("creation RPC changed approved operation or executor")
	}
	current, err := p.registry.Get(ctx, request.VolumeName)
	if err != nil {
		return err
	}
	if current.CreationExecutor == nil || *current.CreationExecutor != p.executor || current.CurrentCopy == nil || *current.CurrentCopy != *p.expected.CurrentCopy || current.CapacityBytes != p.expected.CapacityBytes {
		return volumeapi.ErrStateConflict
	}
	return p.registry.RecordCreationReceipt(ctx, current, creationReceipt(p.executor))
}

func assertResidentCreationIdentity(t *testing.T, expected, current volumeapi.State) {
	t.Helper()
	if current.UID != expected.UID || current.CurrentCopy == nil || !reflect.DeepEqual(current.CurrentCopy, expected.CurrentCopy) ||
		current.CapacityBytes != expected.CapacityBytes || current.CreationOperationID != expected.CreationOperationID ||
		current.OwnerNode != expected.OwnerNode || !reflect.DeepEqual(current.InitialPool, expected.InitialPool) ||
		!reflect.DeepEqual(current.Finalizers, expected.Finalizers) {
		t.Fatalf("resident creation changed approved identity or capacity: before=%+v after=%+v", expected, current)
	}
}

func assertResidentCreationReads(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("resident creation issued a mutation: %+v", action)
		}
	}
}
