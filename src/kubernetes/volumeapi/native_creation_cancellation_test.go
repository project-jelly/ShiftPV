package volumeapi

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func nativeCancellationFixture(t *testing.T) (*Registry, *dynamicfake.FakeDynamicClient, State) {
	t.Helper()
	object := newVolumeObject(unstartedCancellationID, "pvc-original", "worker-a", 64, Pool{Name: "pool-a", UID: "pool-uid"})
	object.SetUID("volume-uid")
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: object.GetName(), VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "worker-a", Role: volume.RoleServing}
	state := State{UID: "volume-uid", Phase: PhasePending, OwnerNode: "worker-a", CurrentCopy: &copy, CreationOperationID: "create-volume-uid"}
	setState(object, state)
	api := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
	r := &Registry{Client: api}
	current, err := r.Get(context.Background(), object.GetName())
	if err != nil {
		t.Fatal(err)
	}
	return r, api, current
}

func TestCreationCancellationPreservesIdentityAndReplaysAcceptedFence(t *testing.T) {
	r, api, initial := nativeCancellationFixture(t)
	lost := false
	api.PrependReactor("update", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
		if phase == PhaseDeleting && !lost {
			lost = true
			if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
				t.Fatal(err)
			}
			return true, nil, apierrors.NewTimeoutError("accepted cancellation response lost", 1)
		}
		return false, nil, nil
	})
	ctx := context.Background()
	if _, err := r.BeginCreationCancellation(ctx, unstartedCancellationID, initial.UID, *initial.CurrentCopy); !apierrors.IsTimeout(err) {
		t.Fatal(err)
	}
	r = &Registry{Client: api}
	current, err := r.BeginCreationCancellation(ctx, unstartedCancellationID, initial.UID, *initial.CurrentCopy)
	if err != nil || !current.CancellingCreation() || current.CapacityBytes != initial.CapacityBytes || current.OwnerNode != initial.OwnerNode || !reflect.DeepEqual(current.CurrentCopy, initial.CurrentCopy) || current.CreationReceipt != nil {
		t.Fatalf("cancellation fence changed authority or invented completion: %+v err=%v", current, err)
	}
	if err := r.CompleteCreate(ctx, unstartedCancellationID, initial.UID, *initial.CurrentCopy); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("late creator crossed cancellation fence: %v", err)
	}
	if _, err := r.BeginCreateInPool(ctx, unstartedCancellationID, initial.RequestName, initial.InitialNode, initial.CapacityBytes, "pool-a", "pool-uid"); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("late create retry crossed cancellation fence: %v", err)
	}
}

func TestCreationCancellationReobservesCompletionWinnerAfterCASConflict(t *testing.T) {
	r, api, initial := nativeCancellationFixture(t)
	updates := 0
	api.PrependReactor("update", "shiftpvvolumes", func(ktesting.Action) (bool, runtime.Object, error) {
		updates++
		object, _ := api.Tracker().Get(VolumeResource, "", unstartedCancellationID)
		winner := object.(*unstructured.Unstructured).DeepCopy()
		_ = unstructured.SetNestedField(winner.Object, PhaseReady, "status", "phase")
		if err := api.Tracker().Update(VolumeResource, winner, ""); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewConflict(VolumeResource.GroupResource(), winner.GetName(), errors.New("completion won"))
	})
	if _, err := r.BeginCreationCancellation(context.Background(), unstartedCancellationID, initial.UID, *initial.CurrentCopy); !errors.Is(err, ErrStateConflict) || updates != 1 {
		t.Fatalf("stale cancellation deleted the Ready winner: err=%v updates=%d", err, updates)
	}
	current, _ := r.Get(context.Background(), unstartedCancellationID)
	if current.Phase != PhaseReady || current.DeletionOperationID != "" || current.HasCleanup {
		t.Fatalf("completion winner changed: %+v", current)
	}
}

func TestCreationCancellationRejectsUnknownAuthority(t *testing.T) {
	for name, change := range map[string]func(*State){
		"owner":                func(s *State) { s.OwnerNode = "worker-b" },
		"initial Pool":         func(s *State) { s.InitialPool.UID = "other-pool" },
		"move":                 func(s *State) { s.ActiveMove = "another-move" },
		"publication":          func(s *State) { s.PublishedNodes = []string{"worker-a"} },
		"creation operation":   func(s *State) { s.CreationOperationID = "create-other" },
		"copy ID":              func(s *State) { s.CurrentCopy.CopyID = "moved-copy" },
		"copy role":            func(s *State) { s.CurrentCopy.Role = volume.RoleIncoming },
		"Ready":                func(s *State) { s.Phase = PhaseReady },
		"unbound NodeCreating": func(s *State) { s.Phase = PhaseNodeCreating },
		"invalid receipt":      func(s *State) { s.CreationReceipt = &CreationReceipt{} },
	} {
		t.Run(name, func(t *testing.T) {
			r, api, initial := nativeCancellationFixture(t)
			copy := *initial.CurrentCopy
			change(&initial)
			object, _ := api.Resource(VolumeResource).Get(context.Background(), unstartedCancellationID, metav1.GetOptions{})
			setState(object, initial)
			if name == "initial Pool" {
				_ = unstructured.SetNestedField(object.Object, initial.InitialPool.UID, "spec", "initialPool", "uid")
			}
			if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
				t.Fatal(err)
			}
			api.ClearActions()
			if _, err := r.BeginCreationCancellation(context.Background(), unstartedCancellationID, initial.UID, copy); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("unknown authority cancelled: %v", err)
			}
			for _, a := range api.Actions() {
				if a.GetVerb() != "get" {
					t.Fatalf("rejection wrote state: %v", a)
				}
			}
		})
	}
}

func TestCreationCancellationRetainsCompletedNativeReceipt(t *testing.T) {
	r, api, initial := nativeCancellationFixture(t)
	initial.Phase = PhaseNodeCreating
	initial.CreationExecutor = &NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-a", NodeName: "worker-a"}
	initial.CreationReceipt = &CreationReceipt{OperationID: initial.CreationOperationID, ExecutorUID: "pod-a", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), LocalReceiptDigest: strings.Repeat("a", 64)}
	object, _ := api.Resource(VolumeResource).Get(context.Background(), unstartedCancellationID, metav1.GetOptions{})
	setState(object, initial)
	if err := api.Tracker().Update(VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	current, err := r.BeginCreationCancellation(context.Background(), unstartedCancellationID, initial.UID, *initial.CurrentCopy)
	if err != nil || !reflect.DeepEqual(current.CreationReceipt, initial.CreationReceipt) || !current.CancellingCreation() {
		t.Fatalf("native history changed: %+v err=%v", current, err)
	}
	if err := r.RecordCreationReceipt(context.Background(), initial, *initial.CreationReceipt); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("late receipt crossed cancellation: %v", err)
	}
}
