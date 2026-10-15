package volumeapi

import (
	"context"
	"errors"
	"reflect"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func sourceStateFixture(t *testing.T) (*Registry, *dynamicfake.FakeDynamicClient, string, State) {
	t.Helper()
	move := preparedMoveFixture()
	id := move.Spec.VolumeID
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
		"metadata": map[string]any{"name": id, "uid": move.Status.SourceCopy.VolumeUID},
		"spec":     map[string]any{"volumeID": id},
	}}
	setState(object, State{UID: move.Status.SourceCopy.VolumeUID, Phase: PhaseReady, OwnerNode: "source", CurrentCopy: move.Status.SourceCopy})
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	registry := &Registry{Client: client}
	state, err := registry.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return registry, client, id, state
}

func TestSourceStateCASRetainsLivePublicationAndExactCopyAcrossConflict(t *testing.T) {
	for _, change := range []string{"publish", "copy", "deleting", "cleanup"} {
		t.Run(change, func(t *testing.T) {
			registry, client, id, observed := sourceStateFixture(t)
			next := observed
			next.ActiveMove = "move"
			writes := 0
			client.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
				writes++
				if writes != 1 {
					return false, nil, nil
				}
				stored, err := client.Tracker().Get(VolumeResource, "", id)
				if err != nil {
					t.Fatal(err)
				}
				object := stored.(*unstructured.Unstructured).DeepCopy()
				switch change {
				case "publish":
					_ = unstructured.SetNestedStringSlice(object.Object, []string{"source"}, "status", "publishedNodes")
				case "copy":
					_ = unstructured.SetNestedField(object.Object, "replacement-copy", "status", "currentCopy", "copyID")
				case "deleting":
					now := metav1.Now()
					object.SetDeletionTimestamp(&now)
				case "cleanup":
					_ = unstructured.SetNestedMap(object.Object, map[string]any{}, "status", "cleanup")
				}
				if err := client.Tracker().Update(VolumeResource, object, ""); err != nil {
					t.Fatal(err)
				}
				return true, nil, apierrors.NewConflict(VolumeResource.GroupResource(), id, errors.New("concurrent update"))
			})
			err := registry.CompareAndSetSourceState(context.Background(), id, observed, next)
			if change == "publish" {
				live, getErr := registry.Get(context.Background(), id)
				if err != nil || getErr != nil || live.ActiveMove != "move" || !reflect.DeepEqual(live.PublishedNodes, []string{"source"}) {
					t.Fatalf("live publication lost: state=%+v err=%v get=%v", live, err, getErr)
				}
			} else if !errors.Is(err, ErrStateConflict) || writes != 1 {
				t.Fatalf("changed authority replayed: writes=%d err=%v", writes, err)
			}
		})
	}
}

func TestSourceStateCASCannotChangeOwnerOrCopy(t *testing.T) {
	for _, change := range []string{"owner", "copy", "UID", "foreign publication"} {
		t.Run(change, func(t *testing.T) {
			registry, client, id, observed := sourceStateFixture(t)
			next := observed
			switch change {
			case "owner":
				next.OwnerNode = "destination"
			case "copy":
				copy := *next.CurrentCopy
				copy.CopyID = "other-copy"
				next.CurrentCopy = &copy
			case "UID":
				next.UID = "other-volume"
			case "foreign publication":
				setPublishedFixture(t, client, id, "destination", true)
			}
			if err := registry.CompareAndSetSourceState(context.Background(), id, observed, next); !errors.Is(err, ErrStateConflict) {
				t.Fatalf("unsafe source mutation accepted: %v", err)
			}
		})
	}
}
