package volumeapi

import (
	"context"
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestBeginDeleteRechecksAdmissionAfterCASConflict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(*State)
		published bool
	}{
		{"new publication", func(s *State) { s.PublishedNodes = []string{s.OwnerNode} }, true},
		{"new Move", func(s *State) { s.ActiveMove = "new-move" }, false},
		{"replacement UID", func(s *State) { s.UID = "replacement" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": copy.VolumeID, "uid": copy.VolumeUID}, "spec": map[string]any{"volumeID": copy.VolumeID}}}
			state := State{UID: copy.VolumeUID, Phase: PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy}
			setState(object, state)
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{VolumeResource: "ShiftPVVolumeList"}, object)
			updates := 0
			client.PrependReactor("update", "shiftpvvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
				updates++
				// Another writer wins after the first read, before its CAS write.
				changed := object.DeepCopy()
				tc.change(&state)
				changed.SetUID(types.UID(state.UID))
				setState(changed, state)
				if err := client.Tracker().Update(VolumeResource, changed, ""); err != nil {
					t.Fatal(err)
				}
				return true, nil, apierrors.NewConflict(VolumeResource.GroupResource(), copy.VolumeID, errors.New("concurrent writer"))
			})
			registry := &Registry{Client: client}
			_, err := registry.BeginDelete(context.Background(), copy.VolumeID, copy.VolumeUID, copy)
			if !errors.Is(err, ErrStateConflict) || errors.Is(err, ErrVolumePublished) != tc.published || updates != 1 {
				t.Fatalf("stale approval survived CAS retry: err=%v updates=%d", err, updates)
			}
			live, err := registry.Get(context.Background(), copy.VolumeID)
			if err != nil || live.Phase != PhaseReady || live.DeletionOperationID != "" {
				t.Fatalf("rejected retry persisted a fence: %#v %v", live, err)
			}
		})
	}
}
