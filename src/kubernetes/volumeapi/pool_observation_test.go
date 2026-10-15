package volumeapi

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func TestPoolObservationGenerationConflictPreservesIdentityErrors(t *testing.T) {
	for _, tc := range []struct {
		name, uid, node string
		approved, stale bool
	}{
		{"generation only", "uid", "node", false, true},
		{"UID replaced", "old-uid", "node", false, false},
		{"wrong node", "uid", "other", false, false},
		{"approval removed", "uid", "node", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := probePoolFixture()
			if tc.approved {
				object.Object["status"].(map[string]any)["registrationApproved"] = true
			}
			api := fake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			registry := &Registry{Client: api}
			err := registry.SetPoolStatus(context.Background(), "pool", tc.uid, tc.node, PoolStatus{ObservedGeneration: 1})
			if !errors.Is(err, ErrStateConflict) || errors.Is(err, ErrPoolObservationChanged) != tc.stale {
				t.Fatalf("error=%v stale=%v", err, tc.stale)
			}
			for _, action := range api.Actions() {
				if action.GetVerb() == "update" {
					t.Fatal("stale or foreign observation wrote status")
				}
			}
		})
	}
}
