package volumeapi

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestPoolSnapshotDistinguishesEmptyRegistrationFromReadFailure(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{PoolResource: "ShiftPVPoolList"})
	registry := &Registry{Client: client}
	ctx := context.Background()
	snapshot, err := registry.ObservePools(ctx)
	if err != nil || len(snapshot.Registered) != 0 || len(snapshot.Ready) != 0 {
		t.Fatalf("empty registry cannot withdraw published capacity: %+v %v", snapshot, err)
	}
	if _, err := registry.ReadyPools(ctx); !errors.Is(err, ErrPoolConfiguration) {
		t.Fatalf("legacy placement API lost its empty registration error: %v", err)
	}
	client.PrependReactor("list", "shiftpvpools", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("API unavailable")
	})
	if _, err := registry.ObservePools(ctx); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("API failure was mistaken for zero capacity: %v", err)
	}
}
