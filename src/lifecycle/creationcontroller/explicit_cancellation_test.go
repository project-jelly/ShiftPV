package creationcontroller

import (
	"context"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestExplicitUnknownOriginCancellationResumesAfterRestart(t *testing.T) {
	r, api, registry, effects := fixture(t)
	ctx := context.Background()
	object, err := api.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(object.Object, "spec", "creationClaim")
	if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
		t.Fatal(err)
	}
	before, err := registry.Get(ctx, testID)
	if err != nil {
		t.Fatal(err)
	}
	// Missing provenance grants no automatic cancellation authority.
	if done, err := r.Reconcile(ctx, testID, before.UID); !done || err != nil || *effects != 0 {
		t.Fatal(done, err, *effects)
	}
	// An explicit exact-UID call races completion on the shared Volume CAS.
	if done, err := r.Cancel.Advance(ctx, testID, before.UID); done || err != nil {
		t.Fatal(done, err)
	}
	fenced, err := registry.Get(ctx, testID)
	if err != nil || !fenced.CancellingCreation() || fenced.CreationClaim != nil {
		t.Fatal(fenced, err)
	}
	intents, err := registry.ListCreationIntents(ctx)
	if err != nil || len(intents) != 1 || intents[0].Name != testID || intents[0].UID != before.UID {
		t.Fatal("the restart scan lost explicit cancellation authority", intents, err)
	}
	restarted := *r
	restarted.Claims = nil // The durable cancellation intent owns progression.
	if done, err := restarted.Reconcile(ctx, testID, before.UID); done || err != nil {
		t.Fatal("restart skipped the exact cancellation's pending scan", done, err)
	}
	pool, err := api.Resource(volumeapi.PoolResource).Get(ctx, "pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
	if _, err := api.Resource(volumeapi.PoolResource).UpdateStatus(ctx, pool, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := restarted.Reconcile(ctx, testID, before.UID); !done || err != nil {
		t.Fatal(done, err)
	}
	if _, err := registry.Get(ctx, testID); !apierrors.IsNotFound(err) || *effects != 1 {
		t.Fatal("fenced cancellation did not settle exactly once", err, *effects)
	}
}
