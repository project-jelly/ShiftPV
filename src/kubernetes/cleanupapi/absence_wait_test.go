package cleanupapi

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestWaitForAbsenceConvergesWithoutAnotherDeleteCall(t *testing.T) {
	parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
	store, client := testStore(parent, testPool(3, 3, true, false, "", nil))
	client.PrependReactor("update", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if action.GetSubresource() == "" {
			object.SetGeneration(object.GetGeneration() + 1)
		}
		return false, nil, nil
	})
	cleanup, settled, err := store.ReconcileAbsence(context.Background(), verifyingCleanup(t, store))
	if err != nil || settled {
		t.Fatalf("opening fence: settled=%v err=%v", settled, err)
	}
	reads := 0
	client.PrependReactor("get", "shiftpvpools", func(action ktesting.Action) (bool, runtime.Object, error) {
		reads++
		if reads == 2 {
			object, err := client.Tracker().Get(volumeapi.PoolResource, "", "pool-a")
			if err != nil {
				t.Fatal(err)
			}
			pool := object.(*unstructured.Unstructured)
			_ = unstructured.SetNestedField(pool.Object, pool.GetGeneration(), "status", "observedGeneration")
			if err := client.Tracker().Update(volumeapi.PoolResource, pool, ""); err != nil {
				t.Fatal(err)
			}
		}
		return false, nil, nil
	})
	result, settled, err := store.WaitForAbsence(context.Background(), cleanup, time.Second)
	if err != nil || !settled || result.Status.Phase != PhaseCompleted || reads < 2 {
		t.Fatalf("result=%+v settled=%v reads=%d err=%v", result.Status, settled, reads, err)
	}
}

func TestWaitForAbsenceKeepsHoldOnTimeoutAndCancellation(t *testing.T) {
	for _, mode := range []string{"timeout", "canceled", "deadline", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			parent := testParent("ShiftPVVolume", testVolumeID, "volume-uid", volumeapi.VolumeProtectionFinalizer)
			store, _ := testStore(parent)
			cleanup := verifyingCleanup(t, store)
			expected := cleanup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := 20 * time.Millisecond
			var want error
			switch mode {
			case "disabled":
				timeout = 0
			case "canceled":
				cancel()
				want = context.Canceled
			case "deadline":
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer deadlineCancel()
				want = context.DeadlineExceeded
			}
			// A completed journal is not synthesized on timeout, even if no
			// observer is running. Use a legitimate pending causal proof.
			cleanup.Status.Phase = PhaseConfirmingAbsence
			cleanup.Status.AbsenceProof = &AbsenceProof{RequestID: "scan", PoolName: "pool-a", PoolUID: "pool-uid", RequiredGeneration: 4}
			if err := store.UpdateStatus(context.Background(), expected, cleanup.Status); err != nil {
				t.Fatal(err)
			}
			if mode == "timeout" {
				// Keep a stale, valid Pool observation; it cannot settle the fence.
				client := store.Client
				_, err := client.Resource(volumeapi.PoolResource).Create(context.Background(), testPool(4, 3, true, false, "", nil), metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			_, settled, err := store.WaitForAbsence(ctx, cleanup, timeout)
			if settled || !errors.Is(err, want) {
				t.Fatalf("settled=%v err=%v want=%v", settled, err, want)
			}
			stored, err := store.Get(context.Background(), cleanup.Spec.Authority)
			if err != nil || stored.Status.Phase != PhaseConfirmingAbsence || stored.Status.AbsenceProof.ConfirmedAt != "" {
				t.Fatalf("hold was settled: status=%+v err=%v", stored.Status, err)
			}
		})
	}
}
