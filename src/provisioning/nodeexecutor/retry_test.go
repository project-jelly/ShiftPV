package nodeexecutor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/retry"
)

func TestCreationBindingConflictRetriesSameIntent(t *testing.T) {
	client, node, api, _, initial := fixture(t)
	conflicts, updates := true, 0
	api.PrependReactor("update", "shiftpvvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if !conflicts {
			return false, nil, nil
		}
		updates++
		return true, nil, apierrors.NewConflict(volumeapi.VolumeResource.GroupResource(), testID, errors.New("object has been modified"))
	})
	ctx := context.Background()
	err := client.CreateCopy(ctx, *initial.CurrentCopy)
	var retryable interface{ Retryable() bool }
	if !apierrors.IsConflict(err) || !errors.As(err, &retryable) || !retryable.Retryable() || updates != retry.DefaultBackoff.Steps {
		t.Fatalf("binding conflict: attempts=%d error=%v", updates, err)
	}
	pending, err := client.Volumes.Get(ctx, testID)
	if err != nil || pending.CreationExecutor != nil || pending.Phase != initial.Phase || *pending.CurrentCopy != *initial.CurrentCopy || pending.CapacityBytes != initial.CapacityBytes {
		t.Fatalf("conflict changed intent: state=%+v error=%v", pending, err)
	}
	conflicts = false
	if err := client.CreateCopy(ctx, *initial.CurrentCopy); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry did not bind and wait for the original effect: %v", err)
	}
	bound, err := client.Volumes.Get(ctx, testID)
	if err != nil || bound.CreationExecutor == nil || *bound.CreationExecutor != node.Identity || *bound.CurrentCopy != *initial.CurrentCopy {
		t.Fatalf("retry changed copy or executor: state=%+v error=%v", bound, err)
	}
	if err := client.Volumes.RecordCreationReceipt(ctx, bound, creationReceipt(node.Identity)); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateCopy(ctx, *initial.CurrentCopy); err != nil {
		t.Fatalf("durable receipt did not settle retry: %v", err)
	}
}

func TestRetryClassificationPreservesAuthorityFailures(t *testing.T) {
	for _, original := range []error{volumeapi.ErrStateConflict, context.Canceled, apierrors.NewForbidden(volumeapi.VolumeResource.GroupResource(), testID, errors.New("denied"))} {
		err := classifyError(fmt.Errorf("binding: %w", original))
		var retryable interface{ Retryable() bool }
		if !errors.Is(err, original) || errors.As(err, &retryable) && retryable.Retryable() {
			t.Fatalf("authority failure became retryable: %v", err)
		}
	}
}
