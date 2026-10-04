package volumeapi

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

var (
	VolumeResource = schema.GroupVersionResource{Group: "shiftpv.io", Version: "v1alpha1", Resource: "shiftpvvolumes"}
	PoolResource   = schema.GroupVersionResource{Group: "shiftpv.io", Version: "v1alpha1", Resource: "shiftpvpools"}
	MoveResource   = schema.GroupVersionResource{Group: "shiftpv.io", Version: "v1alpha1", Resource: "shiftpvmoves"}

	ErrStateConflict     = errors.New("ShiftPV state precondition failed")
	ErrPoolConfiguration = errors.New("ShiftPV Pool configuration is invalid")
	ErrPoolNotFound      = errors.New("ShiftPV Pool is not registered")
	ErrPoolNotReady      = errors.New("ShiftPV Pool is not ready")
	ErrPoolCopyConflict  = errors.New("ShiftPV Pool contains a conflicting serving copy")
)

const (
	PoolConditionReady               = "Ready"
	PoolConditionAccessible          = "Accessible"
	PoolConditionCapacityIndependent = "CapacityIndependent"
	PoolConditionMounted             = "Mounted"
	PoolConditionIdentityReleased    = "IdentityReleased"
	PoolProtectionFinalizer          = "shiftpv.io/pool-protection"
	PoolIdentityReleaseAnnotation    = "shiftpv.io/release-pool-identity"
	PoolConditionWritable            = "Writable"
	PoolConditionCapacityReadable    = "CapacityReadable"
	DefaultPoolReadinessStaleAfter   = 3 * time.Minute
)

const PoolMountPolicyRequireMountPoint = "RequireMountPoint"

const (
	VolumeProtectionFinalizer = "shiftpv.io/volume-protection"
	MoveProtectionFinalizer   = "shiftpv.io/move-protection"

	PhasePending  = "Pending"
	PhaseReady    = "Ready"
	PhaseDeleting = "Deleting"
	PhaseMoving   = "Moving"
	PhaseBlocked  = "Blocked"
)

// PoolReadinessStaleAfterFlag is the one spelling of the probe staleness budget
// shared by every process that judges Pool readiness. Among the node-bound
// helpers only the cleanup helper does: its move source publication proof calls
// ReadyPoolForNode, so the parent controller forwards its own configured value
// there instead of letting the child silently fall back to
// DefaultPoolReadinessStaleAfter. The other helper subcommands recheck
// authority through PoolForIdentity and accept the flag only for forward
// compatibility.
const PoolReadinessStaleAfterFlag = "pool-readiness-stale-after"

// PoolReadinessStaleAfterArgument renders the helper container flag for a
// configured staleness budget, resolving a non-positive value to the default
// exactly as Registry.readiness() resolves it.
func PoolReadinessStaleAfterArgument(staleAfter time.Duration) string {
	if staleAfter <= 0 {
		staleAfter = DefaultPoolReadinessStaleAfter
	}
	return "--" + PoolReadinessStaleAfterFlag + "=" + staleAfter.String()
}

type Registry struct {
	Client                  dynamic.Interface
	PoolReadinessStaleAfter time.Duration
	Now                     func() time.Time
}

// objectMutation describes one read-modify-write attempt against a durable
// ShiftPV object. apply mutates the object that was just read; the identity
// precondition and the write are owned by mutateObject.
type objectMutation struct {
	resource schema.GroupVersionResource
	// kind names the object in the identity precondition message.
	kind string
	name string
	// uid is the expected object identity. An empty uid means the caller
	// enforces identity inside apply instead.
	uid        string
	backoff    wait.Backoff
	readError  string
	writeError string
	// status writes through the status subresource instead of the object.
	status bool
	apply  func(*unstructured.Unstructured) error
}

// errObjectUnchanged lets apply report that the observed object already holds
// the desired value, so no write is issued.
var errObjectUnchanged = errors.New("object already holds the desired value")

// mutateObject runs one compare-and-set loop: read the object, refuse a
// replaced identity, apply the caller's change, and write it back.
func (r *Registry) mutateObject(ctx context.Context, mutation objectMutation) error {
	if err := r.validate(); err != nil {
		return err
	}
	resource := r.Client.Resource(mutation.resource)
	return retry.RetryOnConflict(mutation.backoff, func() error {
		object, err := resource.Get(ctx, mutation.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("%s: %w", mutation.readError, err)
		}
		if mutation.uid != "" && string(object.GetUID()) != mutation.uid {
			return fmt.Errorf("%w: %s %q UID changed from %q to %q", ErrStateConflict, mutation.kind, mutation.name, mutation.uid, object.GetUID())
		}
		if err := mutation.apply(object); err != nil {
			if errors.Is(err, errObjectUnchanged) {
				return nil
			}
			return err
		}
		if mutation.status {
			_, err = resource.UpdateStatus(ctx, object, metav1.UpdateOptions{})
		} else {
			_, err = resource.Update(ctx, object, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("%s: %w", mutation.writeError, err)
		}
		return nil
	})
}

// updateObjectFinalizer keeps its own loop: it must tolerate a NotFound read
// when removing protection, which the shared identity precondition cannot
// express.
func (r *Registry) updateObjectFinalizer(ctx context.Context, resource schema.GroupVersionResource, name, uid, finalizer string, present bool) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" {
		return fmt.Errorf("object name and UID are required")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object, err := r.Client.Resource(resource).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && !present {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read finalizer: %w", err)
		}
		if string(object.GetUID()) != uid {
			return fmt.Errorf("%w: object %q UID changed from %q to %q", ErrStateConflict, name, uid, object.GetUID())
		}
		finalizers := object.GetFinalizers()
		hasFinalizer := slices.Contains(finalizers, finalizer)
		if present == hasFinalizer {
			return nil
		}
		if present {
			if object.GetDeletionTimestamp() != nil {
				return fmt.Errorf("%w: object %q is already deleting without protection", ErrStateConflict, name)
			}
			finalizers = append(finalizers, finalizer)
		} else {
			filtered := finalizers[:0]
			for _, current := range finalizers {
				if current != finalizer {
					filtered = append(filtered, current)
				}
			}
			finalizers = filtered
		}
		object.SetFinalizers(finalizers)
		if _, err := r.Client.Resource(resource).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update finalizer: %w", err)
		}
		return nil
	})
}

func (r *Registry) validate() error {
	if r == nil || r.Client == nil {
		return fmt.Errorf("volume registry is not configured")
	}
	return nil
}

func stringSliceToAny(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}
