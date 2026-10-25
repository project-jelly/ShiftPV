package volumeapi

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
)

func (p Pool) RetirementRequested() bool {
	return p.UID != "" && p.RetirementRequest == p.UID
}

func (p Pool) RegistrationRetired() bool {
	return p.UID != "" && p.RetirementApproval == p.UID
}

// ApprovePoolRetirement records registration retirement, never physical absence
// or permission to release a filesystem identity. The caller holds the Pool UID
// admission fence and has checked every logical reference and active helper.
func (r *Registry) ApprovePoolRetirement(ctx context.Context, name, uid string) error {
	if err := r.validate(); err != nil {
		return err
	}
	if name == "" || uid == "" {
		return fmt.Errorf("ShiftPVPool name and UID are required")
	}
	return r.mutateObject(ctx, objectMutation{
		resource: PoolResource, kind: "ShiftPVPool", name: name, uid: uid, backoff: retry.DefaultRetry,
		readError: "read ShiftPVPool retirement", writeError: "approve ShiftPVPool retirement",
		apply: func(object *unstructured.Unstructured) error {
			pool, err := poolFrom(object)
			if err != nil {
				return err
			}
			if pool.RegistrationRetired() {
				return errObjectUnchanged
			}
			if !pool.RetirementRequested() || !pool.RegistrationApproved() || !slices.Contains(pool.Finalizers, PoolProtectionFinalizer) {
				return fmt.Errorf("%w: retirement requires the current UID request and protected approved registration", ErrStateConflict)
			}
			annotations := object.GetAnnotations()
			annotations[PoolRetirementApprovalAnnotation] = uid
			object.SetAnnotations(annotations)
			return nil
		},
	})
}
