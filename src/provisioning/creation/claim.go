// Package creation verifies the request lifetime recorded before copy approval.
package creation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ErrClaimIdentity denies creation/resume; it never authorizes cleanup.
var ErrClaimIdentity = errors.New("original creation claim identity changed")

type ClaimRequest struct{ Namespace, Name, RequestName, NodeName string }

type ClaimInspection interface {
	Inspect(context.Context, ClaimRequest) (*volumeapi.ClaimReference, error)
}

type ClaimReader interface {
	Claim(context.Context, string, string) (*corev1.PersistentVolumeClaim, error)
}

type ClaimInspector struct{ Reader ClaimReader }

// Inspect verifies the chart's untruncated pvc-UID request contract against a
// live, unbound original PVC. API errors never become missing-claim proof.
func (i ClaimInspector) Inspect(ctx context.Context, request ClaimRequest) (*volumeapi.ClaimReference, error) {
	uid, prefixed := strings.CutPrefix(request.RequestName, "pvc-")
	claim := &volumeapi.ClaimReference{Namespace: request.Namespace, Name: request.Name, UID: uid}
	if !prefixed || !claim.Valid() || request.NodeName == "" {
		return nil, ErrClaimIdentity
	}
	if i.Reader == nil {
		return nil, fmt.Errorf("creation claim reader is not configured")
	}
	pvc, err := i.Reader.Claim(ctx, request.Namespace, request.Name)
	if apierrors.IsNotFound(err) {
		return nil, ErrClaimIdentity
	}
	if err != nil {
		return nil, err
	}
	if pvc == nil {
		return nil, fmt.Errorf("creation claim reader returned no observation")
	}
	if string(pvc.UID) != claim.UID || pvc.Namespace != claim.Namespace || pvc.Name != claim.Name || pvc.DeletionTimestamp != nil || pvc.Spec.VolumeName != "" || pvc.Annotations["volume.kubernetes.io/selected-node"] != request.NodeName {
		return nil, ErrClaimIdentity
	}
	return claim, nil
}
