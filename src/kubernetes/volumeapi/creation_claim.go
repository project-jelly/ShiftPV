package volumeapi

import (
	"fmt"
	"regexp"

	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ClaimReference records the original PVC incarnation before any copy effect.
// Missing provenance is unknown; a later same-name PVC cannot supply it.
type ClaimReference struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

var claimUID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (c ClaimReference) Valid() bool {
	return len(validation.IsDNS1123Label(c.Namespace)) == 0 && len(validation.IsDNS1123Subdomain(c.Name)) == 0 && volume.ValidIdentityToken(c.UID) && claimUID.MatchString(c.UID)
}

// CreationRequest keeps the selected Pool and verified claim in the same first
// protected intent. A nil Pool uses the default registered Pool for the node.
type CreationRequest struct {
	VolumeID, RequestName, NodeName string
	CapacityBytes                   int64
	Pool                            *PoolReference
	Claim                           *ClaimReference
}

func (r CreationRequest) validate() error {
	if volume.ValidateID(r.VolumeID) != nil || r.RequestName == "" || r.NodeName == "" || r.CapacityBytes <= 0 {
		return ErrStateConflict
	}
	if r.Pool != nil && (r.Pool.Name == "" || r.Pool.UID == "") {
		return ErrPoolConfiguration
	}
	if r.Claim != nil && (!r.Claim.Valid() || r.RequestName != "pvc-"+r.Claim.UID) {
		return ErrStateConflict
	}
	return nil
}

func claimFrom(object *unstructured.Unstructured) (*ClaimReference, error) {
	value, found, err := unstructured.NestedMap(object.Object, "spec", "creationClaim")
	if err != nil {
		return nil, fmt.Errorf("decode creationClaim: %w", err)
	}
	if !found {
		return nil, nil
	}
	var claim ClaimReference
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(value, &claim); err != nil {
		return nil, fmt.Errorf("decode creationClaim: %w", err)
	}
	request, _, _ := unstructured.NestedString(object.Object, "spec", "requestName")
	if !claim.Valid() || request != "pvc-"+claim.UID {
		return nil, fmt.Errorf("%w: invalid original creation claim", ErrStateConflict)
	}
	return &claim, nil
}

func sameClaim(a, b *ClaimReference) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
