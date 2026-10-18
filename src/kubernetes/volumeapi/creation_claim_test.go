package volumeapi

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCreationRequestAndDecodedClaimRequireExactSource(t *testing.T) {
	original := CreationRequest{VolumeID: "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestName: "pvc-claim-uid", NodeName: "worker-a", CapacityBytes: 64, Claim: &ClaimReference{Namespace: "app", Name: "data", UID: "claim-uid"}}
	for _, change := range []string{"valid", "namespace", "name", "UID", "request", "missing field", "wrong type"} {
		t.Run(change, func(t *testing.T) {
			request := original
			copy := *original.Claim
			request.Claim = &copy
			switch change {
			case "namespace":
				copy.Namespace = "Bad Namespace"
			case "name":
				copy.Name = ".."
			case "UID":
				copy.UID = "../other"
			case "request":
				request.RequestName = "pvc-other"
			}
			object := newVolumeObject(request, Pool{Name: "pool-a", UID: "pool-uid"})
			if change == "missing field" {
				unstructured.RemoveNestedField(object.Object, "spec", "creationClaim", "uid")
			}
			if change == "wrong type" {
				_ = unstructured.SetNestedField(object.Object, int64(7), "spec", "creationClaim")
			}
			claim, err := claimFrom(object)
			if change == "valid" {
				if err != nil || claim == nil || *claim != copy || request.validate() != nil {
					t.Fatalf("valid source rejected: %+v %v", claim, err)
				}
				return
			}
			if err == nil || claim != nil {
				t.Fatalf("bad claim was decoded: %+v err=%v", claim, err)
			}
			if change != "missing field" && change != "wrong type" && !errors.Is(request.validate(), ErrStateConflict) {
				t.Fatal("request accepted invalid provenance")
			}
		})
	}
	legacy := newVolumeObject(CreationRequest{VolumeID: original.VolumeID, RequestName: original.RequestName, NodeName: original.NodeName, CapacityBytes: 64}, Pool{Name: "pool-a", UID: "pool-uid"})
	if claim, err := claimFrom(legacy); err != nil || claim != nil {
		t.Fatalf("legacy omission invented provenance: %+v %v", claim, err)
	}
}
