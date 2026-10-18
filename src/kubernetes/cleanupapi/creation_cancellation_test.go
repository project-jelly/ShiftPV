package cleanupapi

import (
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestCreationCancellationReasonIsAnExactModeBoundary(t *testing.T) {
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: "shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "node-a", Role: volume.RoleServing}
	original := Spec{OperationID: volume.CreationCancellationPrefix + copy.VolumeUID, Target: copy, Reason: "CreationCancelled", Authority: Authority{Kind: "ShiftPVVolume", Name: copy.VolumeID, UID: copy.VolumeUID}}
	if err := original.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"ordinary reason", "ordinary operation", "other initial copy", "incoming", "Move authority", "unknown reason"} {
		t.Run(change, func(t *testing.T) {
			spec := original
			switch change {
			case "ordinary reason":
				spec.Reason = "VolumeDelete"
			case "ordinary operation":
				spec.OperationID = "delete-volume-uid"
			case "other initial copy":
				spec.Target.CopyID = "initial-another-volume"
			case "incoming":
				spec.Target.Role = volume.RoleIncoming
			case "Move authority":
				spec.Authority.Kind = "ShiftPVMove"
			case "unknown reason":
				spec.Reason = "UnknownCreationMode"
			}
			if err := spec.Validate(); err == nil {
				t.Fatalf("mode boundary accepted %s", change)
			}
		})
	}
	// Existing normal deletion IDs were not required to be canonical.
	original.Reason, original.OperationID = "VolumeDelete", "legacy-delete-operation"
	if err := original.Validate(); err != nil {
		t.Fatalf("legacy normal deletion changed: %v", err)
	}
}
