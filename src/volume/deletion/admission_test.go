package deletion

import (
	"testing"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func servingCopy() volume.CopyIdentity {
	return volume.CopyIdentity{InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing}
}

func TestDeletionAdmissionSafety(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Observation)
	}{
		{"recreated Volume", func(o *Observation) { o.UID = "new-volume" }},
		{"wrong owner", func(o *Observation) { o.OwnerNode = "worker-b" }},
		{"missing copy", func(o *Observation) { o.CurrentCopy = nil }},
		{"changed copy", func(o *Observation) { copy := *o.CurrentCopy; copy.CopyID = "new-copy"; o.CurrentCopy = &copy }},
		{"active Move", func(o *Observation) { o.ActiveMove = "move" }},
		{"blocked phase", func(o *Observation) { o.Phase = "Blocked" }},
		{"foreign publication", func(o *Observation) { o.PublishedNodes = []string{"worker-b"} }},
		{"multiple publications", func(o *Observation) { o.PublishedNodes = []string{"worker-a", "worker-b"} }},
		{"Ready with deletion intent", func(o *Observation) { o.OperationID = "delete-volume-uid" }},
		{"different delete operation", func(o *Observation) { o.Phase = PhaseDeleting; o.OperationID = "old-operation"; o.PublishedNodes = nil }},
		{"publication after fence", func(o *Observation) { o.Phase = PhaseDeleting; o.OperationID = "delete-volume-uid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := servingCopy()
			observed := Observation{UID: copy.VolumeUID, OwnerNode: copy.NodeName, Phase: PhaseReady, CurrentCopy: &copy, PublishedNodes: []string{copy.NodeName}}
			tc.change(&observed)
			if got := Decide(observed, copy.VolumeUID, "delete-"+copy.VolumeUID, copy); got != Reject {
				t.Fatalf("contradiction admitted: %v", got)
			}
		})
	}
}

func TestDeletionAdmissionProgress(t *testing.T) {
	copy := servingCopy()
	op := "delete-" + copy.VolumeUID
	observed := Observation{UID: copy.VolumeUID, OwnerNode: copy.NodeName, Phase: PhaseReady, CurrentCopy: &copy, PublishedNodes: []string{copy.NodeName}}
	if Decide(observed, copy.VolumeUID, op, copy) != WaitForUnpublish {
		t.Fatal("normal publication must wait")
	}
	observed.PublishedNodes = nil
	if Decide(observed, copy.VolumeUID, op, copy) != Begin {
		t.Fatal("unpublish must permit fencing")
	}
	observed.Phase, observed.OperationID = PhaseDeleting, op
	for range 3 {
		if Decide(observed, copy.VolumeUID, op, copy) != Resume {
			t.Fatal("same durable operation must resume")
		}
	}
	if Decide(observed, copy.VolumeUID, "other-operation", copy) != Reject {
		t.Fatal("another operation must not inherit the fence")
	}
	invalid := copy
	invalid.PoolUID = ""
	if Decide(observed, copy.VolumeUID, op, invalid) != Reject || Decide(observed, "", "delete-", copy) != Reject {
		t.Fatal("invalid request must be rejected")
	}
}
