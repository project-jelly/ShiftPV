// Package deletion decides whether an exact copy can be fenced for deletion.
// Decisions must be made again against the state used by the repository CAS.
package deletion

import "github.com/project-jelly/ShiftPV/src/volume"

const (
	PhaseReady    = "Ready"
	PhaseDeleting = "Deleting"
)

type Disposition uint8

const (
	Reject Disposition = iota
	WaitForUnpublish
	Begin
	Resume
)

type Observation struct {
	UID, OwnerNode, Phase, ActiveMove, OperationID string
	CurrentCopy                                    *volume.CopyIdentity
	PublishedNodes                                 []string
}

// Decide separates normal waiting from contradictory ownership. It grants
// only a deletion fence; cleanup still requires fresh authority and receipts.
func Decide(current Observation, uid, operationID string, copy volume.CopyIdentity) Disposition {
	if uid == "" || copy.Validate() != nil || copy.VolumeUID != uid || operationID != "delete-"+uid ||
		current.UID != uid || current.OwnerNode != copy.NodeName || current.CurrentCopy == nil || *current.CurrentCopy != copy || current.ActiveMove != "" {
		return Reject
	}
	if current.Phase == PhaseReady && current.OperationID == "" && len(current.PublishedNodes) == 1 && current.PublishedNodes[0] == copy.NodeName {
		return WaitForUnpublish
	}
	if len(current.PublishedNodes) != 0 {
		return Reject
	}
	if current.Phase == PhaseReady && current.OperationID == "" {
		return Begin
	}
	if current.Phase == PhaseDeleting && current.OperationID == operationID {
		return Resume
	}
	return Reject
}
