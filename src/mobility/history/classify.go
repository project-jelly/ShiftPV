// Package history validates settled Move records without executing or repairing
// them. It owns recorded transaction evidence, not live helper termination or GC.
package history

import (
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// ValidateSettled checks whether a Move's recorded responsibilities are closed.
// A nil current means its Volume no longer exists; a nil journal means no
// embedded cleanup exists. Later Moves and Volume recreation need not retain
// this history's copy as current, but no Volume may still reference this Move.
// Callers separately check live helpers and other volumes' ActiveMove references.
func ValidateSettled(move volumeapi.Move, current *volumeapi.State, journal *cleanupapi.Cleanup) error {
	if !volume.ValidObjectName(move.Name) || !volume.ValidIdentityToken(move.UID) || volume.ValidateID(move.Spec.VolumeID) != nil ||
		!volume.ValidObjectName(move.Spec.SourceNode) {
		return fmt.Errorf("Move history identity is incomplete")
	}
	if move.Spec.ProtocolVersion != "" && move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 {
		return fmt.Errorf("Move history protocol %q is unsupported", move.Spec.ProtocolVersion)
	}
	if move.Status.SettlementRequested && move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 {
		return fmt.Errorf("workload retirement requires prepared Move history")
	}
	if move.Status.Preparation != nil || current != nil && current.ActiveMove == move.Name {
		return fmt.Errorf("Move still owns a preparation reservation or Volume reference")
	}
	if move.Status.Phase == "Cancelled" {
		return validateCancelled(move, journal)
	}
	if err := validateSource(move); err != nil {
		return err
	}
	if move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1 {
		if err := volumeapi.ValidatePreparedMovePlan(move); err != nil {
			return err
		}
	}
	switch move.Status.Phase {
	case "Succeeded":
		if err := validateTransaction(move); err != nil {
			return err
		}
		return validateJournal(move, journal, "MoveSource", *move.Status.SourceCopy)
	case "Blocked":
		return validateRecovered(move, journal)
	default:
		return fmt.Errorf("Move phase %q is not settled history", move.Status.Phase)
	}
}

func validateCancelled(move volumeapi.Move, journal *cleanupapi.Cleanup) error {
	if move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 || move.Status.CapacityApproved ||
		hasDestinationIntent(move) || journal != nil || move.Status.CleanupPhase != "" || move.Status.RollbackRequiredGeneration != 0 ||
		move.Status.RecoveryPhase != "" || move.Status.RecoveryOwner != "" {
		return fmt.Errorf("cancelled Move retains execution or cleanup intent")
	}
	if move.Status.SourceCopy != nil {
		return volumeapi.ValidatePreparedMovePlan(move)
	}
	return nil
}

func validateSource(move volumeapi.Move) error {
	copy := move.Status.SourceCopy
	if copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing ||
		copy.VolumeID != move.Spec.VolumeID || copy.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("Move history lacks its exact source copy")
	}
	return nil
}

func validateTransaction(move volumeapi.Move) error {
	source, incoming := move.Status.SourceCopy, move.Status.IncomingCopy
	if incoming == nil || move.Status.SourceBytes <= 0 || move.Status.DestinationNode == move.Spec.SourceNode ||
		move.Status.DestinationPoolUID == source.PoolUID {
		return fmt.Errorf("Move history lacks its exact destination transaction")
	}
	return volumeapi.ValidateMoveTransactionIdentities(move, volumeapi.MoveDestinationAnchor{
		InstallationID: source.InstallationID, PoolName: incoming.PoolName, PoolUID: move.Status.DestinationPoolUID,
		NodeName: move.Status.DestinationNode, VolumeUID: source.VolumeUID,
	})
}

func validateRecovered(move volumeapi.Move, journal *cleanupapi.Cleanup) error {
	if (move.Spec.Recovery != "ResumeOwner" && !move.Status.SettlementRequested) || move.Status.RecoveryPhase != "Recovered" {
		return fmt.Errorf("Move recovery or capacity settlement is incomplete")
	}
	return validateRecoveryArtifacts(move, journal)
}

// ValidateSettlementCompletion checks the recorded artifacts before the final
// Volume unlock. If CSI deletion wins after that unlock, only owned read-only
// verification Jobs and Move metadata remain; no new disk authority is granted.
func ValidateSettlementCompletion(move volumeapi.Move, journal *cleanupapi.Cleanup) error {
	if !move.Status.SettlementRequested || move.Status.Phase != "Blocked" || move.Status.RecoveryPhase != "Completing" || move.Status.Preparation != nil {
		return fmt.Errorf("Move is not completing workload retirement")
	}
	if err := volumeapi.ValidatePreparedMovePlan(move); err != nil {
		return err
	}
	return validateRecoveryArtifacts(move, journal)
}

func validateRecoveryArtifacts(move volumeapi.Move, journal *cleanupapi.Cleanup) error {
	if move.Status.CapacityApproved || move.Status.CapacityReason != "RecoverySettled" {
		return fmt.Errorf("Move capacity settlement is incomplete")
	}
	if move.Status.SettlementRequested {
		if err := validateTransaction(move); err != nil {
			return err
		}
	}
	if move.Status.RecoveryOwner == move.Spec.SourceNode && !hasDestinationIntent(move) {
		// Legacy partial approvals can have no execution plan but still require
		// a rollback scan. Its generation alone is not a recorded absence proof.
		if journal != nil || move.Status.CleanupPhase != "" || move.Status.RollbackRequiredGeneration != 0 {
			return fmt.Errorf("recovered Move needs independent rollback absence evidence")
		}
		return nil
	}
	if err := validateTransaction(move); err != nil {
		return err
	}
	switch move.Status.RecoveryOwner {
	case move.Status.DestinationNode:
		return validateJournal(move, journal, "MoveSource", *move.Status.SourceCopy)
	case move.Spec.SourceNode:
		// The prepared controller releases this full plan by Retiring CAS after
		// causal absence. No cleanup journal means no purge was authorized.
		if preparedRollbackWithoutCleanup(move, journal) {
			return nil
		}
		if journal == nil || move.Status.RollbackRequiredGeneration <= 0 {
			return fmt.Errorf("recovered Move lacks recorded rollback completion evidence")
		}
		target := journal.Spec.Target
		if target != *move.Status.IncomingCopy && target != *move.Status.DestinationCopy {
			return fmt.Errorf("rollback cleanup targets another transaction copy")
		}
		if err := validateJournal(move, journal, "MoveRollback", target); err != nil {
			return err
		}
		if journal.Status.AbsenceProof.RequiredGeneration < move.Status.RollbackRequiredGeneration {
			return fmt.Errorf("rollback cleanup proof predates the rollback fence")
		}
		return nil
	default:
		return fmt.Errorf("Move recovery owner is neither transaction endpoint")
	}
}

func preparedRollbackWithoutCleanup(move volumeapi.Move, journal *cleanupapi.Cleanup) bool {
	return move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1 && journal == nil &&
		move.Status.CleanupPhase == "" && move.Status.RollbackRequiredGeneration > 0
}

func validateJournal(move volumeapi.Move, journal *cleanupapi.Cleanup, reason string, target volume.CopyIdentity) error {
	if journal == nil || move.Status.CleanupPhase != cleanupapi.PhaseCompleted {
		return fmt.Errorf("Move history lacks a completed cleanup journal")
	}
	operation := volumeapi.MoveCleanupOperationID(move.UID)
	if reason == "MoveRollback" {
		operation = volumeapi.MoveRollbackOperationID(move.UID)
	}
	expected := cleanupapi.Spec{
		OperationID: operation, Target: target, Reason: reason,
		Authority: cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID},
	}
	if journal.UID != move.UID || journal.Spec != expected {
		return fmt.Errorf("cleanup journal does not match the exact Move transaction")
	}
	return cleanupapi.ValidateSettled(*journal)
}

func hasDestinationIntent(move volumeapi.Move) bool {
	status := move.Status
	return status.IncomingCopy != nil || status.DestinationCopy != nil || status.CopyJobName != "" ||
		status.PromotionJobName != "" || status.CopyOperationID != "" || status.PromotionOperationID != ""
}
