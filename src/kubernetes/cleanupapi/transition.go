package cleanupapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// transitionRules are applied in order; the first rule that rejects the write
// decides the error the caller sees. Each rule reads the effective current
// phase, which is Pending for a journal that has never recorded one.
var transitionRules = []func(current Cleanup, phase string, next Status) error{
	validatePhaseEdge,
	validateExecutorRule,
	validateReceiptRule,
	validateVerifyingRule,
	validateFenceIdentityRule,
	validateConfirmationRule,
	validateCompletedRule,
}

func validateTransition(current Cleanup, next Status) error {
	phase := current.Status.Phase
	if phase == "" {
		phase = PhasePending
	}
	for _, rule := range transitionRules {
		if err := rule(current, phase, next); err != nil {
			return err
		}
	}
	return nil
}

func validatePhaseEdge(_ Cleanup, phase string, next Status) error {
	allowed := map[string]map[string]bool{
		PhasePending:           {PhasePending: true, PhaseRunning: true, PhaseNeedsReview: true},
		PhaseRunning:           {PhaseRunning: true, PhaseVerifying: true, PhaseNeedsReview: true},
		PhaseVerifying:         {PhaseVerifying: true, PhaseConfirmingAbsence: true, PhaseNeedsReview: true},
		PhaseConfirmingAbsence: {PhaseConfirmingAbsence: true, PhaseCompleted: true, PhaseNeedsReview: true},
		PhaseCompleted:         {PhaseCompleted: true},
		PhaseNeedsReview:       {PhaseNeedsReview: true},
	}
	if !allowed[phase][next.Phase] {
		return fmt.Errorf("%w: phase %s cannot transition to %s", ErrConflict, phase, next.Phase)
	}
	return nil
}

func validateExecutorRule(current Cleanup, phase string, next Status) error {
	if current.Status.Executor != nil && !executorTransitionAllowed(phase, next.Phase, current.Status.Executor, next.Executor, current.Status.Receipt, next.Receipt) {
		return fmt.Errorf("%w: executor identity is immutable", ErrConflict)
	}
	if next.Phase == PhaseRunning && (next.Executor == nil || !next.Executor.Valid()) {
		return fmt.Errorf("%w: Running requires an executor", ErrConflict)
	}
	return nil
}

func validateReceiptRule(current Cleanup, _ string, next Status) error {
	if current.Status.Receipt != nil && !reflect.DeepEqual(current.Status.Receipt, next.Receipt) {
		return fmt.Errorf("%w: receipt is immutable", ErrConflict)
	}
	if next.Receipt == nil {
		return nil
	}
	if next.Executor == nil || next.Receipt.OperationID != current.Spec.OperationID || next.Receipt.ExecutorUID != next.Executor.ExecutionUID() {
		return fmt.Errorf("%w: receipt identity does not match intent and executor", ErrConflict)
	}
	if _, err := time.Parse(time.RFC3339Nano, next.Receipt.ObservedAt); err != nil {
		return fmt.Errorf("%w: invalid receipt time", ErrConflict)
	}
	return nil
}

func validateVerifyingRule(current Cleanup, _ string, next Status) error {
	if next.Phase == PhaseVerifying && (!validPurgedReceipt(current, next) || next.AbsenceProof != nil) {
		return fmt.Errorf("%w: Verifying requires a Pod-bound executor and only the API receipt", ErrConflict)
	}
	return nil
}

func validateFenceIdentityRule(current Cleanup, _ string, next Status) error {
	if current.Status.AbsenceProof == nil {
		return nil
	}
	if next.AbsenceProof == nil || !sameAbsenceFence(current.Status.AbsenceProof, next.AbsenceProof) {
		return fmt.Errorf("%w: absence fence identity is immutable", ErrConflict)
	}
	if current.Status.AbsenceProof.ConfirmedAt != "" && !reflect.DeepEqual(current.Status.AbsenceProof, next.AbsenceProof) {
		return fmt.Errorf("%w: confirmed absence proof is immutable", ErrConflict)
	}
	return nil
}

func validateConfirmationRule(current Cleanup, _ string, next Status) error {
	if next.Phase != PhaseConfirmingAbsence && next.Phase != PhaseCompleted {
		return nil
	}
	if !validPurgedReceipt(current, next) || !validAbsenceFence(current.Spec.Target, next.AbsenceProof) {
		return fmt.Errorf("%w: cleanup confirmation requires a post-receipt absence fence", ErrConflict)
	}
	return nil
}

func validateCompletedRule(_ Cleanup, _ string, next Status) error {
	if next.Phase != PhaseCompleted {
		return nil
	}
	if !confirmedAbsence(next.AbsenceProof) || next.SettledAt == "" {
		return fmt.Errorf("%w: Completed requires a purged receipt and later complete exact absence proof", ErrConflict)
	}
	if _, err := time.Parse(time.RFC3339Nano, next.SettledAt); err != nil {
		return fmt.Errorf("%w: invalid settlement time", ErrConflict)
	}
	return nil
}

func validPurgedReceipt(cleanup Cleanup, status Status) bool {
	if status.Executor == nil || status.Receipt == nil || !status.Executor.Valid() || !volume.ValidIdentityToken(status.Executor.PodUID) ||
		status.Executor.NodeName != cleanup.Spec.Target.NodeName ||
		status.Receipt.OperationID != cleanup.Spec.OperationID || status.Receipt.ExecutorUID != status.Executor.ExecutionUID() ||
		!status.Receipt.Retired || !status.Receipt.Purged || !validSHA256Digest(status.Receipt.LocalReceiptDigest) {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, status.Receipt.ObservedAt)
	return err == nil
}

func validSHA256Digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func executorTransitionAllowed(currentPhase, nextPhase string, current, next *Executor, currentReceipt, nextReceipt *Receipt) bool {
	if reflect.DeepEqual(current, next) {
		return true
	}
	// A retry may bind a new Pod before any API receipt exists. The backend,
	// operation and node remain fixed; legacy Job retries also retain Job UID. The node-local per-volume lock and operation
	// intent serialize an old Pod with its retry and make the effect idempotent.
	// Once a receipt exists, the complete executor identity is immutable.
	return currentPhase == PhaseRunning && nextPhase == PhaseRunning && currentReceipt == nil && nextReceipt == nil && current != nil && next != nil &&
		volume.ValidIdentityToken(next.PodUID) && next.Valid() && current.Kind == next.Kind && current.Namespace == next.Namespace &&
		current.JobName == next.JobName && current.JobUID == next.JobUID && current.NodeName == next.NodeName
}

func sameAbsenceFence(current, next *AbsenceProof) bool {
	return current != nil && next != nil && current.RequestID == next.RequestID && current.PoolName == next.PoolName &&
		current.PoolUID == next.PoolUID && current.RequiredGeneration == next.RequiredGeneration
}

func validAbsenceFence(target CopyIdentity, proof *AbsenceProof) bool {
	return proof != nil && volume.ValidIdentityToken(proof.RequestID) && proof.PoolName == target.PoolName &&
		proof.PoolUID == target.PoolUID && proof.RequiredGeneration > 0
}

func confirmedAbsence(proof *AbsenceProof) bool {
	if proof == nil || !proof.Valid || !proof.Complete || !proof.Absent || proof.ObservedGeneration < proof.RequiredGeneration || proof.ConfirmedAt == "" {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, proof.ConfirmedAt)
	return err == nil
}
