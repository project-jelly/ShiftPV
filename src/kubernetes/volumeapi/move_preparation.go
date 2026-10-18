package volumeapi

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func movePreparationFrom(object *unstructured.Unstructured) (*MovePreparation, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object.Object, "status", "preparation")
	if err != nil || !found || value == nil {
		return nil, err
	}
	data, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decode preparation: expected object or null")
	}
	var preparation MovePreparation
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(data, &preparation); err != nil {
		return nil, fmt.Errorf("decode preparation: %w", err)
	}
	if err := validatePreparationFields(preparation); err != nil {
		return nil, err
	}
	return &preparation, nil
}

func validatePreparationFields(preparation MovePreparation) error {
	if !volume.ValidObjectName(preparation.Pool.Name) || !volume.ValidIdentityToken(preparation.Pool.UID) ||
		!volume.ValidObjectName(preparation.NodeName) || preparation.ReservedBytes <= 0 {
		return fmt.Errorf("%w: invalid Move preparation reservation", ErrStateConflict)
	}
	return nil
}

// ValidateMovePreparation checks the durable reservation without observing any
// other object. The capacity ledger additionally checks the live Volume,
// requested bytes and ActiveMove authority; admission verifies the live Pool.
func ValidateMovePreparation(move Move) error {
	preparation := move.Status.Preparation
	if preparation == nil {
		return nil
	}
	if move.Spec.ProtocolVersion != MoveProtocolPreparedV1 {
		return fmt.Errorf("%w: preparation requires protocol %q", ErrStateConflict, MoveProtocolPreparedV1)
	}
	if err := validatePreparationFields(*preparation); err != nil {
		return err
	}
	if err := validatePreparedPlan(move); err != nil {
		return err
	}
	if move.Status.CapacityApproved || hasMoveDestinationIntent(move.Status) {
		return fmt.Errorf("%w: preparation cannot coexist with destination execution authority", ErrStateConflict)
	}
	if !volume.ValidIdentityToken(move.Status.PlacementPodUID) {
		return fmt.Errorf("%w: preparation requires the exact placement Pod UID", ErrStateConflict)
	}
	if preparation.Pool.UID == move.Status.SourceCopy.PoolUID {
		return fmt.Errorf("%w: preparation destination is the source", ErrStateConflict)
	}
	if (move.Status.DestinationNode != "" && move.Status.DestinationNode != preparation.NodeName) ||
		(move.Status.DestinationPoolUID != "" && move.Status.DestinationPoolUID != preparation.Pool.UID) {
		return fmt.Errorf("%w: preparation destination does not match the Move plan", ErrStateConflict)
	}
	return nil
}

func validatePreparedPlan(move Move) error {
	if move.Name == "" || !volume.ValidIdentityToken(move.UID) {
		return fmt.Errorf("%w: prepared Move identity is missing", ErrStateConflict)
	}
	for name, value := range map[string]string{
		"persistentVolumeName": move.Status.PersistentVolumeName, "persistentVolumeUID": move.Status.PersistentVolumeUID,
		"claimNamespace": move.Status.ClaimNamespace, "claimName": move.Status.ClaimName, "claimUID": move.Status.ClaimUID,
		"consumerName": move.Status.ConsumerName, "consumerUID": move.Status.ConsumerUID,
	} {
		if !volume.ValidIdentityToken(value) {
			return fmt.Errorf("%w: prepared Move requires %s", ErrStateConflict, name)
		}
	}
	source := move.Status.SourceCopy
	if source == nil || source.Validate() != nil || source.Role != volume.RoleServing ||
		source.VolumeID != move.Spec.VolumeID || source.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("%w: prepared Move source copy does not match its spec", ErrStateConflict)
	}
	return nil
}

// ValidatePreparedMovePlan checks the identities that must survive every
// prepared transaction stage, including execution after its logical hold was
// converted. It does not replace observation of the live binding or copy.
func ValidatePreparedMovePlan(move Move) error {
	if move.Spec.ProtocolVersion != MoveProtocolPreparedV1 {
		return fmt.Errorf("%w: prepared protocol is required", ErrStateConflict)
	}
	return validatePreparedPlan(move)
}

func hasMoveDestinationIntent(status MoveStatus) bool {
	return status.IncomingCopy != nil || status.DestinationCopy != nil || status.CopyOperationID != "" ||
		status.PromotionOperationID != "" || status.CopyJobName != "" || status.PromotionJobName != "" ||
		status.CleanupPhase != "" || status.RollbackRequiredGeneration != 0
}

// CompareAndSetMoveStatus makes exactly one status write against the observed
// Move incarnation and resource version. A conflict or lost response is returned
// to the caller; it must observe authority again before deciding another action.
// The embedded cleanup journal is preserved from that exact resource version.
func (r *Registry) CompareAndSetMoveStatus(ctx context.Context, expected Move, next MoveStatus) (Move, error) {
	if err := r.validate(); err != nil {
		return Move{}, err
	}
	if expected.Name == "" || expected.UID == "" || expected.ResourceVersion == "" {
		return Move{}, fmt.Errorf("%w: Move name, UID and resourceVersion are required", ErrStateConflict)
	}
	resource := r.Client.Resource(MoveResource)
	object, err := resource.Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return Move{}, fmt.Errorf("read Move before status compare-and-set: %w", err)
	}
	if string(object.GetUID()) != expected.UID || object.GetResourceVersion() != expected.ResourceVersion {
		return Move{}, fmt.Errorf("%w: Move UID or resourceVersion changed", ErrStateConflict)
	}
	current, err := moveFrom(object)
	if err != nil {
		return Move{}, err
	}
	if current.Spec != expected.Spec || current.Spec.ProtocolVersion != MoveProtocolPreparedV1 {
		return Move{}, fmt.Errorf("%w: Move protocol or spec changed", ErrStateConflict)
	}
	// Cleanup is a separate writer. Its raw journal and observed phase belong
	// to this resource version, not to the caller's proposed status value.
	next.CleanupPhase = current.Status.CleanupPhase
	if err := validatePreparedProtection(current, next, moveCleanup(object) != nil); err != nil {
		return Move{}, err
	}
	if err := validatePreparedTransition(current, next); err != nil {
		return Move{}, err
	}
	setMoveStatus(object, next)
	stored, err := resource.UpdateStatus(ctx, object, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationStrict})
	if err != nil {
		return Move{}, fmt.Errorf("compare-and-set Move status: %w", err)
	}
	return storedMoveStatus(current, object, stored)
}

func validatePreparedProtection(current Move, next MoveStatus, hasCleanup bool) error {
	if next.Phase == "Cancelled" && hasCleanup {
		return fmt.Errorf("%w: cancellation cannot settle a Move with a cleanup journal", ErrStateConflict)
	}
	granting := (current.Status.Preparation == nil && next.Preparation != nil) || (!current.Status.CapacityApproved && next.CapacityApproved)
	if granting && current.Deleting {
		return fmt.Errorf("%w: deleting Move cannot acquire preparation or execution authority", ErrStateConflict)
	}
	if granting && !slices.Contains(current.Finalizers, MoveProtectionFinalizer) {
		return fmt.Errorf("%w: Move protection is required before granting capacity", ErrStateConflict)
	}
	return nil
}

func storedMoveStatus(current Move, expected, stored *unstructured.Unstructured) (Move, error) {
	wanted, err := moveStatusFrom(expected)
	if err != nil {
		return Move{}, err
	}
	result, err := moveFrom(stored)
	if err != nil {
		return Move{}, err
	}
	if result.UID != current.UID || result.Spec != current.Spec {
		return Move{}, fmt.Errorf("%w: stored Move identity changed", ErrStateConflict)
	}
	if !reflect.DeepEqual(result.Status, wanted) || !reflect.DeepEqual(moveCleanup(expected), moveCleanup(stored)) {
		return Move{}, fmt.Errorf("%w: Move status or cleanup journal was not stored exactly", ErrStateConflict)
	}
	return result, nil
}

func moveCleanup(object *unstructured.Unstructured) any {
	status, _ := object.Object["status"].(map[string]any)
	return status["cleanup"]
}

func validatePreparedTransition(current Move, next MoveStatus) error {
	if err := validateSettlementTransition(current, next); err != nil {
		return err
	}
	if err := preserveMoveIdentity(current.Status, next); err != nil {
		return err
	}
	if err := preservePreparedPlan(current.Status, next); err != nil {
		return err
	}
	candidate := current
	candidate.Status = next
	if err := ValidateMovePreparation(candidate); err != nil {
		return err
	}
	if next.SourceCopy != nil || next.CapacityApproved || next.PlacementPodUID != "" || next.PersistentVolumeUID != "" || next.ClaimUID != "" {
		if err := validatePreparedPlan(candidate); err != nil {
			return err
		}
	}
	if next.CapacityApproved {
		if !volume.ValidIdentityToken(next.PlacementPodUID) || next.Preparation != nil {
			return fmt.Errorf("%w: approved Move requires its placement identity and no logical hold", ErrStateConflict)
		}
	}
	if err := validateExecutionHoldRelease(current, candidate); err != nil {
		return err
	}
	return preservePreparation(current, candidate)
}

func preservePreparedPlan(current, next MoveStatus) error {
	for _, field := range []struct{ name, current, next string }{
		{"persistentVolumeName", current.PersistentVolumeName, next.PersistentVolumeName},
		{"persistentVolumeUID", current.PersistentVolumeUID, next.PersistentVolumeUID},
		{"claimNamespace", current.ClaimNamespace, next.ClaimNamespace},
		{"claimName", current.ClaimName, next.ClaimName}, {"claimUID", current.ClaimUID, next.ClaimUID},
		{"consumerName", current.ConsumerName, next.ConsumerName}, {"consumerUID", current.ConsumerUID, next.ConsumerUID},
		{"placementPodUID", current.PlacementPodUID, next.PlacementPodUID},
		{"destinationNode", current.DestinationNode, next.DestinationNode}, {"destinationPoolUID", current.DestinationPoolUID, next.DestinationPoolUID},
	} {
		if field.current != "" && field.current != field.next {
			return fmt.Errorf("%w: prepared Move %s is immutable", ErrStateConflict, field.name)
		}
	}
	return nil
}

func preservePreparation(current, next Move) error {
	before, after := current.Status.Preparation, next.Status.Preparation
	if before == nil {
		return validatePreparationStart(current, next)
	}
	if err := ValidateMovePreparation(current); err != nil {
		return err
	}
	if after != nil {
		if *before != *after {
			return fmt.Errorf("%w: preparation reservation is immutable", ErrStateConflict)
		}
		return nil
	}
	if current.Status.Phase == "Canceling" && next.Status.Phase == "Cancelled" &&
		!next.Status.CapacityApproved && !hasMoveDestinationIntent(next.Status) {
		return nil
	}
	if current.Status.Phase != "WaitingForCapacity" ||
		!next.Status.CapacityApproved || next.Status.DestinationNode != before.NodeName || next.Status.DestinationPoolUID != before.Pool.UID {
		return fmt.Errorf("%w: preparation may only be released by cancellation or exact execution approval", ErrStateConflict)
	}
	return ValidateMoveTransactionIdentities(next, MoveDestinationAnchor{
		InstallationID: current.Status.SourceCopy.InstallationID, PoolName: before.Pool.Name,
		PoolUID: before.Pool.UID, NodeName: before.NodeName, VolumeUID: current.Status.SourceCopy.VolumeUID,
	})
}

func validatePreparationStart(current, next Move) error {
	if next.Status.CapacityApproved && !current.Status.CapacityApproved {
		return fmt.Errorf("%w: execution approval requires the existing preparation reservation", ErrStateConflict)
	}
	if hasMoveDestinationIntent(next.Status) && !current.Status.CapacityApproved && !hasMoveDestinationIntent(current.Status) {
		return fmt.Errorf("%w: destination intent requires prior execution approval", ErrStateConflict)
	}
	if next.Status.Preparation != nil && (current.Status.CapacityApproved || hasMoveDestinationIntent(current.Status) ||
		(current.Status.Phase != "" && current.Status.Phase != "Pending" && current.Status.Phase != "Preparing")) {
		return fmt.Errorf("%w: preparation can only begin before disruption", ErrStateConflict)
	}
	return nil
}

// The recovery caller must still prove current Volume authority and fresh Pool
// absence/cleanup. This only fences the durable step that releases its hold.
func validateExecutionHoldRelease(current, next Move) error {
	if !current.Status.CapacityApproved || next.Status.CapacityApproved {
		return nil
	}
	before, after := current.Status, next.Status
	if before.Phase != "Blocked" || before.RecoveryPhase != "Retiring" ||
		after.Phase != "Blocked" || after.RecoveryPhase != "Retiring" || after.CapacityReason != "RecoverySettled" {
		return fmt.Errorf("%w: execution hold release requires recovery settlement", ErrStateConflict)
	}
	if before.CleanupPhase != "" && before.CleanupPhase != "Completed" {
		return fmt.Errorf("%w: execution cleanup is not settled", ErrStateConflict)
	}
	if before.CleanupPhase == "" && before.RollbackRequiredGeneration <= 0 {
		return fmt.Errorf("%w: execution hold release requires a recorded rollback scan", ErrStateConflict)
	}
	if before.SourceCopy == nil || before.IncomingCopy == nil {
		return fmt.Errorf("%w: execution hold has no exact disk plan", ErrStateConflict)
	}
	return ValidateMoveTransactionIdentities(current, MoveDestinationAnchor{
		InstallationID: before.SourceCopy.InstallationID, PoolName: before.IncomingCopy.PoolName,
		PoolUID: before.DestinationPoolUID, NodeName: before.DestinationNode, VolumeUID: before.SourceCopy.VolumeUID,
	})
}
