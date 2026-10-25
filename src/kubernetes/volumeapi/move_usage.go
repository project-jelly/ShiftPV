package volumeapi

import (
	"context"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/volume"
)

// RecordSourceUsage records one successful measurement without replaying the
// caller's other status fields. Conflict or response loss requires a fresh
// observation; this method never retries the write or grants execution authority.
func (r *Registry) RecordSourceUsage(ctx context.Context, expected Move, copy volume.CopyIdentity, bytes int64) (Move, error) {
	if err := r.validate(); err != nil {
		return Move{}, err
	}
	if err := sourceUsageRequest(expected, copy, bytes); err != nil {
		return Move{}, err
	}
	resource := r.Client.Resource(MoveResource)
	object, err := resource.Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return Move{}, fmt.Errorf("read Move before recording source usage: %w", err)
	}
	current, err := moveFrom(object)
	if err != nil {
		return Move{}, err
	}
	if current.UID != expected.UID || current.ResourceVersion != expected.ResourceVersion || current.Spec != expected.Spec {
		return Move{}, fmt.Errorf("%w: Move identity, version or spec changed", ErrStateConflict)
	}
	if err := sourceUsageMove(current, copy, bytes, moveCleanup(object) != nil); err != nil {
		return Move{}, err
	}
	state, err := r.Get(ctx, current.Spec.VolumeID)
	if err != nil {
		return Move{}, fmt.Errorf("read source authority before recording usage: %w", err)
	}
	if err := sourceUsageAuthority(current, copy, state); err != nil {
		return Move{}, err
	}
	if current.Status.SourceBytes == bytes {
		return current, nil
	}
	if err := unstructured.SetNestedField(object.Object, bytes, "status", "sourceBytes"); err != nil {
		return Move{}, err
	}
	stored, err := resource.UpdateStatus(ctx, object, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationStrict})
	if err != nil {
		return Move{}, fmt.Errorf("record source usage: %w", err)
	}
	return storedMoveStatus(current, object, stored)
}

func sourceUsageRequest(move Move, copy volume.CopyIdentity, bytes int64) error {
	if move.Name == "" || move.UID == "" || move.ResourceVersion == "" || bytes <= 0 ||
		copy.Validate() != nil || copy.Role != volume.RoleServing || copy.VolumeID != move.Spec.VolumeID || copy.NodeName != move.Spec.SourceNode {
		return fmt.Errorf("%w: source usage requires exact Move/copy identity and positive bytes", ErrStateConflict)
	}
	return nil
}

func sourceUsageMove(move Move, copy volume.CopyIdentity, bytes int64, hasCleanup bool) error {
	if move.Deleting || !slices.Contains(move.Finalizers, MoveProtectionFinalizer) ||
		move.Status.CapacityApproved || hasCleanup || hasMoveDestinationIntent(move.Status) {
		return fmt.Errorf("%w: source usage requires a protected pre-execution Move", ErrStateConflict)
	}
	if move.Spec.ProtocolVersion != MoveProtocolPreparedV1 {
		return fmt.Errorf("%w: unsupported Move protocol", ErrStateConflict)
	}
	if move.Status.Phase != "WaitingForCapacity" || move.Status.Preparation == nil {
		return fmt.Errorf("%w: prepared Move has no waiting reservation", ErrStateConflict)
	}
	if err := ValidateMovePreparation(move); err != nil {
		return err
	}
	if move.Status.SourceCopy == nil || *move.Status.SourceCopy != copy {
		return fmt.Errorf("%w: source copy changed", ErrStateConflict)
	}
	if move.Status.SourceBytes < 0 || (move.Status.SourceBytes > 0 && (move.Status.SourceBytes != bytes || move.Status.SourceCopy == nil)) {
		return fmt.Errorf("%w: recorded source usage is immutable", ErrStateConflict)
	}
	return nil
}

func sourceUsageAuthority(move Move, copy volume.CopyIdentity, state State) error {
	if state.Deleting || state.HasCleanup || !slices.Contains(state.Finalizers, VolumeProtectionFinalizer) ||
		state.UID != copy.VolumeUID || state.Phase != PhaseMoving || state.OwnerNode != move.Spec.SourceNode ||
		state.ActiveMove != move.Name || state.CurrentCopy == nil || *state.CurrentCopy != copy || len(state.PublishedNodes) != 0 {
		return fmt.Errorf("%w: source usage no longer has exact quiesced authority", ErrStateConflict)
	}
	return nil
}
