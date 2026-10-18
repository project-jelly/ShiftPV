package creation

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var (
	ErrNotCancellable        = errors.New("volume is not an unfinished initial creation")
	ErrInspectionUnavailable = errors.New("creation executor inspection is unavailable")
	ErrLegacyCreation        = errors.New("legacy creation helper responsibility must settle before cancellation")
)

type CancellationRepository interface {
	Get(context.Context, string) (volumeapi.State, error)
	CancelUnstartedCreation(context.Context, string, string) error
	BeginCreationCancellation(context.Context, string, string, volume.CopyIdentity) (volumeapi.State, error)
	Delete(context.Context, string, string) error
	RemoveVolumeFinalizer(context.Context, string, string) error
}

type ExecutorInspector interface {
	CreationExists(context.Context, volume.CopyIdentity) (bool, error)
}

type CleanupSettlement interface {
	Advance(context.Context, cleanupapi.Spec) (bool, error)
}

// Canceller can only retire unfinished initial creation. It never falls back to
// normal deletion if completion wins the Volume CAS.
type Canceller struct {
	Volumes   CancellationRepository
	Inspector ExecutorInspector
	Cleanups  CleanupSettlement
}

func (c *Canceller) Advance(ctx context.Context, id, uid string) (bool, error) {
	if c == nil || c.Volumes == nil || uid == "" || volume.ValidateID(id) != nil {
		return false, fmt.Errorf("creation cancellation requires an exact Volume identity and repository")
	}
	state, err := c.Volumes.Get(ctx, id)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if state.UID != uid {
		return false, volumeapi.ErrStateConflict
	}
	if state.UnstartedCreation() {
		err := c.Volumes.CancelUnstartedCreation(ctx, id, uid)
		return err == nil, err
	}
	if !state.CancellableCreation() && !state.CancellingCreation() {
		return false, ErrNotCancellable
	}
	if c.Inspector == nil {
		return false, ErrInspectionUnavailable
	}
	exists, err := c.Inspector.CreationExists(ctx, *state.CurrentCopy)
	if err != nil {
		return false, fmt.Errorf("inspect creation responsibility: %w", err)
	}
	if exists {
		return false, ErrLegacyCreation
	}
	fenced, err := c.Volumes.BeginCreationCancellation(ctx, id, uid, *state.CurrentCopy)
	if err != nil {
		return false, err
	}
	if c.Cleanups == nil {
		return false, fmt.Errorf("creation cleanup settlement is unavailable")
	}
	complete, err := c.Cleanups.Advance(ctx, cleanupapi.Spec{
		OperationID: fenced.DeletionOperationID, Target: *fenced.CurrentCopy, Reason: "CreationCancelled",
		Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: id, UID: uid},
	})
	if err != nil || !complete {
		return false, err
	}
	if err := c.Volumes.Delete(ctx, id, uid); err != nil {
		return false, err
	}
	if err := c.Volumes.RemoveVolumeFinalizer(ctx, id, uid); err != nil {
		return false, err
	}
	return true, nil
}
