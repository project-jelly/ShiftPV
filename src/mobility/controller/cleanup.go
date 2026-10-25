package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

var errCleanupPending = errors.New("cleanup executor settlement is pending")

func (r *Reconciler) cleanupState(ctx context.Context, move volumeapi.Move) (complete, failed bool, err error) {
	spec, err := moveCleanupSpec(move)
	if err != nil {
		return false, false, nil
	}
	request, err := r.Cleanups.Get(ctx, spec.Authority)
	if errors.Is(err, cleanupapi.ErrNoJournal) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if request.Spec != spec {
		return false, false, fmt.Errorf("cleanup request belongs to a different Move")
	}
	if request.Status.Phase == cleanupapi.PhaseCompleted {
		ready, err := r.observeCompletedCleanup(ctx, request)
		return ready, false, err
	}
	return request.Status.Phase == cleanupapi.PhaseCompleted, request.Status.Phase == cleanupapi.PhaseNeedsReview, nil
}

func (r *Reconciler) observeCompletedCleanup(ctx context.Context, current cleanupapi.Cleanup) (bool, error) {
	if r.CleanupOperator == nil {
		return false, fmt.Errorf("completed cleanup observer is not configured")
	}
	ready, err := r.CleanupOperator.ObserveCompletedCleanup(ctx, current)
	if err == nil && !ready {
		return false, errCleanupPending
	}
	return ready, err
}

func (r *Reconciler) observeTerminalCleanupHistory(ctx context.Context, move volumeapi.Move) error {
	if r.Cleanups == nil {
		if move.Status.CleanupPhase != "" {
			return fmt.Errorf("cleanup history store is not configured")
		}
		return nil
	}
	current, err := r.Cleanups.GetHistory(ctx, moveCleanupAuthority(move))
	if errors.Is(err, cleanupapi.ErrNoJournal) {
		return nil
	}
	if err != nil {
		return err
	}
	if !cleanupIntentMatches(move, current.Spec) {
		return fmt.Errorf("retain terminal history with mismatched cleanup intent")
	}
	if current.Status.Phase == cleanupapi.PhaseCompleted {
		_, err := r.observeCompletedCleanup(ctx, current)
		return err
	}
	return nil
}

func (r *Reconciler) acknowledgeCleanup(ctx context.Context, move volumeapi.Move) error {
	complete, failed, err := r.cleanupState(ctx, move)
	if err != nil {
		return err
	}
	if !complete || failed {
		return fmt.Errorf("cleanup success is not confirmed")
	}
	return nil
}

func (r *Reconciler) ensureCleanupContract(ctx context.Context, move *volumeapi.Move) error {
	if move == nil || move.Status.SourceCopy == nil || move.Status.DestinationCopy == nil || move.UID == "" {
		return fmt.Errorf("move cleanup contract is incomplete")
	}
	spec, err := moveCleanupSpec(*move)
	if err != nil {
		return err
	}
	return r.ensureCleanupSpec(ctx, move, spec)
}

func (r *Reconciler) ensureCleanupSpec(ctx context.Context, move *volumeapi.Move, spec cleanupapi.Spec) error {
	if !r.cleanupContractComplete(move, spec) {
		return fmt.Errorf("move cleanup contract is incomplete")
	}
	request, err := r.Cleanups.Ensure(ctx, spec)
	if err != nil {
		return err
	}
	if request.Status.Phase == cleanupapi.PhaseCompleted {
		_, _, err := r.observeTerminalExecutor(ctx, request)
		return err
	}
	// Historical releases could already open an absence fence before a Job
	// exited. Observe that executor before either opening or consuming a fence.
	if cleanupJobNeedsSettlement(request) || request.Status.Phase != cleanupapi.PhaseVerifying && request.Status.Phase != cleanupapi.PhaseConfirmingAbsence {
		var ready bool
		request, ready, err = r.CleanupOperator.AdvanceReclaim(ctx, request, r.Cleanups)
		if err != nil {
			return err
		}
		if !ready {
			return errCleanupPending
		}
	}
	if request.Status.Phase == cleanupapi.PhaseCompleted {
		return nil
	}
	if !cleanupReceiptComplete(request, spec) {
		return fmt.Errorf("cleanup receipt is incomplete")
	}
	return r.driveCleanupAbsence(ctx, request)
}

// cleanupContractComplete reports whether this reconciler may drive that exact
// journal for that exact Move. Anything less is an incomplete contract, never a
// journal to write.
func (r *Reconciler) cleanupContractComplete(move *volumeapi.Move, spec cleanupapi.Spec) bool {
	return r.Cleanups != nil && r.CleanupOperator != nil && move != nil && move.UID != "" &&
		spec.Authority == (cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}) &&
		spec.Target.VolumeID == move.Spec.VolumeID && spec.Validate() == nil
}

// cleanupReceiptComplete reports whether the journal carries the exact purge
// receipt the requested operation's executor wrote. Only such a journal may be
// driven through its absence fence.
func cleanupReceiptComplete(request cleanupapi.Cleanup, spec cleanupapi.Spec) bool {
	return (request.Status.Phase == cleanupapi.PhaseVerifying || request.Status.Phase == cleanupapi.PhaseConfirmingAbsence) &&
		request.Status.Receipt != nil && request.Status.Executor != nil &&
		request.Status.Receipt.OperationID == spec.OperationID && request.Status.Receipt.ExecutorUID == request.Status.Executor.ExecutionUID() &&
		request.Status.Receipt.Retired && request.Status.Receipt.Purged
}

// moveCleanupAuthority is the exact durable parent every Move cleanup journal,
// rollback or source, is embedded on.
func moveCleanupAuthority(move volumeapi.Move) cleanupapi.Authority {
	return cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID}
}

// moveCleanupPending reports whether a Move still carries a cleanup journal in a
// working phase. It reads the phase the Move object already decoded, so it
// answers before any journal store call: a Move that has already lost its
// protection finalizer cannot be read through the store at all.
func moveCleanupPending(move volumeapi.Move) bool {
	switch move.Status.CleanupPhase {
	case "", cleanupapi.PhaseCompleted, cleanupapi.PhaseNeedsReview:
		return false
	default:
		return true
	}
}

// cleanupIntentMatches reports whether an embedded journal is one of the two
// exact cleanup operations this Move owns. A journal bound to anything else is a
// contradiction to preserve, never an intent to drive.
func cleanupIntentMatches(move volumeapi.Move, spec cleanupapi.Spec) bool {
	switch spec.Reason {
	case "MoveRollback":
		return spec.OperationID == volumeapi.MoveRollbackOperationID(move.UID)
	case "MoveSource":
		return spec.OperationID == volumeapi.MoveCleanupOperationID(move.UID)
	default:
		return false
	}
}

// contradictedCleanup reports whether a journal drive failed because the exact
// Pool identity or lifecycle protection behind the absence fence is gone. No
// retry can restore it, so it belongs in the journal as NeedsReview rather than
// in the reconcile log forever.
func contradictedCleanup(err error) bool {
	return err != nil && (errors.Is(err, cleanupapi.ErrConflict) || apierrors.IsNotFound(err))
}

// reviewCleanupJournal records an identity contradiction on the journal itself
// and reports it. Executor, receipt and absence fence are carried forward
// unchanged: NeedsReview preserves evidence instead of erasing it. A failed
// write is returned as itself, so a transient API error is never mistaken for
// the durable contradiction.
func (r *Reconciler) reviewCleanupJournal(ctx context.Context, current cleanupapi.Cleanup, reason, message string) error {
	next := current.Status
	next.Phase, next.Reason, next.Message, next.LastTransitionTime = cleanupapi.PhaseNeedsReview, reason, message, ""
	if err := r.Cleanups.UpdateStatus(ctx, current, next); err != nil {
		return fmt.Errorf("mark cleanup for review after %s: %w", reason, err)
	}
	return needsRecoveryCleanupReview("%s: %s", reason, message)
}

// settleTerminalCleanup drives the unfinished cleanup journal of a terminal
// Move. Journal settlement is what releases the parent finalizer and closes the
// operation, so an unfinished journal is finished here instead of being parked
// for journal GC. A Move that already released its protection finalizer while
// its journal was mid-flight has that protection re-asserted first, because the
// journal store only accepts an exact protected parent; updateObjectFinalizer
// refuses to protect an object that is already deleting.
//
// Only a journal that already holds a purge receipt is driven. A terminal Move
// has released its capacity hold, so an intent that never reached a receipt has
// no live authority left to execute one: missing intent preserves data.
func (r *Reconciler) settleTerminalCleanup(ctx context.Context, move volumeapi.Move) error {
	if r.Cleanups == nil {
		return fmt.Errorf("cleanup journal store is not configured")
	}
	if !slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		return r.Repository.AddMoveFinalizer(ctx, move.Name, move.UID)
	}
	current, err := r.Cleanups.Get(ctx, moveCleanupAuthority(move))
	if errors.Is(err, cleanupapi.ErrNoJournal) {
		return nil
	}
	if err != nil {
		return err
	}
	if !cleanupIntentMatches(move, current.Spec) {
		return r.reviewCleanupJournal(ctx, current, "CleanupIntentMismatch",
			fmt.Sprintf("terminal Move carries cleanup operation %q for reason %q", current.Spec.OperationID, current.Spec.Reason))
	}
	switch current.Status.Phase {
	case cleanupapi.PhaseCompleted:
		_, _, err := r.observeTerminalExecutor(ctx, current)
		return err
	case cleanupapi.PhaseNeedsReview:
		return nil
	case cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence:
	default:
		return r.reviewCleanupJournal(ctx, current, "TerminalMoveCleanupIntentUnfinished",
			fmt.Sprintf("terminal Move carries a cleanup intent still in phase %q with no purge receipt", current.Status.Phase))
	}
	observed, ready, err := r.observeTerminalExecutor(ctx, current)
	if err != nil {
		if errors.Is(err, volumeapi.ErrPoolNotFound) {
			return r.reviewCleanupJournal(ctx, current, "CleanupPoolIdentityChanged", err.Error())
		}
		return err
	}
	if !ready {
		return nil
	}
	current = observed
	if err := r.driveCleanupAbsence(ctx, current); err != nil {
		if contradictedCleanup(err) {
			return r.reviewCleanupJournal(ctx, current, "CleanupPoolIdentityChanged", err.Error())
		}
		return err
	}
	return nil
}

func cleanupJobNeedsSettlement(current cleanupapi.Cleanup) bool {
	return (current.Status.Phase == cleanupapi.PhaseVerifying || current.Status.Phase == cleanupapi.PhaseConfirmingAbsence || current.Status.Phase == cleanupapi.PhaseCompleted) &&
		current.Status.Executor != nil && current.Status.Executor.Kind == ""
}

func (r *Reconciler) observeTerminalExecutor(ctx context.Context, current cleanupapi.Cleanup) (cleanupapi.Cleanup, bool, error) {
	if current.Status.Phase == cleanupapi.PhaseCompleted {
		ready, err := r.observeCompletedCleanup(ctx, current)
		return current, ready, err
	}
	if !cleanupJobNeedsSettlement(current) {
		return current, true, nil
	}
	if r.CleanupOperator == nil {
		return current, false, fmt.Errorf("cleanup executor settlement observer is not configured")
	}
	// Legacy work must already carry completed historical proof. The Node
	// adapter only checks that proof and its exact executor's quiescence.
	return r.CleanupOperator.AdvanceReclaim(ctx, current, r.Cleanups)
}

// driveCleanupAbsence finishes a receipt-bearing journal through its Pool
// fence. It never reaches the executor path, so a caller cannot start
// destructive work here: the terminal arm in particular must not act on behalf
// of a Move that already released its hold. The second pass is the read-back
// both the ordinary cleanup contract and the terminal arm perform.
func (r *Reconciler) driveCleanupAbsence(ctx context.Context, current cleanupapi.Cleanup) error {
	next, complete, err := r.Cleanups.ReconcileAbsence(ctx, current)
	if err != nil || complete {
		return err
	}
	if next.Status.Phase == cleanupapi.PhaseConfirmingAbsence {
		_, _, err = r.Cleanups.ReconcileAbsence(ctx, next)
	}
	return err
}

func moveCleanupSpec(move volumeapi.Move) (cleanupapi.Spec, error) {
	if move.Name == "" || move.UID == "" || move.Status.SourceCopy == nil {
		return cleanupapi.Spec{}, fmt.Errorf("move cleanup identity is incomplete")
	}
	spec := cleanupapi.Spec{
		OperationID: volumeapi.MoveCleanupOperationID(move.UID),
		Target:      *move.Status.SourceCopy,
		Reason:      "MoveSource",
		Authority:   moveCleanupAuthority(move),
	}
	if err := spec.Validate(); err != nil {
		return cleanupapi.Spec{}, err
	}
	return spec, nil
}
