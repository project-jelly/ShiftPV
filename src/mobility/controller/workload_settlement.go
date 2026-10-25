package controller

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/mobility/history"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// ErrRetiredMoveRejected means a per-UID maintenance request has no retirement
// authority. Retrying API/effect uncertainty must never widen this capability.
var ErrRetiredMoveRejected = errors.New("Move is not eligible for retirement settlement")

// AdvanceRetiredMove reuses normal settlement for one exact recorded Move.
// The caller must exclude normal writers throughout maintenance. This does not
// approve a new Move, restore a workload, adopt legacy execution or GC history.
func (r *Reconciler) AdvanceRetiredMove(ctx context.Context, name, uid string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := r.validate(); err != nil {
		return false, fmt.Errorf("%w: %v", ErrRetiredMoveRejected, err)
	}
	if !volume.ValidObjectName(name) || !volume.ValidIdentityToken(uid) {
		return false, fmt.Errorf("%w: exact Move name and UID are required", ErrRetiredMoveRejected)
	}
	move, err := r.Repository.GetMove(ctx, name)
	if err != nil {
		return false, err
	}
	if move.UID != uid || !supportedMoveProtocol(move) {
		return false, fmt.Errorf("%w: Move UID or prepared-v1 protocol does not match", ErrRetiredMoveRejected)
	}
	if !move.Status.SettlementRequested {
		state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
		if err != nil {
			return false, err
		}
		retired, handled, err := r.observeWorkloadRetirement(ctx, move, state)
		if err != nil {
			return false, err
		}
		if !handled || !retired.FSM.WorkloadRetired {
			return false, fmt.Errorf("%w: original claim has not retired from an approved Move", ErrRetiredMoveRejected)
		}
		if err := workloadSettlementAuthority(move, state); err != nil {
			return false, fmt.Errorf("%w: %v", ErrRetiredMoveRejected, err)
		}
	}
	if move.Status.SettlementRequested && volumeapi.MoveCleanupSettled(move) && len(move.Finalizers) == 0 {
		err := r.validateSettledMoves(ctx, []volumeapi.Move{move})
		return err == nil, err
	}
	return false, r.reconcilePersistedMove(ctx, move)
}

// Original claim lifetime is independent of service health and copy authority.
// Only confirmed original-UID retirement requests settlement. An in-use claim
// still waiting on PVC protection must finish normal workload teardown first.
func (r *Reconciler) observeWorkloadRetirement(ctx context.Context, move volumeapi.Move, state volumeapi.State) (observation, bool, error) {
	if !needsWorkloadLifetime(move) {
		return observation{}, false, nil
	}
	claim, err := r.Client.CoreV1().PersistentVolumeClaims(move.Status.ClaimNamespace).Get(ctx, move.Status.ClaimName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return observation{}, true, fmt.Errorf("observe original claim lifetime: %w", err)
	}
	if err == nil {
		if claim.UID == "" {
			return observation{}, true, fmt.Errorf("original claim observation has no UID")
		}
		if string(claim.UID) == move.Status.ClaimUID {
			return observation{}, false, nil
		}
	}
	if !move.Status.CapacityApproved && noDestinationEffectIntent(move) {
		if err := cancellablePreparation(move); err != nil {
			return observation{}, true, err
		}
		if err := volumeapi.ValidateMovePreparation(move); err != nil {
			return observation{}, true, err
		}
		if state.ActiveMove != move.Name {
			return observation{}, true, fmt.Errorf("retired preparation lost its exact source claim")
		}
		if state.Phase != volumeapi.PhaseReady && state.Phase != volumeapi.PhaseMoving && state.Phase != volumeapi.PhaseBlocked {
			return observation{}, true, fmt.Errorf("retired preparation has no restorable source")
		}
		if err := cancellationSourceIdentity(move, state); err != nil {
			return observation{}, true, err
		}
	} else if err := validatePreparedStage(move); err != nil {
		return observation{}, true, err
	}
	if err := recordedOwnerIntact(move, state); err != nil {
		return observation{}, true, err
	}
	return observation{Volume: state, FSM: fsm.Observation{WorkloadRetired: true}}, true, nil
}

func (r *Reconciler) observeBlockedLifecycle(ctx context.Context, move volumeapi.Move) (observation, error) {
	if needsWorkloadLifetime(move) {
		state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
		if err != nil {
			return observation{}, err
		}
		if retired, handled, err := r.observeWorkloadRetirement(ctx, move, state); handled || err != nil {
			return retired, err
		}
	}
	if needsRecoveryObservation(move) {
		return r.observeRecovery(ctx, move)
	}
	return observation{}, nil
}

func needsWorkloadLifetime(move volumeapi.Move) bool {
	if move.Spec.ProtocolVersion != volumeapi.MoveProtocolPreparedV1 || move.Status.SettlementRequested || move.Status.RecoveryPhase == recoveryRecovered {
		return false
	}
	switch fsm.Phase(move.Status.Phase) {
	case fsm.PhaseSucceeded, fsm.PhaseCancelled, fsm.PhaseCanceling:
		return false
	}
	return move.Status.CapacityApproved || !noDestinationEffectIntent(move) ||
		(move.Status.SourceCopy != nil && move.Status.Preparation != nil)
}

// The Volume fence competes with owner commit on the same exact copy. If that
// CAS loses, reobserve the winner; a stale phase never chooses rollback/forward.
func (r *Reconciler) requestWorkloadSettlement(ctx context.Context, move *volumeapi.Move, observed observation) error {
	state := observed.Volume
	if !observed.FSM.WorkloadRetired {
		return fmt.Errorf("workload settlement requires original claim retirement")
	}
	if err := workloadSettlementAuthority(*move, state); err != nil {
		return err
	}
	next := state
	next.Phase = volumeapi.PhaseBlocked
	if err := r.Repository.CompareAndSetSourceState(ctx, move.Spec.VolumeID, state, next); err != nil {
		return err
	}
	move.Status.SettlementRequested = true
	move.Status.RecoveryOwner, move.Status.RecoveryPhase = "", ""
	move.Status.RecoveryReason, move.Status.RecoveryMessage = "", ""
	return nil
}

func workloadSettlementAuthority(move volumeapi.Move, state volumeapi.State) error {
	if state.ActiveMove != move.Name || state.Deleting || state.HasCleanup ||
		(state.Phase != volumeapi.PhaseMoving && state.Phase != volumeapi.PhaseReady && state.Phase != volumeapi.PhaseBlocked) {
		return fmt.Errorf("workload settlement requires this Move's exact live authority")
	}
	return recordedOwnerIntact(move, state)
}

func recordedOwnerIntact(move volumeapi.Move, state volumeapi.State) error {
	copy := move.Status.SourceCopy
	if state.OwnerNode != move.Spec.SourceNode {
		if state.OwnerNode != move.Status.DestinationNode {
			return fmt.Errorf("owner is outside this Move's recorded plan")
		}
		copy = move.Status.DestinationCopy
	}
	if copy == nil || copy.Validate() != nil || copy.Role != volume.RoleServing || copy.VolumeID != move.Spec.VolumeID || copy.VolumeUID != state.UID ||
		copy.NodeName != state.OwnerNode || state.CurrentCopy == nil || *state.CurrentCopy != *copy {
		return fmt.Errorf("current owner does not carry this Move's exact recorded copy")
	}
	return nil
}

func recoveryRequested(move volumeapi.Move) bool {
	return move.Spec.Recovery == "ResumeOwner" || move.Status.SettlementRequested
}

func (r *Reconciler) applySettlementDecision(move *volumeapi.Move, before fsm.State, decision fsm.Decision) {
	if before.RecoveryPhase != decision.Next.RecoveryPhase {
		move.Status.RecoveryReason, move.Status.RecoveryMessage = "", ""
	}
	if decision.Action == fsm.ActionRequestSettlement {
		move.Status.Reason = decision.Reason
	}
	move.Status.Message = "automatic settlement after original claim retirement; waiting for owner verification, cleanup and temporary hold release"
	if move.Status.RecoveryPhase == recoveryRecovered {
		move.Status.Message = "original claim ended; Move settled; authoritative copy and Volume capacity retained"
	}
}

// A retired workload cannot supply publication proof. Fence and verify the
// retained destination instead; this alternative never applies to ResumeOwner.
func (r *Reconciler) settleUnpublishedDestination(ctx context.Context, move *volumeapi.Move, state volumeapi.State) (bool, error) {
	if ready, err := r.ensureRollbackScan(ctx, move); err != nil || !ready {
		return false, err
	}
	if !volumeapi.MoveRetiresUnpublishedSource(*move, state) {
		return false, fmt.Errorf("waiting for exact fenced destination authority without publication")
	}
	copy := move.Status.DestinationCopy
	pool, err := r.Repository.InventoryPoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return false, err
	}
	if !volumeapi.PoolHasUnpublishedCopy(pool, copy, move.Status.RollbackRequiredGeneration) {
		return false, fmt.Errorf("waiting for post-retirement inventory of the exact unpublished destination")
	}
	spec, err := moveCleanupSpec(*move)
	if err != nil {
		return false, err
	}
	return r.ensureRecoveryCleanup(ctx, *move, spec)
}

func (r *Reconciler) settlementArtifactsClosed(ctx context.Context, move volumeapi.Move) error {
	if r.Cleanups == nil {
		return fmt.Errorf("settlement cleanup history store is unavailable")
	}
	stored, err := r.Cleanups.GetHistory(ctx, moveCleanupAuthority(move))
	var journal *cleanupapi.Cleanup
	if err == nil {
		journal = &stored
		// CleanupPhase is decoded from this same embedded journal by Registry.
		// Use the fresh read rather than an older lifecycle snapshot.
		move.Status.CleanupPhase = stored.Status.Phase
	} else if !errors.Is(err, cleanupapi.ErrNoJournal) {
		return err
	}
	return history.ValidateSettlementCompletion(move, journal)
}

// CSI may delete the original Volume after ActiveMove was cleared but before
// the Move's final status write. Finalize recorded artifacts and owned verifier
// Jobs without requiring the already-deleted service or granting another effect.
func (r *Reconciler) observeSettlementRelease(ctx context.Context, move volumeapi.Move, state volumeapi.State, getErr error) (observation, bool, error) {
	result := observation{Volume: state, VolumeMissing: apierrors.IsNotFound(getErr)}
	if getErr != nil && !result.VolumeMissing {
		return result, true, getErr
	}
	if !result.VolumeMissing {
		if state.ActiveMove == move.Name {
			return result, false, nil
		}
		if state.ActiveMove != "" || state.OwnerNode != move.Status.RecoveryOwner {
			return result, true, fmt.Errorf("settlement release has conflicting Volume authority")
		}
		if err := recordedOwnerIntact(move, state); err != nil {
			return result, true, err
		}
		ready := state.Phase == volumeapi.PhaseReady && !state.Deleting && !state.HasCleanup
		deleting := state.Phase == volumeapi.PhaseDeleting && state.DeletionOperationID == "delete-"+state.UID
		if !ready && !deleting {
			return result, true, fmt.Errorf("settlement release has no accepted unlock or exact deletion fence")
		}
	}
	return result, true, r.settlementArtifactsClosed(ctx, move)
}
