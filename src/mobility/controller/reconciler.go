package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const (
	admissionNamespaceLabel           = admission.MobilityNamespaceLabel
	placementHoldName                 = admission.PlacementHold
	placementAnnotationKey            = admission.PlacementKey
	DefaultMoveJournalRetention       = 7 * 24 * time.Hour
	DefaultPlacementPriorityClassName = "shiftpv-placement"
	DefaultReconcileTimeout           = 30 * time.Second
)

type Repository interface {
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	Get(context.Context, string) (volumeapi.State, error)
	CompareAndSetState(context.Context, string, string, string, string, volumeapi.State) error
	CompareAndSetSourceState(context.Context, string, volumeapi.State, volumeapi.State) error
	RequestPoolScan(context.Context, volume.CopyIdentity) (int64, error)
	Pools(context.Context) ([]volumeapi.Pool, error)
	ObservePools(context.Context) (volumeapi.PoolSnapshot, error)
	ReadyPools(context.Context) ([]volumeapi.Pool, error)
	PoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	ReadyPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	InventoryPoolForIdentity(context.Context, string, string, string) (volumeapi.Pool, error)
	CreateMove(context.Context, string, volumeapi.MoveSpec) (volumeapi.Move, error)
	AddMoveFinalizer(context.Context, string, string) error
	RemoveMoveFinalizer(context.Context, string, string) error
	DeleteMove(context.Context, string, string) error
	ListMoves(context.Context) ([]volumeapi.Move, error)
	GetMove(context.Context, string) (volumeapi.Move, error)
	SetMoveStatus(context.Context, string, string, volumeapi.MoveStatus) error
	CompareAndSetMoveStatus(context.Context, volumeapi.Move, volumeapi.MoveStatus) (volumeapi.Move, error)
	RecordSourceUsage(context.Context, volumeapi.Move, volume.CopyIdentity, int64) (volumeapi.Move, error)
}

type CapacityProbe interface {
	StatFSForPool(context.Context, volumeapi.Pool) (poolcapacity.Filesystem, error)
}

type SourceUsage interface {
	AdvanceSourceUsage(context.Context, volumeapi.Move, volume.CopyIdentity) (int64, bool, error)
	FinalizeSourceUsage(context.Context, volumeapi.Move) (bool, error)
}

type Reconciler struct {
	Client                     kubernetes.Interface
	Repository                 Repository
	CapacityProbe              CapacityProbe
	SourceUsage                SourceUsage
	PoolLocks                  *poolcapacity.Locker
	Namespace                  string
	HelperImage                string
	PlacementPriorityClassName string
	ServiceAccountName         string
	Cleanups                   *cleanupapi.Store
	CleanupOperator            interface {
		AdvanceReclaim(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error)
	}
	Interval time.Duration
	// ReconcileTimeout bounds one Volume pass; non-positive values use DefaultReconcileTimeout.
	ReconcileTimeout        time.Duration
	MoveJournalRetention    time.Duration
	PoolReadinessStaleAfter time.Duration
	Now                     func() time.Time
	Recorder                record.EventRecorder
	Wake                    <-chan struct{}
	ObserveDiscovery        func(map[string]int, error)
}

func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.runVolumes(ctx)
}

func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.reconcileVolumePass(ctx)
}

func (r *Reconciler) reconcilePersistedMove(ctx context.Context, move volumeapi.Move) error {
	if !supportedMoveProtocol(move) {
		return fmt.Errorf("retain unsupported protocol %q", move.Spec.ProtocolVersion)
	}
	if handled, err := r.reconcilePreparedStage(ctx, move); handled || err != nil {
		return err
	}
	phase := fsm.Phase(move.Status.Phase)
	if phase == fsm.PhaseBlocked && move.Spec.Recovery == "ResumeOwner" && move.Status.RecoveryPhase != recoveryRecovered {
		// A read-only measurement must not delay restoring source authority.
		_, cleanupErr := r.finalizeSourceUsage(ctx, move)
		if ctx.Err() != nil {
			return errors.Join(cleanupErr, ctx.Err())
		}
		return errors.Join(r.reconcileRecovery(ctx, move), cleanupErr)
	}
	if phase == fsm.PhaseSucceeded || (phase == fsm.PhaseBlocked && move.Status.RecoveryPhase == recoveryRecovered) {
		// Finish a terminal journal's destructive operation before releasing
		// the finalizer it still depends on. Re-observe after settlement.
		if moveCleanupPending(move) {
			return r.settleTerminalCleanup(ctx, move)
		}
		if !volumeapi.MoveCleanupSettled(move) {
			return fmt.Errorf("retain terminal move: cleanup and capacity are not settled")
		}
		return r.reconcileTerminalMove(ctx, move)
	}
	if phase == fsm.PhaseBlocked {
		_, err := r.finalizeSourceUsage(ctx, move)
		return err
	}
	return r.reconcileMove(ctx, move)
}

func legacyMoveProtocol(move volumeapi.Move) bool {
	return move.Spec.ProtocolVersion == "" && move.Status.Preparation == nil
}

func supportedMoveProtocol(move volumeapi.Move) bool {
	return legacyMoveProtocol(move) || move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1
}

func (r *Reconciler) reconcileTerminalMove(ctx context.Context, move volumeapi.Move) error {
	if done, err := r.finalizeSourceUsage(ctx, move); err != nil || !done {
		return err
	}
	if slices.Contains(move.Finalizers, volumeapi.MoveProtectionFinalizer) {
		return r.Repository.RemoveMoveFinalizer(ctx, move.Name, move.UID)
	}
	if len(move.Finalizers) != 0 {
		return nil
	}

	transitionedAt, err := time.Parse(time.RFC3339Nano, move.Status.LastTransitionTime)
	if err != nil {
		return fmt.Errorf("retain journal with invalid terminal transition time: %w", err)
	}
	now := r.now()
	if now.Before(transitionedAt) {
		return fmt.Errorf("retain journal whose terminal transition time is in the future")
	}
	retention := r.MoveJournalRetention
	if retention <= 0 {
		retention = DefaultMoveJournalRetention
	}
	if now.Before(transitionedAt.Add(retention)) {
		return nil
	}

	state, err := r.Repository.Get(ctx, move.Spec.VolumeID)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("confirm volume no longer references journal: %w", err)
	}
	if err == nil && state.ActiveMove == move.Name {
		return fmt.Errorf("retain journal still referenced by active volume")
	}
	klog.Infof("deleting settled ShiftPVMove journal %s after %s retention", move.Name, retention)
	return r.Repository.DeleteMove(ctx, move.Name, move.UID)
}

func (r *Reconciler) validate() error {
	if r == nil || r.Client == nil || r.Repository == nil || r.SourceUsage == nil || r.Namespace == "" || r.HelperImage == "" {
		return fmt.Errorf("mobility reconciler is not configured")
	}
	return nil
}
