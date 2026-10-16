package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/admission"
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

type HistoryInspector interface {
	MovesSettled(context.Context, []volumeapi.Move, map[string]cleanupapi.Cleanup) error
}

type Reconciler struct {
	HistoryInspector           HistoryInspector
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
		AdvanceReclaim(context.Context, cleanupapi.Cleanup, cleanupapi.Journal) (cleanupapi.Cleanup, bool, error)
		ObserveCompletedCleanup(context.Context, cleanupapi.Cleanup) (bool, error)
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
	if legacyMoveProtocol(move) {
		if err := r.validateSettledMoves(ctx, []volumeapi.Move{move}); err != nil {
			return fmt.Errorf("retain legacy Move without execution: %w", err)
		}
		return r.retireMoveHistory(ctx, move)
	}
	if !supportedMoveProtocol(move) {
		return fmt.Errorf("retain unsupported protocol %q", move.Spec.ProtocolVersion)
	}
	return r.reconcileMove(ctx, move)
}

func legacyMoveProtocol(move volumeapi.Move) bool {
	return move.Spec.ProtocolVersion == "" && move.Status.Preparation == nil
}

func supportedMoveProtocol(move volumeapi.Move) bool {
	return move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1
}

func (r *Reconciler) reconcileTerminalMove(ctx context.Context, move volumeapi.Move) error {
	if err := r.observeTerminalCleanupHistory(ctx, move); err != nil {
		return err
	}
	if done, err := r.finalizeSourceUsage(ctx, move); err != nil || !done {
		return err
	}
	return r.retireMoveHistory(ctx, move)
}

func (r *Reconciler) retireMoveHistory(ctx context.Context, move volumeapi.Move) error {
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
