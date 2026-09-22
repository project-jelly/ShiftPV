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
	admissionNamespaceLabel     = admission.MobilityNamespaceLabel
	placementHoldName           = admission.PlacementHold
	placementAnnotationKey      = admission.PlacementKey
	DefaultMoveJournalRetention = 7 * 24 * time.Hour
)

type Repository interface {
	ListVolumes(context.Context) (map[string]volumeapi.State, error)
	Get(context.Context, string) (volumeapi.State, error)
	CompareAndSetState(context.Context, string, string, string, string, volumeapi.State) error
	RequestPoolScan(context.Context, volume.CopyIdentity) (int64, error)
	Pools(context.Context) ([]volumeapi.Pool, error)
	ObservePools(context.Context) (volumeapi.PoolSnapshot, error)
	ReadyPools(context.Context) ([]volumeapi.Pool, error)
	ReadyPoolForNode(context.Context, string) (volumeapi.Pool, error)
	CreateMove(context.Context, string, volumeapi.MoveSpec) (volumeapi.Move, error)
	AddMoveFinalizer(context.Context, string, string) error
	RemoveMoveFinalizer(context.Context, string, string) error
	DeleteMove(context.Context, string, string) error
	ListMoves(context.Context) ([]volumeapi.Move, error)
	SetMoveStatus(context.Context, string, string, volumeapi.MoveStatus) error
}

type CapacityProbe interface {
	StatFS(context.Context, string) (poolcapacity.Filesystem, error)
	VolumeUsage(context.Context, string, string) (int64, error)
}

type Reconciler struct {
	Client             kubernetes.Interface
	Repository         Repository
	CapacityProbe      CapacityProbe
	PoolLocks          *poolcapacity.Locker
	Namespace          string
	HelperImage        string
	ServiceAccountName string
	Cleanups           *cleanupapi.Store
	CleanupOperator    interface {
		Reclaim(context.Context, cleanupapi.Cleanup, helperpod.CleanupJournal) (cleanupapi.Cleanup, error)
	}
	Interval                time.Duration
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
	interval := r.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.ReconcileAll(ctx); err != nil && !errors.Is(err, context.Canceled) {
			klog.Errorf("reconcile ShiftPV mobility: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.Wake:
		case <-ticker.C:
		}
	}
}

func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := r.discoverMoves(ctx); err != nil {
		return err
	}
	moves, err := r.Repository.ListMoves(ctx)
	if err != nil {
		return err
	}
	var reconcileErrors []error
	for _, move := range moves {
		phase := fsm.Phase(move.Status.Phase)
		if phase == fsm.PhaseBlocked && move.Spec.Recovery == "ResumeOwner" && move.Status.RecoveryPhase != recoveryRecovered {
			if err := r.reconcileRecovery(ctx, move); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("recover move %s: %w", move.Name, err))
			}
			continue
		}
		if phase == fsm.PhaseSucceeded || (phase == fsm.PhaseBlocked && move.Status.RecoveryPhase == recoveryRecovered) {
			// A terminal Move whose embedded journal is still working owns an
			// unfinished destructive operation. Finish it here instead of
			// parking it for journal GC, then re-observe before the terminal
			// path releases the finalizer this journal still depends on.
			if moveCleanupPending(move) {
				if err := r.settleTerminalCleanup(ctx, move); err != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("settle cleanup for terminal move %s: %w", move.Name, err))
				}
				continue
			}
			if !volumeapi.MoveCleanupSettled(move) {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("retain terminal move %s: cleanup and capacity are not settled", move.Name))
				continue
			}
			if err := r.reconcileTerminalMove(ctx, move); err != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf("reconcile terminal move %s: %w", move.Name, err))
			}
			continue
		}
		if phase == fsm.PhaseBlocked {
			continue
		}
		if err := r.reconcileMove(ctx, move); err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("move %s: %w", move.Name, err))
		}
	}
	return errors.Join(reconcileErrors...)
}

func (r *Reconciler) reconcileTerminalMove(ctx context.Context, move volumeapi.Move) error {
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
	if r == nil || r.Client == nil || r.Repository == nil || r.Namespace == "" || r.HelperImage == "" {
		return fmt.Errorf("mobility reconciler is not configured")
	}
	return nil
}
