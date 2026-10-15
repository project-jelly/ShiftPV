package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type expiredStatusRepository struct {
	Repository
	writes int
}

type expirySourceUsage struct {
	SourceUsage
	finalize func(context.Context, volumeapi.Move) (bool, error)
}

func (s expirySourceUsage) FinalizeSourceUsage(ctx context.Context, move volumeapi.Move) (bool, error) {
	return s.finalize(ctx, move)
}

type expiryRecoveryRepository struct {
	Repository
	gets int
}

func (r *expiryRecoveryRepository) Get(context.Context, string) (volumeapi.State, error) {
	r.gets++
	return volumeapi.State{}, errors.New("unexpected recovery read")
}

func TestExpiredSourceUsageFinalizationDoesNotStartRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cause := errors.New("finalization reply failed")
	repo := &expiryRecoveryRepository{}
	r := &Reconciler{Repository: repo, SourceUsage: expirySourceUsage{finalize: func(context.Context, volumeapi.Move) (bool, error) {
		cancel()
		return false, cause
	}}}
	move := volumeapi.Move{Spec: volumeapi.MoveSpec{Recovery: "ResumeOwner"}, Status: volumeapi.MoveStatus{Phase: "Blocked"}}
	err := r.reconcilePersistedMove(ctx, move)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || repo.gets != 0 {
		t.Fatalf("expired finalization started recovery: gets=%d err=%v", repo.gets, err)
	}
}

type expiryCleanupObserver struct {
	cancel context.CancelFunc
	cause  error
}

func (o expiryCleanupObserver) AdvanceReclaim(_ context.Context, current cleanupapi.Cleanup, _ helperpod.CleanupJournal) (cleanupapi.Cleanup, bool, error) {
	o.cancel()
	return current, false, o.cause
}

func TestExpiredCleanupObservationDoesNotRereadJournal(t *testing.T) {
	for _, cause := range []error{nil, errors.New("cleanup observation failed")} {
		t.Run(map[bool]string{true: "pending", false: "error"}[cause == nil], func(t *testing.T) {
			r, repo, incoming, _ := rollbackRecoveryFixture(t, nil)
			move := repo.moves[0]
			spec := rollbackCleanupSpec(move, incoming)
			before, err := r.Cleanups.Ensure(context.Background(), spec)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			readsAfterExpiry := 0
			r.Cleanups.Client.(*dynamicfake.FakeDynamicClient).PrependReactor("get", "*", func(ktesting.Action) (bool, runtime.Object, error) {
				if ctx.Err() != nil {
					readsAfterExpiry++
				}
				return false, nil, nil
			})
			r.CleanupOperator = expiryCleanupObserver{cancel: cancel, cause: cause}
			complete, err := r.ensureRecoveryCleanup(ctx, move, spec)
			if complete || !errors.Is(err, context.Canceled) || (cause != nil && !errors.Is(err, cause)) || readsAfterExpiry != 0 {
				t.Fatalf("expired cleanup re-read its journal: complete=%v err=%v reads=%d", complete, err, readsAfterExpiry)
			}
			current, err := r.Cleanups.Get(context.Background(), spec.Authority)
			if err != nil || !reflect.DeepEqual(current, before) || !reflect.DeepEqual(repo.moves[0], move) {
				t.Fatalf("expiry changed cleanup or hold: err=%v journal=%+v move=%+v", err, current, repo.moves[0])
			}
		})
	}
}

func (r *expiredStatusRepository) SetMoveStatus(context.Context, string, string, volumeapi.MoveStatus) error {
	r.writes++
	return errors.New("unexpected legacy write")
}

func (r *expiredStatusRepository) CompareAndSetMoveStatus(context.Context, volumeapi.Move, volumeapi.MoveStatus) (volumeapi.Move, error) {
	r.writes++
	return volumeapi.Move{}, errors.New("unexpected prepared write")
}

func TestExpiredMoveStatusDoesNotWriteOrChangeDiagnostics(t *testing.T) {
	for _, protocol := range []string{"", volumeapi.MoveProtocolPreparedV1} {
		for _, expired := range []string{"cancelled", "deadline"} {
			for _, operation := range []string{"persist changed", "persist unchanged", "move error", "recovery error"} {
				t.Run(protocol+"/"+expired+"/"+operation, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					if expired == "deadline" {
						cancel()
						ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
					} else {
						cancel()
					}
					defer cancel()
					repo := &expiredStatusRepository{}
					recorder := record.NewFakeRecorder(1)
					r := &Reconciler{Repository: repo, Recorder: recorder}
					move := volumeapi.Move{Name: "move", UID: "uid", ResourceVersion: "1", Spec: volumeapi.MoveSpec{ProtocolVersion: protocol},
						Status: volumeapi.MoveStatus{Phase: "WaitingForCapacity", Reason: "keep", CapacityApproved: true}}
					previous := move.Status
					if operation == "persist changed" {
						move.Status.Phase = "Copying"
					}
					before := move
					cause := errors.New("original operation error")
					var err error
					switch operation {
					case "move error":
						err = r.recordMoveError(ctx, &move, previous, "ActionFailed", "test", cause)
					case "recovery error":
						err = r.recoveryError(ctx, move, "ObservationFailed", cause)
					default:
						err = r.persistMoveStatus(ctx, &move, previous)
						if !errors.Is(err, errMoveStatusPersistence) {
							t.Fatalf("expired persistence did not require re-observation: %v", err)
						}
					}
					if !errors.Is(err, ctx.Err()) || repo.writes != 0 || !reflect.DeepEqual(move, before) {
						t.Fatalf("expired caller changed status: err=%v writes=%d move=%+v", err, repo.writes, move)
					}
					if (operation == "move error" || operation == "recovery error") && !errors.Is(err, cause) {
						t.Fatalf("original error was discarded: %v", err)
					}
					assertNoEvent(t, recorder)
				})
			}
		}
	}
}

func TestLiveContextDeadlineErrorStillRecordsDiagnostic(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "move", true: "recovery"}[recovery], func(t *testing.T) {
			ctx := context.Background()
			move := volumeapi.Move{Name: "move", UID: "uid", Status: volumeapi.MoveStatus{Phase: "Blocked"}}
			repo := &memoryRepository{moves: []volumeapi.Move{move}}
			r := &Reconciler{Repository: repo}
			var err error
			if recovery {
				err = r.recoveryError(ctx, move, "ObservationFailed", context.DeadlineExceeded)
			} else {
				err = r.recordMoveError(ctx, &move, move.Status, "ObservationFailed", "read", context.DeadlineExceeded)
			}
			stored := repo.moves[0].Status
			if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil ||
				(recovery && stored.RecoveryReason != "ObservationFailed") || (!recovery && stored.Reason != "ObservationFailed") {
				t.Fatalf("live caller's API deadline error was treated as cancellation: err=%v status=%+v", err, stored)
			}
		})
	}
}
