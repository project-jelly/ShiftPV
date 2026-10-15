package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

type volumeBudgetRepository struct {
	Repository
	getMove func(context.Context, string) (volumeapi.Move, error)
}

func (r *volumeBudgetRepository) GetMove(ctx context.Context, name string) (volumeapi.Move, error) {
	return r.getMove(ctx, name)
}

func TestVolumeBudgetDefaultOverrideAndInheritedDeadline(t *testing.T) {
	for _, test := range []struct {
		name   string
		budget time.Duration
		parent time.Duration
		want   time.Duration
	}{
		{"default", 0, 0, 30 * time.Second},
		{"negative uses default", -time.Second, 0, 30 * time.Second},
		{"override", time.Minute, 0, time.Minute},
		{"earlier parent", time.Minute, 10 * time.Second, 10 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := context.Background()
			if test.parent > 0 {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(parent, test.parent)
				defer cancel()
			}
			started := time.Now()
			var supplied context.Context
			repo := &volumeBudgetRepository{getMove: func(ctx context.Context, name string) (volumeapi.Move, error) {
				supplied = ctx
				deadline, ok := ctx.Deadline()
				if !ok || deadline.Before(started.Add(test.want-time.Second)) || deadline.After(time.Now().Add(test.want)) {
					t.Fatalf("wrong effective deadline: deadline=%v want budget=%v", deadline, test.want)
				}
				if inherited, ok := parent.Deadline(); ok && !deadline.Equal(inherited) {
					t.Fatalf("earlier parent deadline changed: got=%v want=%v", deadline, inherited)
				}
				return volumeapi.Move{}, apierrors.NewNotFound(volumeapi.MoveResource.GroupResource(), name)
			}}
			r := &Reconciler{Repository: repo, ReconcileTimeout: test.budget}
			_, err := r.advanceVolume(parent, volumeWork{volumeID: "volume", moves: []moveReference{{name: "move", uid: "uid"}}})
			if err != nil || supplied == nil || supplied.Err() != context.Canceled || parent.Err() != nil {
				t.Fatalf("attempt context was not retired independently: err=%v child=%v parent=%v", err, supplied, parent.Err())
			}
		})
	}
}

func TestVolumeBudgetStopsFollowingMoveAndOwnerPod(t *testing.T) {
	for _, expiredAtEntry := range []bool{false, true} {
		t.Run(map[bool]string{false: "during first read", true: "already cancelled"}[expiredAtEntry], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if expiredAtEntry {
				cancel()
			}
			calls := 0
			repo := &volumeBudgetRepository{getMove: func(ctx context.Context, name string) (volumeapi.Move, error) {
				calls++
				if name != "first" {
					t.Fatalf("started following Move %q after expiry", name)
				}
				<-ctx.Done()
				return volumeapi.Move{}, ctx.Err()
			}}
			client := fake.NewSimpleClientset()
			r := &Reconciler{Repository: repo, Client: client, ReconcileTimeout: 20 * time.Millisecond}
			_, err := r.advanceVolume(ctx, volumeWork{volumeID: "volume", discover: expiredAtEntry,
				moves:     []moveReference{{name: "first", uid: "first-uid"}, {name: "next", uid: "next-uid"}},
				ownerPods: []podReference{{namespace: "workload", name: "held", uid: "held-uid"}},
			})
			want, wantCalls := context.DeadlineExceeded, 1
			if expiredAtEntry {
				want, wantCalls = context.Canceled, 0
			}
			if !errors.Is(err, want) || calls != wantCalls || len(client.Actions()) != 0 {
				t.Fatalf("expired attempt started more work: err=%v calls=%d actions=%v", err, calls, client.Actions())
			}
		})
	}
}

func TestReconcileAllBudgetContinuesNextVolumeUnlessParentCancelled(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(map[bool]string{false: "child timeout", true: "parent cancellation"}[cancelParent], func(t *testing.T) {
			f := newPreparationRegistryFixture(t, 2)
			beforeMove, beforeState := f.move(t, 0), f.state(t, 0)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			secondReads := 0
			repo := &volumeBudgetRepository{Repository: f.registry, getMove: func(child context.Context, name string) (volumeapi.Move, error) {
				if name == f.moveNames[0] {
					if cancelParent {
						cancel()
					}
					<-child.Done()
					return volumeapi.Move{}, child.Err()
				}
				secondReads++
				return f.registry.GetMove(child, name)
			}}
			f.reconciler.Repository, f.reconciler.ReconcileTimeout = repo, 250*time.Millisecond
			err := f.reconciler.ReconcileAll(ctx)
			want := context.DeadlineExceeded
			if cancelParent {
				want = context.Canceled
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(f.move(t, 0), beforeMove) || !reflect.DeepEqual(f.state(t, 0), beforeState) {
				t.Fatalf("expired observation changed authority: err=%v move=%+v state=%+v", err, f.move(t, 0), f.state(t, 0))
			}
			second := f.move(t, 1)
			if cancelParent {
				if secondReads != 0 || second.Status.SourceCopy != nil {
					t.Fatal("parent cancellation started another Volume")
				}
			} else if secondReads == 0 || second.Status.SourceCopy == nil || second.Status.Phase != "Preparing" {
				t.Fatalf("one child's timeout prevented the next durable plan: reads=%d move=%+v", secondReads, second)
			}
			assertNoEviction(t, f.client)
		})
	}
}

func TestVolumeBudgetExpiryPreservesPendingCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cause := errors.New("original pending probe error")
	repo := &volumeBudgetRepository{getMove: func(context.Context, string) (volumeapi.Move, error) {
		cancel()
		return volumeapi.Move{}, errors.Join(ErrCapacityProbePending, cause)
	}}
	r := &Reconciler{Repository: repo}
	_, err := r.advanceVolume(ctx, volumeWork{volumeID: "volume", moves: []moveReference{{name: "move", uid: "uid"}}})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCapacityProbePending) || !errors.Is(err, cause) {
		t.Fatalf("expiry discarded the pending operation's cause: %v", err)
	}
}
