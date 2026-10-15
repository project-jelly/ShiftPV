package nodeexecutor

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	rpcserver "github.com/project-jelly/ShiftPV/src/node/rpc/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

type cleanupRPCReply struct {
	f       moveClientFixture
	lose    bool
	noProof bool
	calls   atomic.Int32
}

func (r *cleanupRPCReply) Execute(ctx context.Context, req *protocol.EffectRequest, op protocol.Operation) error {
	r.calls.Add(1)
	if op != protocol.Operation_RECLAIM || req.VolumeName != r.f.cleanup.Spec.Target.VolumeID || req.VolumeUid != r.f.cleanup.Spec.Target.VolumeUID || req.OperationId != r.f.cleanup.Spec.OperationID {
		return fmt.Errorf("RPC did not address the exact target Volume/operation")
	}
	if r.noProof {
		return nil
	}
	current, err := r.f.store.Get(ctx, r.f.cleanup.Spec.Authority)
	if err != nil {
		return err
	}
	if err := r.f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
		return err
	}
	if r.lose {
		return context.DeadlineExceeded
	}
	return nil
}

func attachCleanupRPC(t *testing.T, f moveClientFixture, executor rpcserver.Executor) {
	t.Helper()
	cert, public, err := security.NewCertificate(f.node.Identity.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.changePod(t, func(p *corev1.Pod) {
		p.Annotations[security.CertificateAnnotation] = public
		p.Annotations[security.PortAnnotation] = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		p.Status.PodIP = "127.0.0.1"
	})
	f.kube.PrependReactor("create", "tokenreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{security.Audience},
			User: authv1.UserInfo{Username: "system:serviceaccount:shiftpv:controller", UID: "controller-uid"}}}, nil
	})
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("credential"), 0600); err != nil {
		t.Fatal(err)
	}
	f.client.RPC = &connection.Client{TokenFile: path}
	f.client.Timeout = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- rpcserver.Serve(ctx, listener, cert, security.Authorizer{Client: f.kube, Namespace: "shiftpv", ServiceAccount: "controller"},
			&rpcserver.Service{Identity: f.node.Identity, Executor: executor})
	}()
	t.Cleanup(func() {
		_ = f.client.RPC.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("RPC server did not stop")
		}
	})
}

func TestMoveCleanupRPCResponseLossUsesExactSavedReceipt(t *testing.T) {
	for _, reason := range []string{"MoveSource", "MoveRollback"} {
		t.Run(reason, func(t *testing.T) {
			f := newMoveClientFixture(t, reason)
			reply := &cleanupRPCReply{f: f, lose: true}
			attachCleanupRPC(t, f, reply)
			ctx := context.Background()
			if _, err := f.client.Reclaim(ctx, f.cleanup, f.store); status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("lost reply=%v", err)
			}
			verified := f.current(t)
			if verified.Status.Phase != cleanupapi.PhaseVerifying || verified.Status.Receipt == nil || verified.Status.Receipt.ExecutorUID != f.node.Identity.PodUID {
				t.Fatalf("response loss erased receipt: %+v", verified)
			}
			// Parent reconciliation may release ActiveMove before the controller
			// re-observes a lost response. The exact receipt needs no new dispatch.
			parent, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			unstructured.RemoveNestedField(parent.Object, "status", "activeMove")
			if _, err := f.dynamic.Resource(volumeapi.VolumeResource).Update(ctx, parent, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := f.kube.CoreV1().Pods("shiftpv").Delete(ctx, "node-a", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			result, err := f.client.Reclaim(ctx, f.cleanup, f.store)
			if err != nil || !reflect.DeepEqual(result, verified) || reply.calls.Load() != 1 {
				t.Fatalf("retry did not reuse exact receipt: %+v calls=%d err=%v", result, reply.calls.Load(), err)
			}
			f.assertNoJobs(t)
		})
	}
}

func TestMoveCleanupRPCReplyIsNotCompletionEvidence(t *testing.T) {
	f := newMoveClientFixture(t, "MoveSource")
	reply := &cleanupRPCReply{f: f, noProof: true}
	attachCleanupRPC(t, f, reply)
	if _, err := f.client.Reclaim(context.Background(), f.cleanup, f.store); err != cleanupapi.ErrConflict {
		t.Fatalf("bare acknowledgement became completion: %v", err)
	}
	if current := f.current(t); current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Receipt != nil || reply.calls.Load() != 1 {
		t.Fatalf("unproved acknowledgement mutated journal: %+v", current)
	}
	f.assertNoJobs(t)
}

func TestMoveCleanupStoredBindingLostResponseDoesNotInvokeRPC(t *testing.T) {
	f := newMoveClientFixture(t, "MoveSource")
	reply := &cleanupRPCReply{f: f}
	attachCleanupRPC(t, f, reply)
	journal := moveClientJournal{CleanupJournal: f.store, write: func(ctx context.Context, current cleanupapi.Cleanup, next cleanupapi.Status) error {
		if err := f.store.UpdateStatus(ctx, current, next); err != nil {
			return err
		}
		return apierrors.NewTimeoutError("stored binding, response lost", 1)
	}}
	if _, err := f.client.Reclaim(context.Background(), f.cleanup, journal); !apierrors.IsTimeout(err) || reply.calls.Load() != 0 {
		t.Fatalf("effect after ambiguous binding: calls=%d err=%v", reply.calls.Load(), err)
	}
	if result, err := f.client.Reclaim(context.Background(), f.cleanup, f.store); err != nil || result.Status.Receipt == nil || reply.calls.Load() != 1 {
		t.Fatalf("binding retry did not execute once: result=%+v calls=%d err=%v", result, reply.calls.Load(), err)
	}
}

func TestMoveCleanupCompletedReceiptSurvivesReleasedAuthority(t *testing.T) {
	f := newMoveClientFixture(t, "MoveRollback")
	ctx := context.Background()
	if err := f.client.bindCleanup(ctx, f.cleanup, f.store); err != nil {
		t.Fatal(err)
	}
	current := f.current(t)
	if err := f.store.UpdateStatus(ctx, current, cleanupReceipt(current)); err != nil {
		t.Fatal(err)
	}
	current = f.current(t)
	next := current.Status
	next.Phase = cleanupapi.PhaseConfirmingAbsence
	next.AbsenceProof = &cleanupapi.AbsenceProof{RequestID: "absence-move-uid", PoolName: current.Spec.Target.PoolName, PoolUID: current.Spec.Target.PoolUID, RequiredGeneration: 2}
	if err := f.store.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	current = f.current(t)
	next = current.Status
	next.Phase = cleanupapi.PhaseCompleted
	next.AbsenceProof.ObservedGeneration = 2
	next.AbsenceProof.Valid, next.AbsenceProof.Complete, next.AbsenceProof.Absent = true, true, true
	next.AbsenceProof.ConfirmedAt = time.Now().UTC().Format(time.RFC3339Nano)
	next.SettledAt = next.AbsenceProof.ConfirmedAt
	if err := f.store.UpdateStatus(ctx, current, next); err != nil {
		t.Fatal(err)
	}
	completed := f.current(t)
	parent, err := f.dynamic.Resource(volumeapi.VolumeResource).Get(ctx, testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(parent.Object, "status", "activeMove")
	if _, err := f.dynamic.Resource(volumeapi.VolumeResource).Update(ctx, parent, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.CoreV1().Pods("shiftpv").Delete(ctx, "node-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := f.dynamic.Resource(volumeapi.PoolResource).Delete(ctx, "pool-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err := f.client.Reclaim(ctx, f.cleanup, f.store)
	if err != nil || !reflect.DeepEqual(result, completed) {
		t.Fatalf("exact completion needed retired execution authority: result=%+v err=%v", result, err)
	}
	changed := f.cleanup
	changed.Spec.OperationID = "other-operation"
	if _, err := f.client.Reclaim(ctx, changed, f.store); err != cleanupapi.ErrConflict {
		t.Fatalf("foreign receipt adopted: %v", err)
	}
	f.assertNoJobs(t)
}
