//go:build linux

package executor

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type replies struct {
	node     *Node
	lost     atomic.Bool
	falseAck bool
	calls    atomic.Int32
}

func (r *replies) Execute(ctx context.Context, req *protocol.EffectRequest, op protocol.Operation) error {
	r.calls.Add(1)
	if r.falseAck {
		return nil
	}
	if err := r.node.Execute(ctx, req, op); err != nil {
		return err
	}
	if r.lost.Swap(false) {
		return context.DeadlineExceeded
	}
	return nil
}
func attachRPC(t *testing.T, client *Client, node *Node, kube *kubefake.Clientset, executor rpcserver.Executor) {
	t.Helper()
	cert, public, err := security.NewCertificate(node.Identity.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node.RPCCertificate, node.RPCPort = public, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := node.advertise(context.Background()); err != nil {
		t.Fatal(err)
	}
	pod, err := kube.CoreV1().Pods(node.Identity.Namespace).Get(context.Background(), node.Identity.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.PodIP = "127.0.0.1"
	if _, err := kube.CoreV1().Pods(pod.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	kube.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{security.Audience}, User: authv1.UserInfo{Username: "system:serviceaccount:shiftpv:controller", UID: "controller-uid"}}}, nil
	})
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("credential"), 0600); err != nil {
		t.Fatal(err)
	}
	client.RPC = &connection.Client{TokenFile: path}
	client.Timeout = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	service := &rpcserver.Service{Identity: node.Identity, Executor: executor, VerifyIdentity: node.VerifyRPC}
	go func() {
		done <- rpcserver.Serve(ctx, listener, cert, security.Authorizer{Client: kube, Namespace: "shiftpv", ServiceAccount: "controller"}, service)
	}()
	t.Cleanup(func() {
		_ = client.RPC.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("RPC did not stop")
		}
	})
	node.HostRoot = t.TempDir()
	if err := os.MkdirAll(filepath.Join(node.HostRoot, "mnt", "pool"), 0700); err != nil {
		t.Fatal(err)
	}
}
func TestRPCReplyNeverSubstitutesForCreationReceipt(t *testing.T) {
	client, node, _, kube, state := fixture(t)
	r := &replies{node: node, falseAck: true}
	attachRPC(t, client, node, kube, r)
	if err := client.CreateCopy(context.Background(), *state.CurrentCopy); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("unproved reply=%v", err)
	}
	current, _ := client.Volumes.Get(context.Background(), testID)
	if current.Phase != volumeapi.PhaseNodeCreating || current.CreationExecutor == nil || current.CreationReceipt != nil {
		t.Fatal("reply bypassed durable authority")
	}
	if _, err := os.Stat(filepath.Join(node.HostRoot, "mnt", "pool", "volumes", testID)); !os.IsNotExist(err) {
		t.Fatal("false acknowledgement caused filesystem effects")
	}
}
func TestRPCResponseLossRetriesSavedCreateAndCleanupReceipts(t *testing.T) {
	client, node, _, kube, state := fixture(t)
	r := &replies{node: node}
	r.lost.Store(true)
	attachRPC(t, client, node, kube, r)
	ctx := context.Background()
	if err := client.CreateCopy(ctx, *state.CurrentCopy); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("lost reply=%v", err)
	}
	current, _ := client.Volumes.Get(ctx, testID)
	if current.Phase != volumeapi.PhaseNodeCreating || !volumeapi.ValidCreationReceipt(current) {
		t.Fatal("lost reply erased receipt/hold")
	}
	original := *current.CreationReceipt
	if err := client.CreateCopy(ctx, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	current, _ = client.Volumes.Get(ctx, testID)
	if *current.CreationReceipt != original || r.calls.Load() != 1 {
		t.Fatal("retry reexecuted committed creation")
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	state, err := client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy, Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
	if err != nil {
		t.Fatal(err)
	}
	r.lost.Store(true)
	if _, err := client.Reclaim(ctx, cleanup, node.Cleanups); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("lost cleanup reply=%v", err)
	}
	verified, err := client.Reclaim(ctx, cleanup, node.Cleanups)
	if err != nil || verified.Status.Phase != cleanupapi.PhaseVerifying || verified.Status.Receipt == nil {
		t.Fatalf("cleanup=%+v err=%v", verified, err)
	}
	current, _ = client.Volumes.Get(ctx, testID)
	if current.Phase != volumeapi.PhaseDeleting || verified.Status.AbsenceProof != nil {
		t.Fatal("RPC released hold without fresh absence")
	}
	if r.calls.Load() != 2 {
		t.Fatal("retry reexecuted committed deletion")
	}
	if _, err := os.Stat(filepath.Join(node.HostRoot, "mnt", "pool", "volumes", testID)); !os.IsNotExist(err) {
		t.Fatal("cleanup did not reclaim copy")
	}
}
func TestRPCAndRecoveryShareOneImmutableReceipt(t *testing.T) {
	client, node, dynamic, kube, state := fixture(t)
	attachRPC(t, client, node, kube, node)
	ctx := context.Background()
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	dynamic.PrependReactor("update", "shiftpvvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if _, found, _ := unstructured.NestedMap(object.Object, "status", "creationReceipt"); found {
			writes.Add(1)
		}
		return false, nil, nil
	})
	req := &protocol.EffectRequest{ExecutorUid: node.Identity.PodUID, NodeName: node.Identity.NodeName, VolumeName: testID, VolumeUid: state.UID, OperationId: state.CreationOperationID}
	var workers sync.WaitGroup
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			if i%2 == 0 {
				failures <- node.Execute(ctx, req, protocol.Operation_CREATE)
			} else {
				failures <- node.execute(ctx, testID)
			}
		}(i)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 1 {
		t.Fatalf("concurrent execution wrote %d immutable receipts", writes.Load())
	}
	altered := &protocol.EffectRequest{ExecutorUid: req.ExecutorUid, NodeName: req.NodeName, VolumeName: req.VolumeName, VolumeUid: req.VolumeUid, OperationId: "different-operation"}
	if err := node.Execute(ctx, altered, protocol.Operation_CREATE); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("foreign operation=%v", err)
	}
}

func TestRPCRefusesWrongParentAndUnapprovedOperation(t *testing.T) {
	client, node, _, kube, state := fixture(t)
	attachRPC(t, client, node, kube, node)
	ctx := context.Background()
	req := &protocol.EffectRequest{ExecutorUid: node.Identity.PodUID, NodeName: node.Identity.NodeName, VolumeName: testID, VolumeUid: state.UID, OperationId: state.CreationOperationID}
	if err := node.Execute(ctx, req, protocol.Operation_CREATE); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("unbound create=%v", err)
	}
	if err := client.Volumes.BindCreation(ctx, state, node.Identity); err != nil {
		t.Fatal(err)
	}
	altered := &protocol.EffectRequest{ExecutorUid: req.ExecutorUid, NodeName: req.NodeName, VolumeName: req.VolumeName, VolumeUid: "foreign-uid", OperationId: req.OperationId}
	if err := node.Execute(ctx, altered, protocol.Operation_CREATE); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("wrong parent=%v", err)
	}
	if err := node.Execute(ctx, req, protocol.Operation_OPERATION_UNSPECIFIED); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("wrong operation=%v", err)
	}
	if err := node.Execute(ctx, nil, protocol.Operation_CREATE); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("missing request=%v", err)
	}
	if err := node.Execute(ctx, req, protocol.Operation_RECLAIM); err == nil {
		t.Fatal("missing cleanup approval reclaimed copy")
	}
	if err := node.Execute(ctx, req, protocol.Operation_CREATE); err != nil {
		t.Fatal(err)
	}
	if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
		t.Fatal(err)
	}
	// Ready retries may return the same creation receipt without another effect.
	if err := node.Execute(ctx, req, protocol.Operation_CREATE); err != nil {
		t.Fatal(err)
	}
	state, err := client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy, Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
	if err != nil {
		t.Fatal(err)
	}
	req.OperationId = state.DeletionOperationID
	if err := node.Execute(ctx, req, protocol.Operation_RECLAIM); !errors.Is(err, cleanupapi.ErrConflict) {
		t.Fatalf("unbound cleanup=%v", err)
	}
	if err := bindNodeCleanup(ctx, node, cleanup); err != nil {
		t.Fatal(err)
	}
	original := req.OperationId
	req.OperationId = "foreign-operation"
	if err := node.Execute(ctx, req, protocol.Operation_RECLAIM); !errors.Is(err, cleanupapi.ErrConflict) {
		t.Fatalf("wrong cleanup operation=%v", err)
	}
	req.OperationId = original
	if err := node.Execute(ctx, req, protocol.Operation_RECLAIM); err != nil {
		t.Fatal(err)
	}
	before, _ := node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	if err := node.Execute(ctx, req, protocol.Operation_RECLAIM); err != nil {
		t.Fatal(err)
	}
	after, _ := node.Cleanups.Get(ctx, cleanup.Spec.Authority)
	if before.Status.Receipt == nil || after.Status.Receipt == nil || *before.Status.Receipt != *after.Status.Receipt {
		t.Fatal("idempotent cleanup changed receipt")
	}
}

func TestRPCDeletionResponseStillRequiresReceiptAndFreshAbsence(t *testing.T) {
	for _, falseAck := range []bool{false, true} {
		t.Run(strconv.FormatBool(falseAck), func(t *testing.T) {
			client, node, _, kube, state := fixture(t)
			r := &replies{node: node, falseAck: falseAck}
			attachRPC(t, client, node, kube, r)
			ctx := context.Background()
			if !falseAck {
				if err := client.CreateCopy(ctx, *state.CurrentCopy); err != nil {
					t.Fatal(err)
				}
			}
			if err := client.Volumes.CompleteCreate(ctx, testID, state.UID, *state.CurrentCopy); err != nil {
				t.Fatal(err)
			}
			state, err := client.Volumes.BeginDelete(ctx, testID, state.UID, *state.CurrentCopy)
			if err != nil {
				t.Fatal(err)
			}
			cleanup, err := node.Cleanups.Ensure(ctx, cleanupapi.Spec{OperationID: state.DeletionOperationID, Target: *state.CurrentCopy, Reason: "VolumeDelete", Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: testID, UID: state.UID}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Reclaim(ctx, cleanup, node.Cleanups)
			if falseAck {
				if !errors.Is(err, cleanupapi.ErrConflict) || result.Status.Receipt != nil || result.Status.Phase != cleanupapi.PhaseRunning {
					t.Fatalf("unproved deletion reply=%+v err=%v", result, err)
				}
			} else if err != nil || result.Status.Receipt == nil || result.Status.Phase != cleanupapi.PhaseVerifying {
				t.Fatalf("deletion receipt=%+v err=%v", result, err)
			}
			current, _ := client.Volumes.Get(ctx, testID)
			if current.Phase != volumeapi.PhaseDeleting || result.Status.AbsenceProof != nil {
				t.Fatal("RPC alone released hold")
			}
		})
	}
}
