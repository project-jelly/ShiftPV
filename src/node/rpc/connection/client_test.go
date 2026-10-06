package connection_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	"github.com/project-jelly/ShiftPV/src/node/rpc/server"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type executor struct {
	calls atomic.Int32
	fail  error
}

func (e *executor) Execute(context.Context, *protocol.EffectRequest, protocol.Operation) error {
	e.calls.Add(1)
	return e.fail
}

type pools struct{ value volumeapi.Pool }

func (p pools) ReadyPoolForIdentity(_ context.Context, name, uid, node string) (volumeapi.Pool, error) {
	if p.value.Name != name || p.value.UID != uid || p.value.NodeName != node {
		return volumeapi.Pool{}, volumeapi.ErrStateConflict
	}
	return p.value, nil
}

type probe struct {
	calls atomic.Int32
	fail  error
}

func (p *probe) StatFSForPool(context.Context, volumeapi.Pool) (capacity.Filesystem, error) {
	p.calls.Add(1)
	return capacity.Filesystem{TotalBytes: 100, AvailableBytes: 40, AvailableInodes: 20}, p.fail
}

func start(t *testing.T, s *server.Service) (connection.Target, *kubefake.Clientset) {
	t.Helper()
	cert, public, err := security.NewCertificate(s.Identity.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	kube := kubefake.NewClientset()
	kube.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		req := action.(ktesting.CreateAction).GetObject().(*authv1.TokenReview)
		result := authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{security.Audience}, User: authv1.UserInfo{Username: "system:serviceaccount:shiftpv:controller", UID: "sa-uid"}}
		switch req.Spec.Token {
		case "valid", "rotated":
		case "node":
			result.User.Username = "system:serviceaccount:shiftpv:node"
		default:
			result.Authenticated = false
		}
		return true, &authv1.TokenReview{Status: result}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener, cert, security.Authorizer{Client: kube, Namespace: "shiftpv", ServiceAccount: "controller"}, s)
	}()
	t.Cleanup(func() {
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
	return connection.Target{Identity: s.Identity, Address: listener.Addr().String(), Certificate: public}, kube
}
func identity() volumeapi.NodeExecutor {
	return volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-uid", NodeName: "worker-a"}
}
func request() *protocol.EffectRequest {
	return &protocol.EffectRequest{ExecutorUid: "pod-uid", NodeName: "worker-a", VolumeName: "volume-a", VolumeUid: "volume-uid", OperationId: "create-volume-uid"}
}
func token(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func callContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestAuthenticatedTLSCallsRejectWrongCallerAndRecycledTarget(t *testing.T) {
	e := &executor{}
	target, _ := start(t, &server.Service{Identity: identity(), Executor: e})
	client := &connection.Client{TokenFile: token(t, "valid")}
	defer client.Close()
	for _, operation := range []protocol.Operation{protocol.Operation_CREATE, protocol.Operation_RECLAIM} {
		if err := client.Execute(callContext(t), target, operation, request()); err != nil {
			t.Fatal(err)
		}
	}
	if e.calls.Load() != 2 {
		t.Fatal("RPC did not dispatch effects")
	}
	for _, test := range []struct {
		value string
		want  codes.Code
	}{{"node", codes.PermissionDenied}, {"invalid", codes.Unauthenticated}, {"", codes.Unauthenticated}} {
		if err := os.WriteFile(client.TokenFile, []byte(test.value), 0600); err != nil {
			t.Fatal(err)
		}
		err := client.Execute(callContext(t), target, protocol.Operation_CREATE, request())
		// Empty credentials fail client-side before a call reaches the server.
		if test.value != "" && status.Code(err) != test.want {
			t.Fatalf("caller %s: %v", test.value, err)
		}
		if err == nil {
			t.Fatal("unauthorized caller executed")
		}
	}
	if e.calls.Load() != 2 {
		t.Fatal("unauthorized effect executed")
	}
	_ = os.WriteFile(client.TokenFile, []byte("rotated"), 0600)
	stale := request()
	stale.ExecutorUid = "retired-pod"
	if err := client.Execute(callContext(t), target, protocol.Operation_CREATE, stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale Pod=%v", err)
	}
	stale = request()
	stale.NodeName = "worker-b"
	if err := client.Execute(callContext(t), target, protocol.Operation_CREATE, stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong node=%v", err)
	}
	_, otherCert, err := security.NewCertificate(target.Identity.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	impostor := target
	impostor.Certificate = otherCert
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := client.Execute(ctx, impostor, protocol.Operation_CREATE, request()); err == nil {
		t.Fatal("recycled endpoint certificate trusted")
	}
	if e.calls.Load() != 2 {
		t.Fatal("wrong identity reached executor")
	}
	if err := client.Execute(callContext(t), target, protocol.Operation_CREATE, request()); err != nil {
		t.Fatal(err)
	}
	if e.calls.Load() != 3 {
		t.Fatal("rotation/reconnect did not recover")
	}
	replacement := identity()
	replacement.PodUID = "replacement-uid"
	next := &executor{}
	nextTarget, _ := start(t, &server.Service{Identity: replacement, Executor: next})
	nextRequest := request()
	nextRequest.ExecutorUid = replacement.PodUID
	if err := client.Execute(callContext(t), nextTarget, protocol.Operation_CREATE, nextRequest); err != nil {
		t.Fatal(err)
	}
	if next.calls.Load() != 1 {
		t.Fatal("replacement not reached")
	}
	if err := client.Execute(callContext(t), nextTarget, protocol.Operation_OPERATION_UNSPECIFIED, nextRequest); err == nil {
		t.Fatal("invalid operation accepted")
	}
	_ = client.Close()
	if err := client.Execute(callContext(t), nextTarget, protocol.Operation_CREATE, nextRequest); err == nil {
		t.Fatal("closed client reused")
	}
}
func TestCapacityRequiresMatchingEvidenceAndCurrentExecutor(t *testing.T) {
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "worker-a", MountPath: "/mnt/a", Generation: 1}
	evidence, err := capacity.MeasurementEvidence(pool)
	if err != nil {
		t.Fatal(err)
	}
	p := &probe{}
	service := &server.Service{Identity: identity(), Pools: pools{pool}, Probe: p}
	target, _ := start(t, service)
	client := &connection.Client{TokenFile: token(t, "valid")}
	defer client.Close()
	req := &protocol.CapacityRequest{ExecutorUid: "pod-uid", NodeName: "worker-a", PoolName: "pool-a", PoolUid: "pool-uid", Evidence: evidence, Nonce: "0123456789abcdef0123456789abcdef"}
	answer, err := client.GetCapacity(callContext(t), target, req)
	if err != nil || answer.Nonce != req.Nonce || answer.Evidence != evidence || answer.ExecutorUid != req.ExecutorUid || answer.AvailableBytes != 40 {
		t.Fatalf("answer=%v err=%v", answer, err)
	}
	req.PoolUid = "stale-pool"
	if _, err := client.GetCapacity(callContext(t), target, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale pool=%v", err)
	}
	req.PoolUid = pool.UID
	req.Evidence = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := client.GetCapacity(callContext(t), target, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale evidence=%v", err)
	}
	req.Evidence = "bad"
	if _, err := client.GetCapacity(callContext(t), target, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed=%v", err)
	}
	if p.calls.Load() != 1 {
		t.Fatal("invalid authority reached filesystem probe")
	}
	// Direct service tests avoid mutating a live service while calls are active.
	verify := &server.Service{Identity: identity(), Executor: &executor{}, VerifyIdentity: func(context.Context) error { return volumeapi.ErrStateConflict }}
	if _, err := verify.CreateCopy(context.Background(), request()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired server=%v", err)
	}
	cancelled := &server.Service{Identity: identity(), Executor: &executor{fail: context.Canceled}}
	if _, err := cancelled.CreateCopy(context.Background(), request()); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled=%v", err)
	}
	deadline := &server.Service{Identity: identity(), Executor: &executor{fail: context.DeadlineExceeded}}
	if _, err := deadline.ReclaimCopy(context.Background(), request()); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline=%v", err)
	}
	unavailable := &server.Service{Identity: identity(), Executor: &executor{fail: errors.New("API down")}}
	if _, err := unavailable.CreateCopy(context.Background(), request()); status.Code(err) != codes.Unavailable {
		t.Fatalf("failure=%v", err)
	}
}
