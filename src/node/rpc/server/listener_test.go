package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestListenerRequiresAuthenticatedTLSAndStopsOnCancellation(t *testing.T) {
	cert, public, err := security.NewCertificate("pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	kube := kubefake.NewClientset()
	kube.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{security.Audience}, User: authv1.UserInfo{Username: "system:serviceaccount:shiftpv:controller", UID: "sa-uid"}}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	service := &Service{Identity: volumeapi.NodeExecutor{Namespace: "shiftpv", PodName: "node-a", PodUID: "pod-uid", NodeName: "worker-a"}}
	go func() {
		done <- Serve(ctx, listener, cert, security.Authorizer{Client: kube, Namespace: "shiftpv", ServiceAccount: "controller"}, service)
	}()
	tlsConfig, err := security.ClientTLS("pod-uid", public)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("credential"), 0600); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithPerRPCCredentials(security.FileToken{Path: path}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	_, err = protocol.NewNodeClient(conn).CreateCopy(callCtx, &protocol.EffectRequest{ExecutorUid: "pod-uid", NodeName: "worker-a", VolumeName: "volume", VolumeUid: "volume-uid", OperationId: "create-volume-uid"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("missing executor bypassed listener: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener remained open")
	}
}
