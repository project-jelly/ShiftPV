package security

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestTokenReviewFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name, header string
		change       func(*authv1.TokenReviewStatus)
		failure      error
		want         codes.Code
	}{
		{name: "controller", header: "Bearer credential", want: codes.OK},
		{name: "missing", want: codes.Unauthenticated},
		{name: "empty", header: "Bearer ", want: codes.Unauthenticated},
		{name: "rejected", header: "Bearer credential", change: func(s *authv1.TokenReviewStatus) { s.Authenticated = false }, want: codes.Unauthenticated},
		{name: "wrong audience", header: "Bearer credential", change: func(s *authv1.TokenReviewStatus) { s.Audiences = []string{"kubernetes"} }, want: codes.Unauthenticated},
		{name: "wrong account", header: "Bearer credential", change: func(s *authv1.TokenReviewStatus) { s.User.Username = "system:serviceaccount:shiftpv:node" }, want: codes.PermissionDenied},
		{name: "wrong namespace", header: "Bearer credential", change: func(s *authv1.TokenReviewStatus) { s.User.Username = "system:serviceaccount:other:controller" }, want: codes.PermissionDenied},
		{name: "missing uid", header: "Bearer credential", change: func(s *authv1.TokenReviewStatus) { s.User.UID = "" }, want: codes.PermissionDenied},
		{name: "API unavailable", header: "Bearer credential", failure: errors.New("offline"), want: codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			kube := kubefake.NewClientset()
			kube.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
				req := action.(ktesting.CreateAction).GetObject().(*authv1.TokenReview)
				if req.Spec.Token != "credential" || len(req.Spec.Audiences) != 1 || req.Spec.Audiences[0] != Audience {
					t.Fatal("incorrect TokenReview request")
				}
				answer := authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{Audience}, User: authv1.UserInfo{Username: "system:serviceaccount:shiftpv:controller", UID: "sa-uid"}}
				if test.change != nil {
					test.change(&answer)
				}
				return true, &authv1.TokenReview{Status: answer}, test.failure
			})
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", test.header))
			called := false
			_, err := (Authorizer{Client: kube, Namespace: "shiftpv", ServiceAccount: "controller"}).Unary(ctx, nil, nil, func(context.Context, any) (any, error) { called = true; return nil, nil })
			if status.Code(err) != test.want || called != (test.want == codes.OK) {
				t.Fatalf("code=%v called=%v", err, called)
			}
		})
	}
}
func TestProjectedTokenRotationAndTLSIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	credential := FileToken{Path: path}
	if !credential.RequireTransportSecurity() {
		t.Fatal("token must require TLS")
	}
	for _, value := range []string{"first", "rotated"} {
		if err := os.WriteFile(path, []byte(value+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		md, err := credential.GetRequestMetadata(context.Background())
		if err != nil || md["authorization"] != "Bearer "+value {
			t.Fatalf("metadata=%v error=%v", md, err)
		}
	}
	if _, _, err := NewCertificate(""); err == nil {
		t.Fatal("missing identity accepted")
	}
	_, public, err := NewCertificate("pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ClientTLS("pod-uid", public); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "invalid", public + public} {
		if _, err := ClientTLS("pod-uid", value); err == nil {
			t.Fatal("invalid certificate accepted")
		}
	}
	if _, err := ClientTLS("replacement-uid", public); err == nil {
		t.Fatal("replaced Pod identity trusted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := credential.GetRequestMetadata(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}
