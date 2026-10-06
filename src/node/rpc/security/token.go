package security

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// FileToken reads on every call so kubelet's projected-token rotation is used.
type FileToken struct{ Path string }

func (t FileToken) RequireTransportSecurity() bool { return true }
func (t FileToken) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(t.Path)
	if err != nil {
		return nil, fmt.Errorf("read Node RPC token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return nil, fmt.Errorf("empty Node RPC token")
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

type Authorizer struct {
	Client                    kubernetes.Interface
	Namespace, ServiceAccount string
}

// Every call checks the bound token with Kubernetes; revocation and Pod
// deletion are not hidden behind a positive authentication cache.
func (a Authorizer) Unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) > 16384 || strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer ")) == "" {
		return nil, status.Error(codes.Unauthenticated, "Node RPC requires a bearer token")
	}
	if a.Client == nil || a.Namespace == "" || a.ServiceAccount == "" {
		return nil, status.Error(codes.Unavailable, "Node RPC authorization is unavailable")
	}
	reviewCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	review, err := a.Client.AuthenticationV1().TokenReviews().Create(reviewCtx, &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: strings.TrimPrefix(values[0], "Bearer "), Audiences: []string{Audience}}}, metav1.CreateOptions{})
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Node RPC token verification is unavailable")
	}
	if !review.Status.Authenticated || review.Status.Error != "" || !slices.Contains(review.Status.Audiences, Audience) {
		return nil, status.Error(codes.Unauthenticated, "Node RPC token rejected")
	}
	if review.Status.User.Username != "system:serviceaccount:"+a.Namespace+":"+a.ServiceAccount || review.Status.User.UID == "" {
		return nil, status.Error(codes.PermissionDenied, "Node RPC caller is not the configured controller")
	}
	return next(ctx, req)
}
