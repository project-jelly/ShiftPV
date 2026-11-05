package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeClient struct {
	csi.ControllerClient
	response error
	request  *csi.CreateVolumeRequest
}

func TestReplayRejectsUnavailableUnixSocket(t *testing.T) {
	conn, err := grpc.NewClient("unix://"+filepath.Join(t.TempDir(), "missing.sock"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	uid := "4ec5a5a1-b4a4-4c37-b6ba-92d7cb6289be"
	digest := sha256.Sum256([]byte("pvc-" + uid))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := replay(ctx, csi.NewControllerClient(conn), uid, fmt.Sprintf("shiftpv-%x", digest[:16])); err == nil {
		t.Fatal("missing CSI socket was accepted as a claim identity denial")
	}
}

func (c *fakeClient) CreateVolume(_ context.Context, request *csi.CreateVolumeRequest, _ ...grpc.CallOption) (*csi.CreateVolumeResponse, error) {
	c.request = request
	return nil, c.response
}

func TestReplayRequiresExactIdentityDenial(t *testing.T) {
	uid := "4ec5a5a1-b4a4-4c37-b6ba-92d7cb6289be"
	digest := sha256.Sum256([]byte("pvc-" + uid))
	id := fmt.Sprintf("shiftpv-%x", digest[:16])
	for _, test := range []struct {
		name string
		err  error
		ok   bool
	}{
		{"expected", status.Error(codes.FailedPrecondition, identityError), true},
		{"success", nil, false},
		{"transport", status.Error(codes.Unavailable, "connection refused"), false},
		{"timeout", context.DeadlineExceeded, false},
		{"wrong denial", status.Error(codes.FailedPrecondition, "unrelated fence"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{response: test.err}
			err := replay(context.Background(), client, uid, id)
			if (err == nil) != test.ok {
				t.Fatalf("error=%v, expected success=%v", err, test.ok)
			}
			request := client.request
			if request == nil || request.Name != "pvc-"+uid || request.Parameters["csi.storage.k8s.io/pvc/namespace"] != testNamespace ||
				request.Parameters["csi.storage.k8s.io/pvc/name"] != "partial" || request.Parameters["csi.storage.k8s.io/pv/name"] != request.Name ||
				request.GetCapacityRange().GetRequiredBytes() != 8<<20 || len(request.GetAccessibilityRequirements().GetPreferred()) != 1 ||
				request.GetAccessibilityRequirements().GetPreferred()[0].GetSegments()["topology.csi.shiftpv.io/node"] != testNode {
				t.Fatalf("unexpected reconstructed request: %v", request)
			}
		})
	}
	client := &fakeClient{}
	if err := replay(context.Background(), client, uid, "shiftpv-wrong"); err == nil || client.request != nil {
		t.Fatal("mismatched Volume ID reached CSI")
	}
	if err := replay(context.Background(), client, strings.Repeat("a", 36), id); err == nil || client.request != nil {
		t.Fatal("invalid PVC UID reached CSI")
	}
}
