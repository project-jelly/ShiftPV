// Command replay sends one reconstructed stale request to the isolated kind CSI socket.
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	testNamespace = "shiftpv-upgrade-creation"
	testNode      = "shiftpv-upgrade-e2e-creation-recovery-worker"
	identityError = "original creation claim identity changed"
)

var claimUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func replay(ctx context.Context, client csi.ControllerClient, uid, volumeID string) error {
	if !claimUIDPattern.MatchString(uid) {
		return fmt.Errorf("invalid test PVC UID")
	}
	name := "pvc-" + uid
	digest := sha256.Sum256([]byte(name))
	if volumeID != fmt.Sprintf("shiftpv-%x", digest[:16]) {
		return fmt.Errorf("test Volume ID does not match the PVC request")
	}
	request := &csi.CreateVolumeRequest{
		Name:          name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 << 20},
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/name": "partial", "csi.storage.k8s.io/pvc/namespace": testNamespace,
			"csi.storage.k8s.io/pv/name": name, "shiftpv.io/capacity-enforcement": "none",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		AccessibilityRequirements: &csi.TopologyRequirement{Preferred: []*csi.Topology{{
			Segments: map[string]string{"topology.csi.shiftpv.io/node": testNode},
		}}},
	}
	_, err := client.CreateVolume(ctx, request)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), identityError) {
		return fmt.Errorf("stale request did not receive the expected claim identity denial: %v", err)
	}
	fmt.Println("stale CreateVolume rejected: original creation claim identity changed")
	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "expected test PVC UID and Volume ID")
		os.Exit(1)
	}
	conn, err := grpc.NewClient("unix:///run/csi/csi.sock", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := replay(ctx, csi.NewControllerClient(conn), os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
