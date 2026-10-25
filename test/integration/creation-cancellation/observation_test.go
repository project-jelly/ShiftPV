package creationcancellation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/consumer"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Optional live native case: identity is explicit, and the fixture is preserved.
// Only the original-PVC GET response is faulted; ledger reads use the real API.
func testPendingSourceUncertainty(t *testing.T, cfg *rest.Config, api dynamic.Interface) {
	id, uid := os.Getenv("SHIFTPV_TEST_EXISTING_CLAIM_VOLUME"), os.Getenv("SHIFTPV_TEST_EXISTING_CLAIM_VOLUME_UID")
	if id == "" && uid == "" {
		t.Skip("requires an explicitly selected owned native pending fixture")
	}
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	r := &volumeapi.Registry{Client: api}
	before, err := r.Get(ctx, id)
	if err != nil || before.UID != uid || before.Phase != volumeapi.PhaseNodeCreating || before.CreationClaim == nil || before.CurrentCopy == nil || before.CreationReceipt != nil {
		t.Fatalf("owned native pending identity missing: %+v err=%v", before, err)
	}
	claimPath := "/api/v1/namespaces/" + before.CreationClaim.Namespace + "/persistentvolumeclaims/" + before.CreationClaim.Name
	fault := rest.CopyConfig(cfg)
	calls := 0
	fault.WrapTransport = func(base http.RoundTripper) http.RoundTripper {
		return transportFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == claimPath {
				calls++
				body, _ := json.Marshal(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonInternalError, Code: 500, Message: "original claim observation unavailable"})
				return &http.Response{StatusCode: 500, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
			}
			return base.RoundTrip(request)
		})
	}
	client, err := kubernetes.NewForConfig(fault)
	if err != nil {
		t.Fatal(err)
	}
	effects := noEffects{t}
	s := &controllercsi.Service{Client: client, Namespace: "shiftpv-system", Volumes: r, Operator: effects, CleanupOperator: effects, Cleanups: &cleanupapi.Store{Client: api}, CreationClaims: creation.ClaimInspector{Reader: consumer.KubernetesReader{Client: client}}}
	request := &csi.CreateVolumeRequest{Name: before.RequestName, CapacityRange: &csi.CapacityRange{RequiredBytes: before.CapacityBytes}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}, AccessibilityRequirements: &csi.TopologyRequirement{Preferred: []*csi.Topology{{Segments: map[string]string{controllercsi.TopologyKey: before.InitialNode}}}}}
	if _, err := s.CreateVolume(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("actual pending operation lost retry classification after API500: %v", err)
	}
	after, err := r.Get(ctx, id)
	if err != nil || !reflect.DeepEqual(before, after) || calls == 0 {
		t.Fatalf("uncertain lookup changed native authority: %+v calls=%d err=%v", after, calls, err)
	}
	t.Log("PASS real NodeCreating identity/hold preserved and CSI Unavailable returned after original-PVC API500")
}
