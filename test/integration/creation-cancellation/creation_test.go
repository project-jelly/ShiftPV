package creationcancellation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/internal/testutil/kubeapi"
	controllercsi "github.com/project-jelly/ShiftPV/src/csi/controller"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type noEffects struct{ t *testing.T }

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (n noEffects) CreateCopy(context.Context, volume.CopyIdentity) error {
	n.t.Fatal("unstarted cancellation attempted creation")
	return nil
}
func (n noEffects) FinalizeCreate(context.Context, volume.CopyIdentity) error {
	n.t.Fatal("unstarted cancellation attempted creation completion")
	return nil
}
func (n noEffects) Reclaim(context.Context, cleanupapi.Cleanup, cleanupapi.Journal) (cleanupapi.Cleanup, error) {
	n.t.Fatal("unstarted cancellation invented a disk cleanup")
	return cleanupapi.Cleanup{}, nil
}

// Run only against an explicitly selected disposable cluster. No default
// kubeconfig/context is used by the normal unit-test suite.
func TestAPIServerUnstartedCancellation(t *testing.T) {
	if os.Getenv("SHIFTPV_API_CANCELLATION_TEST") != "1" {
		t.Skip("requires an isolated API-server fixture")
	}
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer setupCancel()
	cfg, err := kubeapi.Config(setupCtx, os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	if namespace := os.Getenv("SHIFTPV_TEST_AS_CONTROLLER_NAMESPACE"); namespace != "" {
		cfg.Impersonate.UserName = "system:serviceaccount:" + namespace + ":shiftpv-controller"
	}
	cfg.QPS, cfg.Burst = 50, 100
	api, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	registry := &volumeapi.Registry{Client: api}
	for _, mode := range []string{"ordinary", "foreign protection and late initialization", "stale revision"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			effects := noEffects{t}
			s := &controllercsi.Service{Client: client, Namespace: "shiftpv-system", Volumes: registry,
				Operator: effects, CleanupOperator: effects, Cleanups: &cleanupapi.Store{Client: api}}
			random := make([]byte, 16)
			if _, err := rand.Read(random); err != nil {
				t.Fatal(err)
			}
			id := "shiftpv-" + hex.EncodeToString(random)
			finalizers := []any{volumeapi.VolumeProtectionFinalizer}
			if mode == "foreign protection and late initialization" {
				finalizers = append(finalizers, "example.test/cancellation-fixture")
			}
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume",
				"metadata": map[string]any{"name": id, "finalizers": finalizers},
				"spec": map[string]any{"volumeID": id, "requestName": "api-cancellation-fixture", "initialNode": "unassigned-test-node", "capacityBytes": int64(64),
					"initialPool": map[string]any{"name": "unassigned-test-pool", "uid": "unassigned-test-pool-uid"}},
			}}
			created, err := api.Resource(volumeapi.VolumeResource).Create(ctx, object, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "stale revision" {
				var changed atomic.Bool
				faultConfig := rest.CopyConfig(cfg)
				faultConfig.WrapTransport = func(base http.RoundTripper) http.RoundTripper {
					return transportFunc(func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodDelete && changed.CompareAndSwap(false, true) {
							current, err := api.Resource(volumeapi.VolumeResource).Get(req.Context(), id, metav1.GetOptions{})
							if err != nil {
								return nil, err
							}
							current.SetAnnotations(map[string]string{"example.test/concurrent-writer": "won"})
							if _, err := api.Resource(volumeapi.VolumeResource).Update(req.Context(), current, metav1.UpdateOptions{}); err != nil {
								return nil, err
							}
						}
						return base.RoundTrip(req)
					})
				}
				faultClient, err := dynamic.NewForConfig(faultConfig)
				if err != nil {
					t.Fatal(err)
				}
				s.Volumes = &volumeapi.Registry{Client: faultClient}
			}
			request := &csi.DeleteVolumeRequest{VolumeId: id}
			_, err = s.DeleteVolume(ctx, request)
			if mode == "stale revision" {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("stale RV DELETE was not rejected: %v", err)
				}
				current, readErr := registry.Get(ctx, id)
				if readErr != nil || current.Deleting || !current.HasInitialReservation() {
					t.Fatalf("stale deletion changed reservation: %+v err=%v", current, readErr)
				}
				_, err = s.DeleteVolume(ctx, request)
			}
			if mode == "foreign protection and late initialization" {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("foreign protection=%v", err)
				}
				held, readErr := registry.Get(ctx, id)
				if readErr != nil || held.UID != string(created.GetUID()) || !held.Deleting || !held.HasInitialReservation() || len(held.Finalizers) != 2 {
					t.Fatalf("terminating reservation=%+v err=%v", held, readErr)
				}
				if _, err := registry.BeginCreate(ctx, volumeapi.CreationRequest{VolumeID: id, RequestName: "api-cancellation-fixture", NodeName: "unassigned-test-node", CapacityBytes: 64, Pool: &volumeapi.PoolReference{Name: "unassigned-test-pool", UID: "unassigned-test-pool-uid"}}); err == nil {
					t.Fatal("deletion timestamp allowed late initialization")
				}
				current, readErr := api.Resource(volumeapi.VolumeResource).Get(ctx, id, metav1.GetOptions{})
				if readErr != nil {
					t.Fatal(readErr)
				}
				// The fixture releases only its own foreign protection. The real
				// cancellation code must remove ShiftPV protection and the object.
				current.SetFinalizers([]string{volumeapi.VolumeProtectionFinalizer})
				if _, err := api.Resource(volumeapi.VolumeResource).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				_, err = s.DeleteVolume(ctx, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.Get(ctx, id); !apierrors.IsNotFound(err) {
				t.Fatalf("real API object not removed: %v", err)
			}
			if _, err := s.DeleteVolume(ctx, request); err != nil {
				t.Fatalf("absent replay=%v", err)
			}
		})
	}
	t.Run("claim provenance", func(t *testing.T) { testClaimProvenance(t, cfg, api, client) })
	t.Run("pending source uncertainty", func(t *testing.T) { testPendingSourceUncertainty(t, cfg, api) })
}
