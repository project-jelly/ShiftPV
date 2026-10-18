package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/provisioning/creation"
	"github.com/project-jelly/ShiftPV/src/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func recordedCreationPVC(namespace, node string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "claim", UID: "uid", Annotations: map[string]string{selectedNodeAnnotation: node}}}
}

type claimInspectionFunc func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error)

func (f claimInspectionFunc) Inspect(ctx context.Context, r creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
	return f(ctx, r)
}

func claimCreateRequest() *csi.CreateVolumeRequest {
	r := validCreateRequest("worker-a")
	r.Parameters[PVCNamespaceKey], r.Parameters[PVCNameKey], r.Parameters[PVNameKey] = "app", "claim", r.Name
	return r
}

func TestCreatePersistsClaimBeforeFirstStatusAndRetriesExactOriginal(t *testing.T) {
	for _, failure := range []string{"status rejected", "create response lost", "status response lost"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			api, r, newService, _ := initializationFixture(t)
			s := newService()
			if _, err := s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Client.CoreV1().PersistentVolumeClaims("app").Create(ctx, recordedCreationPVC("app", "worker-a"), metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			req := claimCreateRequest()
			id, _ := volume.IDFromName(req.Name)
			injected := false
			api.PrependReactor("*", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
				if injected {
					return false, nil, nil
				}
				if a.GetVerb() == "create" {
					object := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
					claim, _, _ := unstructured.NestedMap(object.Object, "spec", "creationClaim")
					if claim["uid"] != "uid" || claim["namespace"] != "app" {
						t.Fatal("first API intent omitted verified original claim")
					}
					if failure == "create response lost" {
						object.SetUID("original-volume")
						if err := api.Tracker().Create(volumeapi.VolumeResource, object, ""); err != nil {
							t.Fatal(err)
						}
						injected = true
						return true, nil, apierrors.NewTimeoutError("accepted create response lost", 1)
					}
				}
				if a.GetVerb() == "update" && a.GetSubresource() == "status" {
					object := a.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
					if failure == "status response lost" {
						if err := api.Tracker().Update(volumeapi.VolumeResource, object, ""); err != nil {
							t.Fatal(err)
						}
					}
					injected = true
					return true, nil, apierrors.NewTimeoutError("status boundary", 1)
				}
				return false, nil, nil
			})
			if _, err := s.CreateVolume(ctx, req); status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}
			before, err := r.Get(ctx, id)
			if err != nil || before.CreationClaim == nil || *before.CreationClaim != (volumeapi.ClaimReference{Namespace: "app", Name: "claim", UID: "uid"}) {
				t.Fatalf("first intent=%+v err=%v", before, err)
			}
			if s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("failed intent reached a copy effect")
			}
			restarted := newService()
			restarted.Client = s.Client
			restarted.CreationClaims = s.CreationClaims
			if _, err := restarted.CreateVolume(ctx, req); err != nil {
				t.Fatal(err)
			}
			after, err := r.Get(ctx, id)
			if err != nil || after.UID != before.UID || !reflect.DeepEqual(after.CreationClaim, before.CreationClaim) || after.Phase != volumeapi.PhaseReady {
				t.Fatalf("retry changed original: %+v err=%v", after, err)
			}
			// A Ready response has already crossed the creation boundary. It must
			// remain replayable after ordinary claim retirement/Retain semantics.
			if err := s.Client.CoreV1().PersistentVolumeClaims("app").Delete(ctx, "claim", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.CreateVolume(ctx, req); err != nil {
				t.Fatalf("Ready replay requires a surviving original claim: %v", err)
			}
		})
	}
}

func TestCreateClaimInspectionFailureHasNoIntentOrEffect(t *testing.T) {
	for _, test := range []struct {
		name    string
		inspect claimInspectionFunc
		code    codes.Code
	}{
		{"unconfigured", nil, codes.Unavailable},
		{"changed UID", func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
			return nil, creation.ErrClaimIdentity
		}, codes.FailedPrecondition},
		{"unknown API", func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
			return nil, apierrors.NewTimeoutError("unknown", 1)
		}, codes.Unavailable},
		{"empty proof", func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) { return nil, nil }, codes.Unavailable},
		{"wrong reference", func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
			return &volumeapi.ClaimReference{Namespace: "foreign", Name: "claim", UID: "uid"}, nil
		}, codes.FailedPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, r, newService, _ := initializationFixture(t)
			s := newService()
			s.CreationClaims = nil
			if test.inspect != nil {
				s.CreationClaims = test.inspect
			}
			if _, err := s.CreateVolume(context.Background(), claimCreateRequest()); status.Code(err) != test.code {
				t.Fatal(err)
			}
			states, err := r.ListVolumes(context.Background())
			if err != nil || len(states) != 0 || s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
				t.Fatal("unproven identity reached intent/effect")
			}
		})
	}
}

func TestCreateRetryCannotUseSameNameReplacementOrOmitRecordedProof(t *testing.T) {
	ctx := context.Background()
	api, r, newService, _ := initializationFixture(t)
	s := newService()
	_, _ = s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{})
	_, _ = s.Client.CoreV1().PersistentVolumeClaims("app").Create(ctx, recordedCreationPVC("app", "worker-a"), metav1.CreateOptions{})
	api.PrependReactor("update", "shiftpvvolumes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewTimeoutError("before creation approval", 1)
	})
	req := claimCreateRequest()
	if _, err := s.CreateVolume(ctx, req); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	id, _ := volume.IDFromName(req.Name)
	before, err := r.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Client.CoreV1().PersistentVolumeClaims("app").Delete(ctx, "claim", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := recordedCreationPVC("app", "worker-a")
	replacement.UID = "replacement-uid"
	if _, err := s.Client.CoreV1().PersistentVolumeClaims("app").Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Retry metadata cannot bypass the original reference persisted at CREATE.
	req.Parameters = map[string]string{}
	if _, err := s.CreateVolume(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	after, err := r.Get(ctx, id)
	if err != nil || !reflect.DeepEqual(before, after) || s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
		t.Fatal("replacement/metadata omission changed original intent")
	}
	if _, err := r.BeginCreate(ctx, volumeapi.CreationRequest{VolumeID: id, RequestName: req.Name, NodeName: "worker-a", CapacityBytes: before.CapacityBytes, Claim: &volumeapi.ClaimReference{Namespace: "foreign", Name: "claim", UID: "uid"}}); !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("registry adopted another source: %v", err)
	}
}

func TestPrunedClaimCannotAuthorizeCreation(t *testing.T) {
	ctx := context.Background()
	api, r, newService, _ := initializationFixture(t)
	s := newService()
	_, _ = s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{})
	_, _ = s.Client.CoreV1().PersistentVolumeClaims("app").Create(ctx, recordedCreationPVC("app", "worker-a"), metav1.CreateOptions{})
	api.PrependReactor("create", "shiftpvvolumes", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		unstructured.RemoveNestedField(object.Object, "spec", "creationClaim")
		return false, nil, nil
	})
	req := claimCreateRequest()
	if _, err := s.CreateVolume(ctx, req); err == nil {
		t.Fatal("pruned claim allowed creation")
	}
	id, _ := volume.IDFromName(req.Name)
	state, err := r.Get(ctx, id)
	if err != nil || !state.UnstartedCreation() || state.CreationClaim != nil || s.Operator.(*fakeDirectoryOperator).createCalls != 0 {
		t.Fatalf("pruned intent approved effects: %+v err=%v", state, err)
	}
	assertInitialReservation(t, r, "pool-a-uid", req.CapacityRange.RequiredBytes)
}

func TestLegacyCreationDoesNotAdoptNewProvenance(t *testing.T) {
	ctx := context.Background()
	_, r, newService, _ := initializationFixture(t)
	s := newService()
	req := claimCreateRequest()
	id, _ := volume.IDFromName(req.Name)
	before, err := r.BeginCreate(ctx, volumeapi.CreationRequest{VolumeID: id, RequestName: req.Name, NodeName: "worker-a", CapacityBytes: req.CapacityRange.RequiredBytes, Pool: &volumeapi.PoolReference{Name: "pool-a", UID: "pool-a-uid"}})
	if err != nil {
		t.Fatal(err)
	}
	s.CreationClaims = claimInspectionFunc(func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
		t.Fatal("legacy retry attempted provenance backfill")
		return nil, nil
	})
	_, _ = s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{})
	if _, err := s.CreateVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	after, err := r.Get(ctx, id)
	if err != nil || after.UID != before.UID || after.CreationClaim != nil {
		t.Fatalf("legacy provenance changed: %+v err=%v", after, err)
	}
}

func TestClaimProvenanceWithoutPoolCapacityAdmission(t *testing.T) {
	ctx := context.Background()
	_, r, newService, _ := initializationFixture(t)
	s := newService()
	s.CapacityPools, s.CapacityProbe = nil, nil
	_, _ = s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{})
	_, _ = s.Client.CoreV1().PersistentVolumeClaims("app").Create(ctx, recordedCreationPVC("app", "worker-a"), metav1.CreateOptions{})
	req := claimCreateRequest()
	if _, err := s.CreateVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	id, _ := volume.IDFromName(req.Name)
	state, err := r.Get(ctx, id)
	if err != nil || state.CreationClaim == nil || state.CreationClaim.UID != "uid" {
		t.Fatalf("non-capacity path lost provenance: %+v err=%v", state, err)
	}
}

func TestOptionalMetadataOrOpaqueRequestKeepsUnknownProvenance(t *testing.T) {
	for _, mode := range []string{"no metadata", "partial metadata", "opaque request"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			_, r, newService, _ := initializationFixture(t)
			s := newService()
			req := validCreateRequest("worker-a")
			_, _ = s.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, metav1.CreateOptions{})
			s.CreationClaims = claimInspectionFunc(func(context.Context, creation.ClaimRequest) (*volumeapi.ClaimReference, error) {
				t.Fatal("unknown source attempted to acquire claim provenance")
				return nil, nil
			})
			switch mode {
			case "partial metadata":
				req.Parameters[PVCNamespaceKey] = "app"
			case "opaque request":
				req.Name = "standalone-request"
				req.Parameters[PVCNamespaceKey], req.Parameters[PVCNameKey] = "app", "claim"
			}
			if _, err := s.CreateVolume(ctx, req); err != nil {
				t.Fatal(err)
			}
			id, _ := volume.IDFromName(req.Name)
			state, err := r.Get(ctx, id)
			if err != nil || state.CreationClaim != nil {
				t.Fatalf("unknown source acquired provenance: %+v %v", state, err)
			}
		})
	}
}
