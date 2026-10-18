package creation

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type claimReaderFunc func(context.Context, string, string) (*corev1.PersistentVolumeClaim, error)

func (f claimReaderFunc) Claim(ctx context.Context, ns, name string) (*corev1.PersistentVolumeClaim, error) {
	return f(ctx, ns, name)
}

func TestClaimInspectorVerifiesOriginalBeforeApproval(t *testing.T) {
	for _, change := range []string{"valid", "missing", "new UID", "deleting", "bound", "node changed", "namespace changed", "name changed", "unknown API", "empty observation", "missing reader", "invalid metadata", "opaque name"} {
		t.Run(change, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: "claim-uid", Annotations: map[string]string{"volume.kubernetes.io/selected-node": "worker-a"}}}
			request := ClaimRequest{Namespace: "app", Name: "data", RequestName: "pvc-claim-uid", NodeName: "worker-a"}
			var readErr error
			switch change {
			case "missing":
				readErr = apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, "data")
			case "new UID":
				pvc.UID = "new-claim"
			case "deleting":
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
			case "bound":
				pvc.Spec.VolumeName = "already-created-pv"
			case "node changed":
				pvc.Annotations["volume.kubernetes.io/selected-node"] = "worker-b"
			case "namespace changed":
				pvc.Namespace = "other"
			case "name changed":
				pvc.Name = "other"
			case "unknown API":
				readErr = apierrors.NewTimeoutError("unknown claim", 1)
			case "empty observation":
				pvc = nil
			case "invalid metadata":
				request.Namespace = ""
			case "opaque name":
				request.RequestName = "standalone-request"
			}
			inspector := ClaimInspector{Reader: claimReaderFunc(func(_ context.Context, ns, name string) (*corev1.PersistentVolumeClaim, error) {
				if ns != "app" || name != "data" {
					t.Fatalf("wrong claim lookup %s/%s", ns, name)
				}
				return pvc, readErr
			})}
			if change == "missing reader" {
				inspector.Reader = nil
			}
			claim, err := inspector.Inspect(context.Background(), request)
			if change == "valid" {
				if err != nil || claim == nil || claim.UID != "claim-uid" {
					t.Fatalf("claim=%+v err=%v", claim, err)
				}
				return
			}
			if err == nil || claim != nil {
				t.Fatalf("unproved claim granted provenance: claim=%+v err=%v", claim, err)
			}
			if change == "unknown API" && (!apierrors.IsTimeout(err) || errors.Is(err, ErrClaimIdentity)) {
				t.Fatal("API uncertainty became identity/absence proof")
			}
		})
	}
}
