package creation

import (
	"context"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type RetirementReader interface {
	ClaimReader
	Volumes(context.Context) ([]corev1.PersistentVolume, error)
}

// RetirementInspector requires a monotonic end of the recorded PVC incarnation
// and excludes a PV handoff. A failed create precondition alone is insufficient.
type RetirementInspector struct{ Reader RetirementReader }

func (i RetirementInspector) Retired(ctx context.Context, id string, state volumeapi.State) (bool, error) {
	claim := state.CreationClaim
	if claim == nil || !claim.Valid() || state.RequestName != "pvc-"+claim.UID {
		return false, ErrClaimIdentity
	}
	if i.Reader == nil {
		return false, fmt.Errorf("creation retirement reader is not configured")
	}
	pvc, err := i.Reader.Claim(ctx, claim.Namespace, claim.Name)
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return false, err
	}
	if !missing {
		if pvc == nil || pvc.Namespace != claim.Namespace || pvc.Name != claim.Name || pvc.UID == "" {
			return false, fmt.Errorf("original claim observation has no exact identity")
		}
		if string(pvc.UID) == claim.UID && pvc.DeletionTimestamp == nil {
			return false, nil
		}
		if string(pvc.UID) == claim.UID && pvc.Spec.VolumeName != "" {
			return false, fmt.Errorf("%w: original claim already records a PV binding", volumeapi.ErrStateConflict)
		}
	}
	pvs, err := i.Reader.Volumes(ctx)
	if err != nil {
		return false, err
	}
	for _, pv := range pvs {
		if pvHandoff(pv, id, *claim) {
			return false, fmt.Errorf("%w: PV %q contradicts unfinished creation retirement", volumeapi.ErrStateConflict, pv.Name)
		}
	}
	return true, nil
}

func pvHandoff(pv corev1.PersistentVolume, id string, claim volumeapi.ClaimReference) bool {
	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == volume.DriverName && pv.Spec.CSI.VolumeHandle == id ||
		pv.Spec.ClaimRef != nil && string(pv.Spec.ClaimRef.UID) == claim.UID && pv.Spec.ClaimRef.Namespace == claim.Namespace && pv.Spec.ClaimRef.Name == claim.Name
}

type KubernetesRetirementReader struct{ Client kubernetes.Interface }

func (r KubernetesRetirementReader) Claim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return r.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (r KubernetesRetirementReader) Volumes(ctx context.Context) ([]corev1.PersistentVolume, error) {
	list, err := r.Client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}
