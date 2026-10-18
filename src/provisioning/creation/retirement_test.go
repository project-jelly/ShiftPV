package creation

import (
	"context"
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRetirementRequiresOriginalIncarnationEndAndNoPVHandoff(t *testing.T) {
	const id = "shiftpv-0123456789abcdef0123456789abcdef"
	for _, test := range []struct {
		name             string
		claim            *corev1.PersistentVolumeClaim
		pv               *corev1.PersistentVolume
		claimErr, pvErr  error
		unknown          bool
		retired, failure bool
	}{
		{name: "not found", retired: true},
		{name: "same incarnation", claim: originalRetirementClaim()},
		{name: "same incarnation binding changed", claim: func() *corev1.PersistentVolumeClaim {
			p := originalRetirementClaim()
			p.Spec.VolumeName = "pv"
			return p
		}()},
		{name: "same incarnation node changed", claim: func() *corev1.PersistentVolumeClaim {
			p := originalRetirementClaim()
			p.Annotations = map[string]string{"volume.kubernetes.io/selected-node": "different"}
			return p
		}()},
		{name: "replacement", claim: func() *corev1.PersistentVolumeClaim { p := originalRetirementClaim(); p.UID = "replacement"; return p }(), retired: true},
		{name: "terminating", claim: func() *corev1.PersistentVolumeClaim {
			p := originalRetirementClaim()
			now := metav1.Now()
			p.DeletionTimestamp = &now
			return p
		}(), retired: true},
		{name: "terminating bound original", claim: func() *corev1.PersistentVolumeClaim {
			p := originalRetirementClaim()
			now := metav1.Now()
			p.DeletionTimestamp = &now
			p.Spec.VolumeName = "missing-pv"
			return p
		}(), failure: true},
		{name: "unknown provenance", unknown: true, failure: true},
		{name: "server error", claimErr: apierrors.NewInternalError(errors.New("API unavailable")), failure: true},
		{name: "forbidden", claimErr: apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, "data", errors.New("no")), failure: true},
		{name: "PV list uncertain", pvErr: errors.New("unavailable PV observation"), failure: true},
		{name: "PV handoff including Retain", pv: &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "retained"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: volume.DriverName, VolumeHandle: id}}}}, failure: true},
		{name: "contradictory original binding", pv: &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "bound"}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "ns", Name: "data", UID: "original"}}}, failure: true},
		{name: "unrelated PV", pv: &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "other"}}, retired: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var objects []runtime.Object
			if test.claim != nil {
				objects = append(objects, test.claim)
			}
			if test.pv != nil {
				objects = append(objects, test.pv)
			}
			client := fake.NewClientset(objects...)
			if test.claimErr != nil {
				client.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, test.claimErr })
			}
			if test.pvErr != nil {
				client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, test.pvErr })
			}
			state := volumeapi.State{RequestName: "pvc-original", CreationClaim: &volumeapi.ClaimReference{Namespace: "ns", Name: "data", UID: "original"}}
			if test.unknown {
				state.CreationClaim = nil
			}
			inspector := RetirementInspector{Reader: KubernetesRetirementReader{Client: client}}
			retired, err := inspector.Retired(context.Background(), id, state)
			if retired != test.retired || (err != nil) != test.failure {
				t.Fatalf("retired=%v err=%v", retired, err)
			}
		})
	}
}

func originalRetirementClaim() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "data", UID: "original"}}
}
