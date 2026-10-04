// Transfer resource naming, shared metadata, and acquisition/release ordering.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type resourceNames struct {
	Base          string
	PlacementPod  string
	Secret        string
	Config        string
	SourcePod     string
	SourceService string
	CopyJob       string
	PromotionJob  string
}

func namesFor(moveName string) resourceNames {
	sum := sha256.Sum256([]byte(moveName))
	base := "shiftpv-move-" + hex.EncodeToString(sum[:6])
	return resourceNames{
		Base: base, PlacementPod: base + "-placement", Secret: base + "-auth", Config: base + "-config", SourcePod: base + "-source",
		SourceService: base + "-source", CopyJob: base + "-copy", PromotionJob: base + "-promote",
	}
}

func (r *Reconciler) ensureCopyResources(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	if err := r.ensureTransferSecret(ctx, move, names); err != nil {
		return err
	}
	if err := r.ensureRsyncConfig(ctx, move, names); err != nil {
		return err
	}
	if err := r.ensureSourcePod(ctx, move, names); err != nil {
		return err
	}
	if err := r.ensureSourceService(ctx, move, names); err != nil {
		return err
	}
	ready, err := r.sourcePodReady(ctx, names.SourcePod)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}
	return r.ensureCopyJob(ctx, move, names)
}

func podNameEnvironment() []corev1.EnvVar {
	return []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}}}
}

func (r *Reconciler) poolMountPath(ctx context.Context, copy *volume.CopyIdentity) (string, error) {
	if copy == nil || copy.Validate() != nil {
		return "", fmt.Errorf("Pool copy identity is missing")
	}
	pool, err := r.Repository.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return "", err
	}
	result := filepath.Clean(pool.MountPath)
	if !filepath.IsAbs(result) || result == "/" {
		return "", fmt.Errorf("Pool %q has no valid mountPath", pool.Name)
	}
	return result, nil
}

func (r *Reconciler) deleteTransferResources(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	pods := r.Client.CoreV1().Pods(r.Namespace)
	services := r.Client.CoreV1().Services(r.Namespace)
	configMaps := r.Client.CoreV1().ConfigMaps(r.Namespace)
	secrets := r.Client.CoreV1().Secrets(r.Namespace)
	owned := []ownedTransferObject{
		{kind: "Pod", missingUIDKind: "source Pod", name: names.SourcePod,
			get: func() (metav1.Object, error) { return pods.Get(ctx, names.SourcePod, metav1.GetOptions{}) },
			del: func(options metav1.DeleteOptions) error { return pods.Delete(ctx, names.SourcePod, options) }},
		{kind: "Service", missingUIDKind: "source Service", name: names.SourceService,
			get: func() (metav1.Object, error) { return services.Get(ctx, names.SourceService, metav1.GetOptions{}) },
			del: func(options metav1.DeleteOptions) error { return services.Delete(ctx, names.SourceService, options) }},
		{kind: "ConfigMap", missingUIDKind: "rsync ConfigMap", name: names.Config,
			get: func() (metav1.Object, error) { return configMaps.Get(ctx, names.Config, metav1.GetOptions{}) },
			del: func(options metav1.DeleteOptions) error { return configMaps.Delete(ctx, names.Config, options) }},
		{kind: "Secret", missingUIDKind: "rsync Secret", name: names.Secret,
			get: func() (metav1.Object, error) { return secrets.Get(ctx, names.Secret, metav1.GetOptions{}) },
			del: func(options metav1.DeleteOptions) error { return secrets.Delete(ctx, names.Secret, options) }},
	}
	var errs []error
	for _, target := range owned {
		errs = append(errs, target.delete(move.UID)...)
	}
	return errors.Join(errs...)
}

// ownedTransferObject deletes one Move-owned transfer object under its exact UID,
// refusing anything that is not labelled and owned by the current ShiftPVMove.
type ownedTransferObject struct {
	kind, missingUIDKind, name string
	get                        func() (metav1.Object, error)
	del                        func(metav1.DeleteOptions) error
}

func (o ownedTransferObject) delete(moveUID string) []error {
	object, err := o.get()
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return []error{err}
	}
	if !moveOwned(object.GetOwnerReferences(), moveUID) || object.GetLabels()["shiftpv.io/move-uid"] != moveUID {
		return []error{fmt.Errorf("refusing to delete unrelated %s %q", o.kind, o.name)}
	}
	uid := object.GetUID()
	if uid == "" {
		return []error{fmt.Errorf("%s %q has no UID", o.missingUIDKind, o.name)}
	}
	if err := o.del(metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return []error{err}
	}
	return nil
}

func transferLabels(names resourceNames, moves ...volumeapi.Move) map[string]string {
	labels := map[string]string{"app.kubernetes.io/name": "shiftpv", "app.kubernetes.io/component": "mobility", "shiftpv.io/move": names.Base}
	if len(moves) == 1 && moves[0].UID != "" {
		labels["shiftpv.io/move-uid"] = moves[0].UID
	}
	return labels
}

func moveObjectMeta(move volumeapi.Move, name, namespace string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: name, Namespace: namespace, Labels: labels,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name, UID: types.UID(move.UID),
			Controller: boolPointer(true), BlockOwnerDeletion: boolPointer(true),
		}},
	}
}
