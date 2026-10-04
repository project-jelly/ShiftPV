// Copy and promotion Jobs, including the shared node-bound operation template.
package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func (r *Reconciler) ensureCopyJob(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	if move.Status.IncomingCopy == nil || move.Status.CopyOperationID == "" {
		return fmt.Errorf("copy identity is missing")
	}
	command := []string{"/shiftpv-volume-helper"}
	args := []string{
		"copy", "--move-name=" + move.Name, "--move-uid=" + move.UID,
		"--operation-id=" + move.Status.CopyOperationID, "--namespace=" + r.Namespace,
		"--source-service=" + names.SourceService, "--password-file=/auth/password",
	}
	secretMode := int32(0o644)
	return r.ensureJob(ctx, move, names.CopyJob, move.Status.DestinationNode, names, command, args, nil,
		[]corev1.VolumeMount{{Name: "auth", MountPath: "/auth", ReadOnly: true}},
		[]corev1.Volume{{Name: "auth", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: names.Secret, DefaultMode: &secretMode}}}})
}

func (r *Reconciler) ensurePromotionJob(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	if move.Status.IncomingCopy == nil || move.Status.DestinationCopy == nil || move.Status.PromotionOperationID == "" {
		return fmt.Errorf("promotion identity is missing")
	}
	return r.ensureJob(ctx, move, names.PromotionJob, move.Status.DestinationNode, names,
		[]string{"/shiftpv-volume-helper"}, []string{
			"promote", "--move-name=" + move.Name, "--move-uid=" + move.UID,
			"--operation-id=" + move.Status.PromotionOperationID, "--namespace=" + r.Namespace,
		}, nil, nil, nil)
}

func (r *Reconciler) ensureJob(ctx context.Context, move volumeapi.Move, name, nodeName string, names resourceNames, command, args []string, env []corev1.EnvVar, extraMounts []corev1.VolumeMount, extraVolumes []corev1.Volume) error {
	env = append(append([]corev1.EnvVar{}, env...), podNameEnvironment()...)
	if move.Status.IncomingCopy == nil || move.Status.IncomingCopy.NodeName != nodeName {
		return fmt.Errorf("operation Pool copy identity is missing")
	}
	job, err := r.operationJob(ctx, name, move.Status.IncomingCopy, names, env, extraMounts, extraVolumes)
	if err != nil {
		return err
	}
	job.OwnerReferences = []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name, UID: types.UID(move.UID), Controller: boolPointer(true), BlockOwnerDeletion: boolPointer(true)}}
	job.Labels["shiftpv.io/move-uid"] = move.UID
	job.Spec.Template.Labels["shiftpv.io/move-uid"] = move.UID
	job.Spec.Template.Spec.Containers[0].Command = command
	job.Spec.Template.Spec.Containers[0].Args = args
	_, err = r.Client.BatchV1().Jobs(r.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, getErr := r.Client.BatchV1().Jobs(r.Namespace).Get(ctx, name, metav1.GetOptions{})
		err = getErr
		if err == nil && !sameOperationJob(created, job) {
			return fmt.Errorf("mobility Job %q identity changed", name)
		}
	}
	if err != nil {
		return fmt.Errorf("create mobility Job %q: %w", name, err)
	}
	return nil
}

func (r *Reconciler) operationJob(ctx context.Context, name string, copy *volume.CopyIdentity, names resourceNames, env []corev1.EnvVar, extraMounts []corev1.VolumeMount, extraVolumes []corev1.Volume) (*batchv1.Job, error) {
	backoff := int32(2)
	ttl := int32(600)
	deadline := int64(300)
	poolRoot, err := r.poolMountPath(ctx, copy)
	if err != nil {
		return nil, err
	}
	mounts := append([]corev1.VolumeMount{{Name: "pool", MountPath: "/pool"}}, extraMounts...)
	volumes := append([]corev1.Volume{{Name: "pool", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: poolRoot, Type: hostPathTypePointer(corev1.HostPathDirectory)}}}}, extraVolumes...)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: transferLabels(names)},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, TTLSecondsAfterFinished: &ttl, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: transferLabels(names)},
				Spec: corev1.PodSpec{NodeName: copy.NodeName, ServiceAccountName: r.ServiceAccountName, RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{Name: "operation", Image: r.HelperImage, Env: env, VolumeMounts: mounts,
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolPointer(false), RunAsUser: int64Pointer(0)}}},
					Volumes: volumes,
				},
			},
		},
	}, nil
}
