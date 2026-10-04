// Read-only rsync source resources: credentials, configuration, Pod, and Service.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func (r *Reconciler) sourcePodReady(ctx context.Context, name string) (bool, error) {
	pod, err := r.Client.CoreV1().Pods(r.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("read rsync source Pod: %w", err)
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue, nil
		}
	}
	return false, nil
}

func (r *Reconciler) ensureTransferSecret(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	if move.UID == "" {
		return fmt.Errorf("rsync Secret requires ShiftPVMove UID")
	}
	passwordBytes := make([]byte, 24)
	if _, err := rand.Read(passwordBytes); err != nil {
		return fmt.Errorf("generate rsync password: %w", err)
	}
	password := hex.EncodeToString(passwordBytes)
	expected := &corev1.Secret{
		ObjectMeta: moveObjectMeta(move, names.Secret, r.Namespace, transferLabels(names, move)),
		Data:       map[string][]byte{"password": []byte(password), "secrets": []byte("shiftpv:" + password + "\n")},
	}
	current, err := r.Client.CoreV1().Secrets(r.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, err = r.Client.CoreV1().Secrets(r.Namespace).Get(ctx, names.Secret, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("create rsync Secret: %w", err)
	}
	if !sameMoveObject(&current.ObjectMeta, &expected.ObjectMeta) || !validTransferSecret(current) {
		return fmt.Errorf("rsync Secret %q identity changed", names.Secret)
	}
	return nil
}

func (r *Reconciler) ensureRsyncConfig(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	configuration := fmt.Sprintf(`uid = 0
gid = 0
use chroot = no
read only = yes
strict modes = yes
[data]
path = /pool/volumes/%s
auth users = shiftpv
secrets file = /auth/secrets
`, move.Spec.VolumeID)
	expected := &corev1.ConfigMap{
		ObjectMeta: moveObjectMeta(move, names.Config, r.Namespace, transferLabels(names, move)),
		Data:       map[string]string{"rsyncd.conf": configuration},
	}
	current, err := r.Client.CoreV1().ConfigMaps(r.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, err = r.Client.CoreV1().ConfigMaps(r.Namespace).Get(ctx, names.Config, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("create rsync ConfigMap: %w", err)
	}
	if !sameMoveObject(&current.ObjectMeta, &expected.ObjectMeta) || !reflect.DeepEqual(current.Data, expected.Data) || len(current.BinaryData) != 0 {
		return fmt.Errorf("rsync ConfigMap %q identity changed", names.Config)
	}
	return nil
}

func (r *Reconciler) ensureSourcePod(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	secretMode := int32(0o400)
	configMode := int32(0o644)
	poolRoot, err := r.poolMountPath(ctx, move.Status.SourceCopy)
	if err != nil {
		return err
	}
	expected := &corev1.Pod{
		ObjectMeta: moveObjectMeta(move, names.SourcePod, r.Namespace, sourceLabels(names, move)),
		Spec: corev1.PodSpec{
			NodeName: move.Spec.SourceNode, ServiceAccountName: r.ServiceAccountName,
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{{
				Name: "rsync", Image: r.HelperImage, ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"/shiftpv-volume-helper"}, Args: []string{
					"serve-source", "--move-name=" + move.Name, "--move-uid=" + move.UID,
					"--operation-id=" + move.Status.CopyOperationID, "--namespace=" + r.Namespace,
				},
				Env:                    podNameEnvironment(),
				Ports:                  []corev1.ContainerPort{{Name: "rsync", ContainerPort: 873, Protocol: corev1.ProtocolTCP}},
				ReadinessProbe:         &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstrFromInt(873)}}, InitialDelaySeconds: 1, TimeoutSeconds: 1, PeriodSeconds: 1, SuccessThreshold: 1, FailureThreshold: 3},
				SecurityContext:        &corev1.SecurityContext{AllowPrivilegeEscalation: boolPointer(false), RunAsUser: int64Pointer(0)},
				TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "pool", MountPath: "/pool", ReadOnly: true},
					{Name: "auth", MountPath: "/auth", ReadOnly: true},
					{Name: "config", MountPath: "/config", ReadOnly: true},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "pool", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: poolRoot, Type: hostPathTypePointer(corev1.HostPathDirectory)}}},
				{Name: "auth", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: names.Secret, DefaultMode: &secretMode}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: names.Config}, DefaultMode: &configMode}}},
			},
		},
	}
	current, err := r.Client.CoreV1().Pods(r.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, err = r.Client.CoreV1().Pods(r.Namespace).Get(ctx, names.SourcePod, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("create rsync source Pod: %w", err)
	}
	if !sameMoveObject(&current.ObjectMeta, &expected.ObjectMeta) || !sameSourcePod(current, expected) {
		return fmt.Errorf("rsync source Pod %q identity changed", names.SourcePod)
	}
	return nil
}

func (r *Reconciler) ensureSourceService(ctx context.Context, move volumeapi.Move, names resourceNames) error {
	expected := &corev1.Service{
		ObjectMeta: moveObjectMeta(move, names.SourceService, r.Namespace, transferLabels(names, move)),
		Spec:       corev1.ServiceSpec{Selector: sourceLabels(names, move), Ports: []corev1.ServicePort{{Name: "rsync", Port: 873, TargetPort: intstrFromInt(873)}}},
	}
	current, err := r.Client.CoreV1().Services(r.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, err = r.Client.CoreV1().Services(r.Namespace).Get(ctx, names.SourceService, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("create rsync source Service: %w", err)
	}
	if !sameMoveObject(&current.ObjectMeta, &expected.ObjectMeta) || !reflect.DeepEqual(current.Spec.Selector, expected.Spec.Selector) || !sameServicePorts(current.Spec.Ports, expected.Spec.Ports) {
		return fmt.Errorf("rsync source Service %q identity changed", names.SourceService)
	}
	return nil
}

func sourceLabels(names resourceNames, moves ...volumeapi.Move) map[string]string {
	labels := transferLabels(names, moves...)
	labels["shiftpv.io/role"] = "source"
	return labels
}
