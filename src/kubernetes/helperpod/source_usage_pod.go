package helperpod

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const sourceUsageSnapshotEnv = "SHIFTPV_SOURCE_USAGE_SNAPSHOT"

type sourceUsageSnapshot struct {
	InitialGeneration     int64  `json:"initialGeneration"`
	ConfigurationEvidence string `json:"configurationEvidence"`
}

func sourceUsagePodName(moveUID string) string {
	sum := sha256.Sum256([]byte(moveUID))
	return fmt.Sprintf("shiftpv-usage-%x", sum[:16])
}

func validSourceUsageMove(move volumeapi.Move) bool {
	_, err := volume.Path(mountPath, move.Spec.VolumeID)
	return err == nil && volume.ValidObjectName(move.Name) && volume.ValidIdentityToken(move.UID) && volume.ValidObjectName(move.Spec.SourceNode)
}

func (r *Runner) sourceUsagePod(move volumeapi.Move, copy volume.CopyIdentity, pool volumeapi.Pool) (*corev1.Pod, error) {
	if pool.Generation <= 0 {
		return nil, fmt.Errorf("source usage requires a positive Pool generation")
	}
	evidence, err := poolcapacity.UsageConfigurationEvidence(pool)
	if err != nil {
		return nil, err
	}
	command, err := r.measurementCommand(pool, "usage", copy.VolumeID, &copy)
	if err != nil {
		return nil, err
	}
	pod := r.helperPod(copy.NodeName, copy.VolumeID, pool.MountPath, command)
	pod.Name, pod.Namespace, pod.GenerateName = sourceUsagePodName(move.UID), r.Namespace, ""
	pod.Labels[SourceUsagePurposeLabel] = "true"
	pod.OwnerReferences = sourceUsageOwner(move)
	pod.Spec.AutomountServiceAccountToken = boolPtr(true)
	deadline := int64(r.Timeout / time.Second)
	if r.Timeout%time.Second != 0 {
		deadline++
	}
	if deadline <= 0 {
		return nil, fmt.Errorf("source usage requires a bounded helper timeout")
	}
	pod.Spec.ActiveDeadlineSeconds = &deadline
	pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
	snapshot, err := json.Marshal(sourceUsageSnapshot{InitialGeneration: pool.Generation, ConfigurationEvidence: evidence})
	if err != nil {
		return nil, err
	}
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: sourceUsageSnapshotEnv, Value: string(snapshot)}}
	return pod, nil
}

func sourceUsageOwner(move volumeapi.Move) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: move.Name,
		UID: types.UID(move.UID), Controller: boolPtr(true), BlockOwnerDeletion: boolPtr(true)}}
}

func sourceUsageSnapshotFrom(pod *corev1.Pod) (sourceUsageSnapshot, error) {
	var snapshot sourceUsageSnapshot
	if pod == nil || len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Env) != 1 {
		return snapshot, fmt.Errorf("source usage snapshot missing")
	}
	env := pod.Spec.Containers[0].Env[0]
	if env.Name != sourceUsageSnapshotEnv || env.ValueFrom != nil || json.Unmarshal([]byte(env.Value), &snapshot) != nil ||
		snapshot.InitialGeneration <= 0 || !sourceUsageDigest(snapshot.ConfigurationEvidence) {
		return snapshot, fmt.Errorf("source usage snapshot invalid")
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil || string(canonical) != env.Value {
		return snapshot, fmt.Errorf("source usage snapshot contains unknown or noncanonical fields")
	}
	return snapshot, nil
}

func sourceUsageDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

// sourceUsagePodOwned validates the stored readonly execution intrinsically.
// It must also work after the Pool disappears or the controller image changes.
func (r *Runner) sourceUsagePodOwned(move volumeapi.Move, pod *corev1.Pod) error {
	if pod == nil || pod.UID == "" || pod.Name != sourceUsagePodName(move.UID) || pod.Namespace != r.Namespace ||
		!reflect.DeepEqual(pod.OwnerReferences, sourceUsageOwner(move)) || len(pod.Spec.Containers) != 1 ||
		pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds <= 0 {
		return fmt.Errorf("source usage Pod identity or ownership changed")
	}
	if _, err := sourceUsageSnapshotFrom(pod); err != nil {
		return err
	}
	copy, err := sourceUsageCommandCopy(pod.Spec.Containers[0].Command)
	if err != nil || copy.VolumeID != move.Spec.VolumeID || copy.NodeName != move.Spec.SourceNode ||
		(move.Status.SourceCopy != nil && *move.Status.SourceCopy != copy) {
		return fmt.Errorf("source usage command does not match Move source")
	}
	root, err := sourceUsageRoot(pod)
	if err != nil {
		return err
	}
	profile := *r
	profile.Image, profile.Resources, profile.ServiceAccountName = pod.Spec.Containers[0].Image, pod.Spec.Containers[0].Resources, pod.Spec.ServiceAccountName
	expected := profile.helperPod(copy.NodeName, copy.VolumeID, root, pod.Spec.Containers[0].Command)
	expected.Name, expected.Namespace = pod.Name, pod.Namespace
	expected.OwnerReferences = sourceUsageOwner(move)
	expected.Spec.AutomountServiceAccountToken = boolPtr(true)
	expected.Spec.ActiveDeadlineSeconds = pod.Spec.ActiveDeadlineSeconds
	expected.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
	expected.Spec.Containers[0].Env = pod.Spec.Containers[0].Env
	current := pod.DeepCopy()
	current.DeletionTimestamp = nil
	if profile.Image == "" || !sameSourceUsagePod(expected, current) {
		return fmt.Errorf("source usage Pod readonly execution changed")
	}
	return nil
}

func sourceUsageCommandCopy(command []string) (volume.CopyIdentity, error) {
	var copy volume.CopyIdentity
	prefixes := []string{"--pool-name=", "--pool-uid=", "--node-name=", "--pool-evidence=", "--pool-readiness-stale-after=", "--volume-id=", "--copy-identity="}
	if len(command) != 9 || command[0] != "/shiftpv-volume-helper" || command[1] != "usage" {
		return copy, fmt.Errorf("source usage command invalid")
	}
	values := make([]string, len(prefixes))
	for index, prefix := range prefixes {
		if !strings.HasPrefix(command[index+2], prefix) {
			return copy, fmt.Errorf("source usage command argument changed")
		}
		values[index] = strings.TrimPrefix(command[index+2], prefix)
	}
	staleAfter, err := time.ParseDuration(values[4])
	if err != nil || staleAfter <= 0 || !sourceUsageDigest(values[3]) || json.Unmarshal([]byte(values[6]), &copy) != nil ||
		copy.Validate() != nil || copy.Role != volume.RoleServing || copy.PoolName != values[0] || copy.PoolUID != values[1] ||
		copy.NodeName != values[2] || copy.VolumeID != values[5] {
		return copy, fmt.Errorf("source usage command identity incomplete")
	}
	return copy, nil
}

func sourceUsageRoot(pod *corev1.Pod) (string, error) {
	root := ""
	for _, entry := range pod.Spec.Volumes {
		if entry.HostPath == nil {
			continue
		}
		if entry.Name != "pool" || root != "" || entry.HostPath.Type == nil || *entry.HostPath.Type != corev1.HostPathDirectory ||
			!filepath.IsAbs(entry.HostPath.Path) || filepath.Clean(entry.HostPath.Path) == "/" {
			return "", fmt.Errorf("source usage host path changed")
		}
		root = entry.HostPath.Path
	}
	if root == "" {
		return "", fmt.Errorf("source usage Pool mount missing")
	}
	poolMounts := 0
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == "pool" {
			poolMounts++
		}
	}
	if poolMounts != 1 {
		return "", fmt.Errorf("source usage requires one readonly Pool mount")
	}
	return root, nil
}

func sameSourceUsagePod(expected, current *corev1.Pod) bool {
	if firstDifference(resultPodShapeChecks(expected, current)) != "" ||
		!reflect.DeepEqual(expected.OwnerReferences, current.OwnerReferences) ||
		!reflect.DeepEqual(expected.Spec.ActiveDeadlineSeconds, current.Spec.ActiveDeadlineSeconds) ||
		!reflect.DeepEqual(expected.Spec.Containers[0].Env, current.Spec.Containers[0].Env) || !helperPoolMountBound(expected, current) {
		return false
	}
	// Keep the existing no-environment helper contract unchanged; this path has
	// already checked its one immutable literal snapshot and nothing else.
	want, got := expected.DeepCopy(), current.DeepCopy()
	want.Spec.Containers[0].Env, got.Spec.Containers[0].Env = nil, nil
	return firstDifference(helperPodCommandChecks(want, got)) == ""
}
