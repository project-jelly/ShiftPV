package helperauth

import (
	"context"
	"errors"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestMoveAuthorityRequiresExactMoveJobPoolAndSourceCopy(t *testing.T) {
	const (
		volumeID = "shiftpv-0123456789abcdef0123456789abcdef"
		moveName = "move-test"
		moveUID  = "move-uid"
		jobName  = "copy-job"
		jobUID   = "job-uid"
	)
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	incoming := volume.CopyIdentity{InstallationID: "installation", PoolName: "destination-pool", PoolUID: "destination-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming}
	moveObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": moveName, "uid": moveUID}, "spec": map[string]any{"volumeID": volumeID, "sourceNode": "source", "protocolVersion": volumeapi.MoveProtocolPreparedV1}}}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": volumeID, "uid": source.VolumeUID}, "spec": map[string]any{"volumeID": volumeID}}}
	poolObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": incoming.PoolName, "uid": incoming.PoolUID}, "spec": map[string]any{"nodeName": incoming.NodeName, "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}}}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": source.InstallationID}}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, moveObject, volumeObject, poolObject, clusterIdentity)
	registry := &volumeapi.Registry{Client: dynamicClient}
	moveStatus := volumeapi.MoveStatus{Phase: "Copying", SourceCopy: &source, IncomingCopy: &incoming, CopyOperationID: "copy-operation", CopyJobName: jobName}
	setMoveStatusFixture(t, dynamicClient, moveName, moveStatus)
	setVolumeStateFixture(t, dynamicClient, volumeID, volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: moveName, CurrentCopy: &source})
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "system", UID: types.UID(jobUID), Labels: map[string]string{"shiftpv.io/move-uid": moveUID}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: moveName, UID: types.UID(moveUID), Controller: &controller}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "copy-pod", Namespace: "system", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: jobName, UID: types.UID(jobUID), Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: incoming.NodeName}}
	client := fake.NewClientset(job, pod)
	options := MoveOptions{MoveName: moveName, MoveUID: moveUID, OperationID: "copy-operation", Namespace: "system", PodName: pod.Name}
	authority := MoveAuthority(client, registry, options, "copy", incoming)
	if err := authority(context.Background()); err != nil {
		t.Fatal(err)
	}
	promotion := moveStatus
	destination := incoming
	destination.Role = volume.RoleServing
	promotion.PromotionJobName, promotion.PromotionOperationID, promotion.DestinationCopy = jobName, "copy-operation", &destination
	setMoveStatusFixture(t, dynamicClient, moveName, promotion)
	if err := MoveAuthority(client, registry, options, "promote", incoming)(context.Background()); err != nil {
		t.Fatalf("exact promotion operation rejected: %v", err)
	}
	moveStatus = promotion
	moveStatus.CopyOperationID = "replacement"
	setMoveStatusFixture(t, dynamicClient, moveName, moveStatus)
	if err := authority(context.Background()); err == nil {
		t.Fatal("changed durable operation retained helper authority")
	}
	setMoveStatusFixture(t, dynamicClient, moveName, promotion)
	job.Labels["shiftpv.io/move-uid"] = "replacement"
	if _, err := client.BatchV1().Jobs("system").Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := authority(context.Background()); err == nil {
		t.Fatal("changed executor identity was accepted")
	}
}

func TestRecoveryAuthorityRequiresBlockedOwnerAndOwnedExecutorJob(t *testing.T) {
	const (
		volumeID = "shiftpv-fedcba9876543210fedcba9876543210"
		moveName = "move-recovery"
		moveUID  = "move-uid"
		jobName  = "verify-job"
		jobUID   = "job-uid"
	)
	identity := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	moveObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": moveName, "uid": moveUID}, "spec": map[string]any{"volumeID": volumeID, "sourceNode": identity.NodeName, "recovery": "ResumeOwner", "protocolVersion": volumeapi.MoveProtocolPreparedV1}}}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": volumeID, "uid": identity.VolumeUID}, "spec": map[string]any{"volumeID": volumeID}}}
	poolObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": identity.PoolName, "uid": identity.PoolUID}, "spec": map[string]any{"nodeName": identity.NodeName, "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}}}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": identity.InstallationID}}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, moveObject, volumeObject, poolObject, clusterIdentity)
	registry := &volumeapi.Registry{Client: dynamicClient}
	setMoveStatusFixture(t, dynamicClient, moveName, volumeapi.MoveStatus{
		Phase: "Blocked", RecoveryPhase: "Verifying", RecoveryOwner: identity.NodeName, SourceCopy: &identity,
	})
	state := volumeapi.State{
		UID: identity.VolumeUID, Phase: volumeapi.PhaseBlocked, OwnerNode: identity.NodeName,
		ActiveMove: moveName, CurrentCopy: &identity, PublishedNodes: []string{identity.NodeName},
	}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "system", UID: types.UID(jobUID), Labels: map[string]string{"shiftpv.io/move-uid": moveUID}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: moveName, UID: types.UID(moveUID), Controller: &controller}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "verify-pod", Namespace: "system", UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: jobName, UID: types.UID(jobUID), Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: identity.NodeName}}
	client := fake.NewClientset(job, pod)
	options := MoveOptions{MoveName: moveName, MoveUID: moveUID, OperationID: "recovery-operation", Namespace: "system", PodName: pod.Name}
	authority := RecoveryAuthority(client, registry, options, identity)
	if err := authority(context.Background()); err != nil {
		t.Fatalf("exact recovery owner verification rejected: %v", err)
	}
	state.PublishedNodes = []string{identity.NodeName, "destination"}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	if err := authority(context.Background()); err == nil {
		t.Fatal("foreign publication kept recovery authority")
	}
	state.PublishedNodes = []string{identity.NodeName}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	job.Labels["shiftpv.io/move-uid"] = "replacement"
	if _, err := client.BatchV1().Jobs("system").Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := authority(context.Background()); err == nil {
		t.Fatal("changed executor Job identity kept recovery authority")
	}
}

func TestOwnedMoveJobReportsTerminatingExecutorPodWithoutNilError(t *testing.T) {
	const (
		moveName = "move-test"
		moveUID  = "move-uid"
	)
	deletionTimestamp := metav1.Now()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "copy-pod", Namespace: "system", UID: "pod-uid", DeletionTimestamp: &deletionTimestamp,
		Finalizers: []string{"shiftpv.io/test"},
	}, Spec: corev1.PodSpec{NodeName: "destination"}}
	client := fake.NewClientset(pod)
	options := MoveOptions{MoveName: moveName, MoveUID: moveUID, OperationID: "copy-operation", Namespace: "system", PodName: pod.Name}
	move := volumeapi.Move{Name: moveName, UID: moveUID}
	_, err := ownedMoveJob(context.Background(), client, options, move, "destination")
	if err == nil {
		t.Fatal("terminating executor Pod was accepted")
	}
	if !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("error = %v, want a state conflict", err)
	}
	if got := err.Error(); got != "move executor Pod is terminating: "+volumeapi.ErrStateConflict.Error() {
		t.Fatalf("error = %q", got)
	}
}

func TestSourceAuthorityRequiresCurrentMoveOwnedPodAndUnpublishedCopy(t *testing.T) {
	const (
		volumeID = "shiftpv-0123456789abcdef0123456789abcdef"
		moveName = "move-source"
		moveUID  = "move-uid"
	)
	identity := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	moveObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVMove", "metadata": map[string]any{"name": moveName, "uid": moveUID}, "spec": map[string]any{"volumeID": volumeID, "sourceNode": "source", "protocolVersion": volumeapi.MoveProtocolPreparedV1}}}
	volumeObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVVolume", "metadata": map[string]any{"name": volumeID, "uid": identity.VolumeUID}, "spec": map[string]any{"volumeID": volumeID}}}
	poolObject := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "shiftpv.io/v1alpha1", "kind": "ShiftPVPool", "metadata": map[string]any{"name": identity.PoolName, "uid": identity.PoolUID}, "spec": map[string]any{"nodeName": identity.NodeName, "mountPath": "/pool", "capacity": map[string]any{"limit": "1Gi"}}}}
	clusterIdentity := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "kube-system", "uid": identity.InstallationID}}}
	namespaceResource := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		volumeapi.VolumeResource: "ShiftPVVolumeList", volumeapi.MoveResource: "ShiftPVMoveList", volumeapi.PoolResource: "ShiftPVPoolList", namespaceResource: "NamespaceList",
	}, moveObject, volumeObject, poolObject, clusterIdentity)
	registry := &volumeapi.Registry{Client: dynamicClient}
	status := volumeapi.MoveStatus{Phase: "Copying", SourceCopy: &identity, CopyOperationID: "copy-operation"}
	setMoveStatusFixture(t, dynamicClient, moveName, status)
	setVolumeStateFixture(t, dynamicClient, volumeID, volumeapi.State{UID: identity.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: identity.NodeName, ActiveMove: moveName, CurrentCopy: &identity})
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "source-pod", Namespace: "system", UID: "pod-uid", Labels: map[string]string{"shiftpv.io/move-uid": moveUID},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "shiftpv.io/v1alpha1", Kind: "ShiftPVMove", Name: moveName, UID: types.UID(moveUID), Controller: &controller}},
	}, Spec: corev1.PodSpec{NodeName: identity.NodeName}}
	client := fake.NewClientset(pod)
	options := MoveOptions{MoveName: moveName, MoveUID: moveUID, OperationID: "copy-operation", Namespace: "system", PodName: pod.Name}
	authority := SourceAuthority(client, registry, options, identity)
	if err := authority(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := registry.Get(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	state.PublishedNodes = []string{identity.NodeName}
	setVolumeStateFixture(t, dynamicClient, volumeID, state)
	if err := authority(context.Background()); err == nil {
		t.Fatal("published source was accepted for transfer")
	}
}
