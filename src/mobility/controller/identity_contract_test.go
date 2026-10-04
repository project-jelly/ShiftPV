package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func TestTransferResourcesRejectPreviousMoveIncarnation(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{Name: "move-test", UID: "current-uid", Spec: volumeapi.MoveSpec{VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", SourceNode: "source"}}
	source, _, _ := testCopyIdentities(move.Spec.VolumeID, "source", "destination")
	move.Status.SourceCopy = &source
	previous := move
	previous.UID = "previous-uid"
	names := namesFor(move.Name)
	staleMeta := func(name string, labels map[string]string) metav1.ObjectMeta {
		metadata := moveObjectMeta(previous, name, "system", labels)
		metadata.UID = "resource-uid"
		return metadata
	}
	tests := []struct {
		name   string
		object runtime.Object
		ensure func(*Reconciler) error
	}{
		{
			name: "Secret",
			object: &corev1.Secret{ObjectMeta: staleMeta(names.Secret, transferLabels(names, previous)), Data: map[string][]byte{
				"password": []byte("password"), "secrets": []byte("shiftpv:password\n"),
			}},
			ensure: func(r *Reconciler) error { return r.ensureTransferSecret(ctx, move, names) },
		},
		{
			name:   "ConfigMap",
			object: &corev1.ConfigMap{ObjectMeta: staleMeta(names.Config, transferLabels(names, previous))},
			ensure: func(r *Reconciler) error { return r.ensureRsyncConfig(ctx, move, names) },
		},
		{
			name:   "Pod",
			object: &corev1.Pod{ObjectMeta: staleMeta(names.SourcePod, sourceLabels(names, previous))},
			ensure: func(r *Reconciler) error { return r.ensureSourcePod(ctx, move, names) },
		},
		{
			name:   "Service",
			object: &corev1.Service{ObjectMeta: staleMeta(names.SourceService, transferLabels(names, previous))},
			ensure: func(r *Reconciler) error { return r.ensureSourceService(ctx, move, names) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconciler := &Reconciler{
				Client: fake.NewSimpleClientset(test.object), Namespace: "system", HelperImage: "helper",
				Repository: &memoryRepository{pools: []volumeapi.Pool{{Name: "source-pool", NodeName: "source", MountPath: "/source"}}},
			}
			if err := test.ensure(reconciler); err == nil || !strings.Contains(err.Error(), "identity changed") {
				t.Fatalf("previous Move resource accepted: %v", err)
			}
		})
	}
}

func TestSourcePodAcceptsOnlyKubernetesServiceAccountProjection(t *testing.T) {
	ctx := context.Background()
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid",
		Spec:   volumeapi.MoveSpec{VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", SourceNode: "source"},
		Status: volumeapi.MoveStatus{CopyOperationID: "copy-operation"},
	}
	source, _, _ := testCopyIdentities(move.Spec.VolumeID, "source", "destination")
	move.Status.SourceCopy = &source
	names := namesFor(move.Name)
	client := fake.NewSimpleClientset()
	reconciler := &Reconciler{
		Client: client, Namespace: "system", ServiceAccountName: "shiftpv-controller", HelperImage: "helper:test",
		Repository: &memoryRepository{pools: []volumeapi.Pool{{Name: "source-pool", NodeName: "source", MountPath: "/source"}}},
	}
	if err := reconciler.ensureSourcePod(ctx, move, names); err != nil {
		t.Fatal(err)
	}
	pod, err := client.CoreV1().Pods("system").Get(ctx, names.SourcePod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	expiration := int64(3607)
	mode := int32(0o644)
	apiVolume := "kube-api-access-test"
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: apiVolume,
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			DefaultMode: &mode,
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{ExpirationSeconds: &expiration, Path: "token"}},
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
				{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
			},
		}},
	})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: apiVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true,
	})
	if _, err := client.CoreV1().Pods("system").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureSourcePod(ctx, move, names); err != nil {
		t.Fatalf("Kubernetes-injected service account projection rejected: %v", err)
	}

	pod, err = client.CoreV1().Pods("system").Get(ctx, names.SourcePod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "foreign", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	if _, err := client.CoreV1().Pods("system").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureSourcePod(ctx, move, names); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("foreign volume accepted: %v", err)
	}
}

func TestMoveJobsUseIdentityHelperAndRejectReplacement(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	incoming := volume.CopyIdentity{InstallationID: "installation", PoolName: "destination-pool", PoolUID: "destination-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming}
	destination := incoming
	destination.CopyID = "destination-copy"
	destination.Role = volume.RoleServing
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{
			DestinationNode: "destination", DestinationPoolUID: destination.PoolUID, SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			CopyOperationID: "copy-operation", PromotionOperationID: "promote-operation",
		},
	}
	names := namesFor(move.Name)
	move.Status.CopyJobName = names.CopyJob
	move.Status.PromotionJobName = names.PromotionJob
	client := fake.NewSimpleClientset()
	assignJobUIDs(client)
	reconciler := &Reconciler{
		Client: client, Namespace: "system", ServiceAccountName: "shiftpv-controller", HelperImage: "helper:test",
		Repository: &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {OwnerNode: source.NodeName, CurrentCopy: &source}}, pools: []volumeapi.Pool{
			{Name: source.PoolName, UID: source.PoolUID, NodeName: source.NodeName, MountPath: "/source"},
			{Name: incoming.PoolName, UID: incoming.PoolUID, NodeName: incoming.NodeName, MountPath: "/destination"},
		}},
	}
	if err := reconciler.ensureCopyJob(ctx, move, names); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensurePromotionJob(ctx, move, names); err != nil {
		t.Fatal(err)
	}
	copyJob, err := client.BatchV1().Jobs("system").Get(ctx, names.CopyJob, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	promotionJob, err := client.BatchV1().Jobs("system").Get(ctx, names.PromotionJob, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(append(copyJob.Spec.Template.Spec.Containers[0].Command, copyJob.Spec.Template.Spec.Containers[0].Args...), " "); !strings.Contains(got, "/shiftpv-volume-helper copy") || !strings.Contains(got, "--operation-id=copy-operation") {
		t.Fatalf("copy command=%q", got)
	}
	if got := strings.Join(append(promotionJob.Spec.Template.Spec.Containers[0].Command, promotionJob.Spec.Template.Spec.Containers[0].Args...), " "); !strings.Contains(got, "/shiftpv-volume-helper promote") || !strings.Contains(got, "--operation-id=promote-operation") {
		t.Fatalf("promotion command=%q", got)
	}
	for _, job := range []*batchv1.Job{copyJob, promotionJob} {
		foundPodName := false
		for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
			foundPodName = foundPodName || variable.Name == "POD_NAME" && variable.ValueFrom != nil && variable.ValueFrom.FieldRef != nil && variable.ValueFrom.FieldRef.FieldPath == "metadata.name"
		}
		if !foundPodName {
			t.Fatalf("job %q cannot prove its executor Pod identity", job.Name)
		}
	}
	copyJob.Spec.Template.Spec.Containers[0].Args = append(copyJob.Spec.Template.Spec.Containers[0].Args, "--pool-root=/foreign")
	if _, err := client.BatchV1().Jobs("system").Update(ctx, copyJob, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureCopyJob(ctx, move, names); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replacement copy Job accepted: %v", err)
	}
}

// TestMoveHelperContainersOmitPoolReadinessBudget pins the blast radius of the
// forwarded probe staleness budget. Only the cleanup helper consumes it: its
// publication proof calls ReadyPoolForIdentity, while serve-source, copy, promote
// and verify-owner recheck authority through PoolForIdentity and never judge probe
// freshness. Keeping the argument out of these four containers keeps
// sameSourcePod and sameOperationJob stable, so re-tuning
// poolReadiness.staleAfter cannot strand an in-flight Move.
func TestMoveHelperContainersOmitPoolReadinessBudget(t *testing.T) {
	ctx := context.Background()
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	incoming := volume.CopyIdentity{InstallationID: "installation", PoolName: "destination-pool", PoolUID: "destination-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "incoming-copy", NodeName: "destination", Role: volume.RoleIncoming}
	destination := incoming
	destination.CopyID, destination.Role = "destination-copy", volume.RoleServing
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{
			DestinationNode: "destination", DestinationPoolUID: destination.PoolUID, SourceCopy: &source, IncomingCopy: &incoming, DestinationCopy: &destination,
			CopyOperationID: "copy-operation", PromotionOperationID: "promote-operation",
		},
	}
	names := namesFor(move.Name)
	move.Status.CopyJobName, move.Status.PromotionJobName = names.CopyJob, names.PromotionJob
	client := fake.NewSimpleClientset()
	assignJobUIDs(client)
	reconciler := &Reconciler{
		Client: client, Namespace: "system", ServiceAccountName: "shiftpv-controller", HelperImage: "helper:test",
		PoolReadinessStaleAfter: 7 * time.Minute,
		Repository: &memoryRepository{volumes: map[string]volumeapi.State{volumeID: {OwnerNode: source.NodeName, CurrentCopy: &source}}, pools: []volumeapi.Pool{
			{Name: source.PoolName, UID: source.PoolUID, NodeName: source.NodeName, MountPath: "/source"},
			{Name: incoming.PoolName, UID: incoming.PoolUID, NodeName: incoming.NodeName, MountPath: "/destination"},
		}},
	}
	unwanted := volumeapi.PoolReadinessStaleAfterArgument(7 * time.Minute)
	if unwanted != "--pool-readiness-stale-after=7m0s" {
		t.Fatalf("forwarded argument = %q", unwanted)
	}
	for _, ensure := range []func() error{
		func() error { return reconciler.ensureSourcePod(ctx, move, names) },
		func() error { return reconciler.ensureCopyJob(ctx, move, names) },
		func() error { return reconciler.ensurePromotionJob(ctx, move, names) },
	} {
		if err := ensure(); err != nil {
			t.Fatal(err)
		}
	}
	sourcePod, err := client.CoreV1().Pods("system").Get(ctx, names.SourcePod, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	copyJob, err := client.BatchV1().Jobs("system").Get(ctx, names.CopyJob, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	promotionJob, err := client.BatchV1().Jobs("system").Get(ctx, names.PromotionJob, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recoveryNames := recoveryNames(move)
	recoveryJob, err := reconciler.verifyOwnerJob(ctx, move, recoveryNames, recoveryNames.Base+"-verify", move.Spec.SourceNode)
	if err != nil {
		t.Fatal(err)
	}
	built := map[string][]string{
		"serve-source": sourcePod.Spec.Containers[0].Args,
		"copy":         copyJob.Spec.Template.Spec.Containers[0].Args,
		"promote":      promotionJob.Spec.Template.Spec.Containers[0].Args,
		"verify-owner": recoveryJob.Spec.Template.Spec.Containers[0].Args,
	}
	for action, args := range built {
		if slices.Contains(args, unwanted) {
			t.Fatalf("%s helper carries a budget it never consumes: %v", action, args)
		}
		for _, argument := range args {
			if strings.Contains(argument, volumeapi.PoolReadinessStaleAfterFlag) {
				t.Fatalf("%s helper carries a budget it never consumes: %v", action, args)
			}
		}
	}
}

type receiptCleanupOperator struct{}

func (receiptCleanupOperator) Reclaim(ctx context.Context, request cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	executor := &cleanupapi.Executor{JobName: request.Name + "-effect", JobUID: "job-uid", PodUID: "pod-uid", NodeName: request.Spec.Target.NodeName}
	if err := store.UpdateStatus(ctx, request, cleanupapi.Status{Phase: cleanupapi.PhaseRunning, Executor: executor}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	receipt := &cleanupapi.Receipt{
		OperationID: request.Spec.OperationID, ExecutorUID: executor.JobUID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true,
		LocalReceiptDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if err := store.UpdateStatus(ctx, request, cleanupapi.Status{Phase: cleanupapi.PhaseVerifying, Executor: executor, Receipt: receipt}); err != nil {
		return cleanupapi.Cleanup{}, err
	}
	return store.Get(ctx, request.Spec.Authority)
}

func TestMoveCopyIdentityIsPersistedBeforeJobsAndCommittedExactly(t *testing.T) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid",
		VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "initial-volume-uid", NodeName: "source", Role: volume.RoleServing,
	}
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}, Status: volumeapi.MoveStatus{SourceCopy: &source, DestinationNode: "destination", DestinationPoolUID: "destination-pool-uid"}}
	repository := &memoryRepository{
		volumes: map[string]volumeapi.State{volumeID: {UID: source.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: "source", ActiveMove: move.Name, CurrentCopy: &source}},
		pools: []volumeapi.Pool{
			{Name: source.PoolName, UID: source.PoolUID, NodeName: "source", MountPath: "/source"},
			{Name: "destination-pool", UID: "destination-pool-uid", NodeName: "destination", MountPath: "/destination"},
		},
		moves: []volumeapi.Move{move},
	}
	reconciler := &Reconciler{Client: fake.NewSimpleClientset(), Namespace: "system", Repository: repository}
	if err := reconciler.prepareMoveCopyIdentities(context.Background(), &move); err != nil {
		t.Fatal(err)
	}
	if move.Status.IncomingCopy == nil || move.Status.IncomingCopy.Role != volume.RoleIncoming || move.Status.IncomingCopy.PoolUID != "destination-pool-uid" ||
		move.Status.DestinationCopy == nil || move.Status.DestinationCopy.Role != volume.RoleServing || move.Status.CopyOperationID == "" || move.Status.PromotionOperationID == "" {
		t.Fatalf("move identities=%#v", move.Status)
	}
	observed := observation{Volume: identifiedTestState(volumeID, repository.volumes[volumeID], repository.pools), DestinationNode: "destination"}
	if err := reconciler.commitOwner(context.Background(), &move, observed); err == nil {
		// A live placement reservation is intentionally required before the CAS.
		t.Fatal("owner commit bypassed placement authority")
	}
	next := repository.volumes[volumeID]
	next.Phase, next.OwnerNode, next.CurrentCopy = volumeapi.PhaseReady, "destination", move.Status.DestinationCopy
	repository.volumes[volumeID] = next
	observed.Volume = next
	observed.FSM.OwnerCommitted = true
	if err := reconciler.commitOwner(context.Background(), &move, observed); err != nil {
		t.Fatal(err)
	}
	changed := *move.Status.DestinationCopy
	changed.CopyID = "replacement"
	bad := next
	bad.CurrentCopy = &changed
	observed.Volume = bad
	if err := reconciler.commitOwner(context.Background(), &move, observed); err == nil {
		t.Fatal("committed owner with a different copy identity was accepted")
	}
}

func TestMoveCopyIdentityRejectsPoolRecreatedAfterCapacityApproval(t *testing.T) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source, _, _ := testCopyIdentities(volumeID, "source", "destination")
	move := volumeapi.Move{
		Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"},
		Status: volumeapi.MoveStatus{SourceCopy: &source, DestinationNode: "destination", DestinationPoolUID: "admitted-pool-uid"},
	}
	repository := &memoryRepository{pools: []volumeapi.Pool{
		{Name: source.PoolName, UID: source.PoolUID, NodeName: "source", MountPath: "/source"},
		{Name: "destination-pool", UID: "replacement-pool-uid", NodeName: "destination", MountPath: "/destination"},
	}}
	reconciler := &Reconciler{Repository: repository}
	if err := reconciler.prepareMoveCopyIdentities(context.Background(), &move); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("recreated destination Pool was accepted: %v", err)
	}
	if move.Status.IncomingCopy != nil || move.Status.DestinationCopy != nil {
		t.Fatalf("copy identities were created for replacement Pool: %+v", move.Status)
	}
}

func TestMoveCleanupSettlesReceiptAndFreshAbsenceForExactSource(t *testing.T) {
	volumeID := "shiftpv-0123456789abcdef0123456789abcdef"
	source := volume.CopyIdentity{InstallationID: "installation", PoolName: "source-pool", PoolUID: "source-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "source-copy", NodeName: "source", Role: volume.RoleServing}
	destination := volume.CopyIdentity{InstallationID: "installation", PoolName: "destination-pool", PoolUID: "destination-pool-uid", VolumeID: volumeID, VolumeUID: "volume-uid", CopyID: "destination-copy", NodeName: "destination", Role: volume.RoleServing}
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: "source"}, Status: volumeapi.MoveStatus{Phase: "CleaningSource", SourceCopy: &source, DestinationCopy: &destination, DestinationNode: "destination", DestinationPoolUID: destination.PoolUID}}
	repository := &memoryRepository{moves: []volumeapi.Move{move}, volumes: map[string]volumeapi.State{volumeID: {UID: source.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: "destination", ActiveMove: move.Name, CurrentCopy: &destination, PublishedNodes: []string{"destination"}}}}
	reconciler := &Reconciler{Repository: repository, Cleanups: newTestCleanupStore(), CleanupOperator: receiptCleanupOperator{}}
	if err := reconciler.ensureCleanupContract(context.Background(), &move); err != nil {
		t.Fatal(err)
	}
	complete, failed, err := reconciler.cleanupState(context.Background(), move)
	if err != nil || !complete || failed {
		t.Fatalf("cleanup complete=%v failed=%v err=%v move=%#v", complete, failed, err, move.Status)
	}
	request, err := reconciler.Cleanups.Get(context.Background(), cleanupapi.Authority{Kind: "ShiftPVMove", Name: move.Name, UID: move.UID})
	if err != nil || request.Status.Phase != cleanupapi.PhaseCompleted || request.Status.AbsenceProof == nil || request.Spec.Target != source {
		t.Fatalf("settled cleanup=%#v err=%v", request, err)
	}
}
