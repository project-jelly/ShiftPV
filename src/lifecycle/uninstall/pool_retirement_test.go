package uninstall

import (
	"context"
	"errors"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

const retirementVolume = "shiftpv-0123456789abcdef0123456789abcdef"

type retirementFixture struct {
	pool    volumeapi.Pool
	repo    *memoryRepository
	kube    *fake.Clientset
	cleanup *cleanupRepository
	checker *Checker
}

func newRetirementFixture() retirementFixture {
	f := retirementFixture{pool: volumeapi.Pool{Name: "lost", UID: "lost-uid", NodeName: "node", MountPath: "/mnt/lost",
		RetirementRequest: "lost-uid", Status: volumeapi.PoolStatus{RegistrationApproved: true}}}
	f.repo = &memoryRepository{pools: []volumeapi.Pool{f.pool}, volumes: map[string]volumeapi.State{}}
	f.kube, f.cleanup = fake.NewClientset(), &cleanupRepository{}
	f.checker = &Checker{Client: f.kube, Volumes: f.repo, Cleanups: f.cleanup, Namespace: "system"}
	return f
}

func retirementCopy(poolUID string) *volume.CopyIdentity {
	return &volume.CopyIdentity{InstallationID: "install", PoolName: "lost", PoolUID: poolUID, NodeName: "node",
		VolumeID: retirementVolume, VolumeUID: "volume-uid", CopyID: "copy", Role: volume.RoleServing}
}

func (f retirementFixture) owner(copy *volume.CopyIdentity) {
	f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: copy}
}

func retirementPodSpec(path, node string) corev1.PodSpec {
	return corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{Name: "pool", VolumeSource: corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{Path: path}}}}}
}

func TestPoolRetirementReferenceClosure(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*retirementFixture)
		block bool
	}{
		{"missing inventory is not physical absence but allows registration retirement", func(*retirementFixture) {}, false},
		{"current owner", func(f *retirementFixture) { f.owner(retirementCopy(f.pool.UID)) }, true},
		{"contradictory copy still retains Pool UID", func(f *retirementFixture) {
			copy := retirementCopy(f.pool.UID)
			copy.PoolName, copy.NodeName = "contradictory", "other-node"
			f.owner(copy)
		}, true},
		{"independent peer owner", func(f *retirementFixture) {
			copy := retirementCopy("peer-uid")
			copy.PoolName = "peer"
			f.owner(copy)
		}, false},
		{"initial reservation UID with contradictory name", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{InitialPool: &volumeapi.PoolReference{Name: "contradictory", UID: f.pool.UID}}
		}, true},
		{"legacy unbound creation hint cannot deadlock retirement", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", RequestName: "pvc", InitialNode: "node", CapacityBytes: 1,
				Finalizers: []string{volumeapi.VolumeProtectionFinalizer}}
		}, false},
		{"unproven creation state remains blocked", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", InitialNode: "node"}
		}, true},
		{"damaged owner cannot be excluded using its historical initial node", func(f *retirementFixture) {
			f.repo.volumes[retirementVolume] = volumeapi.State{UID: "volume-uid", Phase: volumeapi.PhaseReady, InitialNode: "other-node"}
		}, true},
		{"unapproved destination hints on an independent peer owner", func(f *retirementFixture) {
			copy := retirementCopy("peer-uid")
			copy.PoolName = "peer"
			f.owner(copy)
			f.repo.moves = []volumeapi.Move{{Name: "waiting", Spec: volumeapi.MoveSpec{VolumeID: retirementVolume, SourceNode: "node"},
				Status: volumeapi.MoveStatus{DestinationNode: "node", CandidateNodes: []string{"node"}}}}
		}, false},
		{"approved destination without proven Pool", func(f *retirementFixture) {
			copy := retirementCopy("peer-uid")
			copy.PoolName = "peer"
			f.owner(copy)
			f.repo.moves = []volumeapi.Move{{Name: "approved", Spec: volumeapi.MoveSpec{VolumeID: retirementVolume, SourceNode: "node"},
				Status: volumeapi.MoveStatus{CapacityApproved: true, DestinationNode: "node"}}}
		}, true},
		{"Move preparation UID", func(f *retirementFixture) {
			f.repo.moves = []volumeapi.Move{{Name: "preparing", Spec: volumeapi.MoveSpec{SourceNode: "other-node"}, Status: volumeapi.MoveStatus{
				Preparation: &volumeapi.MovePreparation{Pool: volumeapi.PoolReference{Name: "contradictory", UID: f.pool.UID}}}}}
		}, true},
		{"unfinished cleanup UID", func(f *retirementFixture) {
			f.cleanup.items = []cleanupapi.Cleanup{{Name: "cleanup", Spec: cleanupapi.Spec{Target: *retirementCopy(f.pool.UID),
				Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: retirementVolume}}, Status: cleanupapi.Status{Phase: cleanupapi.PhaseRunning}}}
		}, true},
		{"unproven cleanup identity", func(f *retirementFixture) {
			f.cleanup.items = []cleanupapi.Cleanup{{Name: "cleanup", Spec: cleanupapi.Spec{Target: volume.CopyIdentity{NodeName: "node"},
				Authority: cleanupapi.Authority{Kind: "ShiftPVVolume", Name: retirementVolume}}}}
		}, true},
		{"pending source Pod", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "system"}, Spec: retirementPodSpec("/mnt/lost", "node")})
		}, true},
		{"completed helper Pod", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "system"}, Spec: retirementPodSpec("/mnt/lost", "node"), Status: corev1.PodStatus{Phase: corev1.PodSucceeded}})
		}, false},
		{"pending Job without a Pod", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "system"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: retirementPodSpec("/mnt/lost", "node")}}})
		}, true},
		{"resident Node and unrelated peer helper", func(f *retirementFixture) {
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "resident", Namespace: "system"}, Spec: retirementPodSpec("/", "node")})
			_ = f.kube.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "system"}, Spec: retirementPodSpec("/mnt/peer", "node")})
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRetirementFixture()
			test.setup(&f)
			report, err := f.checker.CheckPoolRetirement(context.Background(), f.pool.Name, types.UID(f.pool.UID))
			if err != nil || report.Safe() == test.block {
				t.Fatalf("blockers=%+v error=%v want blocked=%v", report.Blockers, err, test.block)
			}
			for _, action := range f.kube.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("reference observation mutated %s", action.GetVerb())
				}
			}
		})
	}
}

func TestPoolRetirementObservationFailuresStayClosed(t *testing.T) {
	for _, resource := range []string{"pools", "volumes", "moves", "cleanups", "persistentvolumes", "pods", "jobs"} {
		t.Run(resource, func(t *testing.T) {
			f := newRetirementFixture()
			failure := errors.New("unavailable")
			switch resource {
			case "pools":
				f.repo.poolsErr = failure
			case "volumes":
				f.repo.volumesErr = failure
			case "moves":
				f.repo.movesErr = failure
			case "cleanups":
				f.cleanup.err = failure
			default:
				f.kube.PrependReactor("list", resource, func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, failure })
			}
			if _, err := f.checker.CheckPoolRetirement(context.Background(), f.pool.Name, types.UID(f.pool.UID)); err == nil {
				t.Fatal("failed read produced retirement approval")
			}
		})
	}
}
