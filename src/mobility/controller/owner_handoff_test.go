package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

func ownerHandoffFixture() (*Reconciler, *memoryRepository, volumeapi.Move, observation) {
	id := "shiftpv-0123456789abcdef0123456789abcdef"
	source, _, destination := testCopyIdentities(id, "source", "destination")
	move := volumeapi.Move{Name: "move-test", UID: "move-uid", Spec: volumeapi.MoveSpec{VolumeID: id, SourceNode: source.NodeName}, Status: volumeapi.MoveStatus{
		SourceCopy: &source, DestinationCopy: &destination, DestinationPoolUID: destination.PoolUID,
		DestinationNode: destination.NodeName, CandidateNodes: []string{destination.NodeName}, CapacityApproved: true,
	}}
	state := volumeapi.State{UID: source.VolumeUID, Phase: volumeapi.PhaseMoving, OwnerNode: source.NodeName, ActiveMove: move.Name, CurrentCopy: &source}
	repo := &memoryRepository{volumes: map[string]volumeapi.State{id: state}}
	r := newTestReconciler(nil, repo)
	pod := r.placementPod(move, &corev1.Pod{}, namesFor(move.Name))
	pod.Spec.NodeName = destination.NodeName
	r.Client = fake.NewSimpleClientset(pod)
	return r, repo, move, observation{Volume: state, DestinationNode: destination.NodeName}
}

func TestOwnerHandoffRequiresExactPoolIdentitiesAndLocker(t *testing.T) {
	for name, mutate := range map[string]func(*Reconciler, *volumeapi.Move, *observation){
		"missing locker":    func(r *Reconciler, _ *volumeapi.Move, _ *observation) { r.PoolLocks = nil },
		"missing source":    func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.SourceCopy = nil },
		"source pool UID":   func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.SourceCopy.PoolUID = "" },
		"source volume UID": func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.SourceCopy.VolumeUID = "other-uid" },
		"source volume ID": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Spec.VolumeID = "shiftpv-ffffffffffffffffffffffffffffffff"
		},
		"source node": func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Spec.SourceNode = "other-node" },
		"source role": func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.SourceCopy.Role = volume.RoleRetired },
		"source copy changed": func(_ *Reconciler, _ *volumeapi.Move, o *observation) {
			changed := *o.Volume.CurrentCopy
			changed.CopyID = "different-copy"
			o.Volume.CurrentCopy = &changed
		},
		"source copy absent":   func(_ *Reconciler, _ *volumeapi.Move, o *observation) { o.Volume.CurrentCopy = nil },
		"missing destination":  func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.DestinationCopy = nil },
		"destination pool UID": func(_ *Reconciler, m *volumeapi.Move, _ *observation) { m.Status.DestinationCopy.PoolUID = "" },
		"same source pool": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.PoolUID = m.Status.SourceCopy.PoolUID
			m.Status.DestinationPoolUID = m.Status.SourceCopy.PoolUID
		},
		"approved pool mismatch": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationPoolUID = "different-pool-uid"
		},
		"destination volume UID": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.VolumeUID = "different-volume-uid"
		},
		"destination volume ID": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.VolumeID = "shiftpv-ffffffffffffffffffffffffffffffff"
		},
		"destination node": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.NodeName = "other-node"
		},
		"destination role": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.Role = volume.RoleIncoming
		},
		"destination installation": func(_ *Reconciler, m *volumeapi.Move, _ *observation) {
			m.Status.DestinationCopy.InstallationID = "other-installation"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, repo, move, observed := ownerHandoffFixture()
			originalID := move.Spec.VolumeID
			mutate(r, &move, &observed)
			if err := r.commitOwner(context.Background(), &move, observed); err == nil {
				t.Fatal("owner handoff accepted incomplete or conflicting lock identity")
			}
			if repo.volumes[originalID].OwnerNode != "source" {
				t.Fatal("rejected handoff changed the owner")
			}
		})
	}
}

func TestOwnerHandoffAlreadyCommittedOnlyConfirmsCopy(t *testing.T) {
	r, _, move, observed := ownerHandoffFixture()
	r.PoolLocks, r.Client, r.Repository = nil, nil, nil
	observed.FSM.OwnerCommitted = true
	observed.Volume.Phase, observed.Volume.OwnerNode, observed.Volume.CurrentCopy = volumeapi.PhaseReady, "destination", move.Status.DestinationCopy
	if err := r.commitOwner(context.Background(), &move, observed); err != nil {
		t.Fatal(err)
	}
	changed := *observed.Volume.CurrentCopy
	changed.CopyID = "other-copy"
	observed.Volume.CurrentCopy = &changed
	if err := r.commitOwner(context.Background(), &move, observed); err == nil {
		t.Fatal("changed committed copy was accepted")
	}
}
