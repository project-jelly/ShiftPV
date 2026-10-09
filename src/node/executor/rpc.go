package executor

import (
	"context"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Execute and the recovery Watch share the same gate and filesystem primitives.
// A successful reply means the durable receipt already exists.
func (n *Node) Execute(ctx context.Context, req *protocol.EffectRequest, operation protocol.Operation) error {
	if req == nil || req.ExecutorUid != n.Identity.PodUID || req.NodeName != n.Identity.NodeName {
		return volumeapi.ErrStateConflict
	}
	unlock, err := n.lockEffect(ctx, req.VolumeName)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := n.readEffectState(ctx, req.VolumeName)
	if err != nil {
		return err
	}
	if state.UID != req.VolumeUid {
		return volumeapi.ErrStateConflict
	}
	var complete bool
	switch operation {
	case protocol.Operation_CREATE:
		complete, err = n.creationRPC(state, req)
	case protocol.Operation_RECLAIM:
		complete, err = n.cleanupRPC(ctx, state, req)
	default:
		err = volumeapi.ErrStateConflict
	}
	if err != nil || complete {
		return err
	}
	return n.executeLocked(ctx, req.VolumeName)
}

func (n *Node) VerifyRPC(ctx context.Context) error {
	pod, err := n.Pods.Get(ctx, n.Identity.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pod.UID) != n.Identity.PodUID || pod.Spec.NodeName != n.Identity.NodeName || pod.DeletionTimestamp != nil || pod.Annotations[security.CertificateAnnotation] != n.RPCCertificate {
		return volumeapi.ErrStateConflict
	}
	return nil
}

func (n *Node) creationRPC(state volumeapi.State, req *protocol.EffectRequest) (bool, error) {
	if state.CreationOperationID != req.OperationId || state.CreationExecutor == nil || *state.CreationExecutor != n.Identity {
		return false, volumeapi.ErrStateConflict
	}
	if volumeapi.ValidCreationReceipt(state) {
		return true, nil
	}
	if state.Phase != volumeapi.PhaseNodeCreating {
		return false, volumeapi.ErrStateConflict
	}
	return false, nil
}
func (n *Node) cleanupRPC(ctx context.Context, state volumeapi.State, req *protocol.EffectRequest) (bool, error) {
	approved, err := n.Cleanups.Get(ctx, cleanupapi.Authority{Kind: "ShiftPVVolume", Name: req.VolumeName, UID: req.VolumeUid})
	if err != nil {
		return false, err
	}
	if approved.Spec.OperationID != req.OperationId || approved.Status.Executor == nil || approved.Status.Executor.Kind != cleanupapi.ExecutorNode || nodeBinding(*approved.Status.Executor) != n.Identity {
		return false, cleanupapi.ErrConflict
	}
	if approved.Status.Receipt != nil && (approved.Status.Phase == cleanupapi.PhaseVerifying || approved.Status.Phase == cleanupapi.PhaseCompleted) {
		return true, nil
	}
	if state.Phase != volumeapi.PhaseDeleting || approved.Status.Phase != cleanupapi.PhaseRunning {
		return false, cleanupapi.ErrConflict
	}
	return false, nil
}

func nodeBinding(e cleanupapi.Executor) volumeapi.NodeExecutor {
	return volumeapi.NodeExecutor{Namespace: e.Namespace, PodName: e.PodName, PodUID: e.PodUID, NodeName: e.NodeName}
}
