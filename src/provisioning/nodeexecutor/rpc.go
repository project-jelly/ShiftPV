package nodeexecutor

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	"github.com/project-jelly/ShiftPV/src/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// Endpoint is read through the authenticated API for the exact bound Pod.
// IP and public certificate are hints until TLS and request identity agree.
func (d Discovery) Endpoint(ctx context.Context, identity volumeapi.NodeExecutor) (connection.Target, bool, error) {
	if !identity.Valid() || identity.Namespace != d.Namespace || d.DaemonSet == "" {
		return connection.Target{}, false, volumeapi.ErrStateConflict
	}
	pod, err := d.Client.CoreV1().Pods(identity.Namespace).Get(ctx, identity.PodName, metav1.GetOptions{})
	if err != nil {
		return connection.Target{}, false, err
	}
	if string(pod.UID) != identity.PodUID || pod.Spec.NodeName != identity.NodeName || pod.DeletionTimestamp != nil {
		return connection.Target{}, false, volumeapi.ErrStateConflict
	}
	ds, err := d.Client.AppsV1().DaemonSets(d.Namespace).Get(ctx, d.DaemonSet, metav1.GetOptions{})
	if err != nil {
		return connection.Target{}, false, err
	}
	if !ownedExecutor(*pod, ds) {
		return connection.Target{}, false, volumeapi.ErrStateConflict
	}
	certificate := pod.Annotations[security.CertificateAnnotation]
	if certificate == "" {
		return connection.Target{}, false, nil
	}
	port, err := strconv.Atoi(pod.Annotations[security.PortAnnotation])
	if err != nil || port < 1 || port > 65535 || net.ParseIP(pod.Status.PodIP) == nil {
		return connection.Target{}, false, retryError{volumeapi.ErrStateConflict}
	}
	return connection.Target{Identity: identity, Address: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port)), Certificate: certificate}, true, nil
}

func (c *Client) invoke(ctx context.Context, executor volumeapi.NodeExecutor, operation protocol.Operation, req *protocol.EffectRequest) (bool, error) {
	if c.RPC == nil {
		return false, nil
	}
	target, supported, err := c.Discovery.Endpoint(ctx, executor)
	if err != nil || !supported {
		return supported, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	err = c.RPC.Execute(ctx, target, operation, req)
	klog.V(2).InfoS("ShiftPV Node RPC", "operation", operation.String(), "operationID", req.OperationId, "node", executor.NodeName, "executorUID", executor.PodUID, "duration", time.Since(started), "error", err)
	return true, err
}

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
	pod, err := n.Discovery.Client.CoreV1().Pods(n.Identity.Namespace).Get(ctx, n.Identity.PodName, metav1.GetOptions{})
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
func (c *Client) createDirect(ctx context.Context, copy volume.CopyIdentity, state volumeapi.State) (bool, error) {
	if state.UID != copy.VolumeUID || state.CurrentCopy == nil || *state.CurrentCopy != copy || state.CreationExecutor == nil {
		return false, volumeapi.ErrStateConflict
	}
	if volumeapi.ValidCreationReceipt(state) {
		return true, nil
	}
	direct, err := c.invoke(ctx, *state.CreationExecutor, protocol.Operation_CREATE, &protocol.EffectRequest{ExecutorUid: state.CreationExecutor.PodUID, NodeName: copy.NodeName, VolumeName: copy.VolumeID, VolumeUid: copy.VolumeUID, OperationId: state.CreationOperationID})
	if err != nil || !direct {
		return direct, err
	}
	current, err := c.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return true, err
	}
	if current.UID != copy.VolumeUID || current.CurrentCopy == nil || *current.CurrentCopy != copy || !volumeapi.ValidCreationReceipt(current) {
		return true, volumeapi.ErrStateConflict
	}
	return true, nil
}
func (c *Client) cleanupResult(ctx context.Context, expected cleanupapi.Cleanup, store helperpod.CleanupJournal) (cleanupapi.Cleanup, error) {
	current, err := store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return current, err
	}
	if current.Spec != expected.Spec || current.UID != expected.UID || current.Status.Receipt == nil || current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode || current.Status.Phase != cleanupapi.PhaseVerifying {
		return current, cleanupapi.ErrConflict
	}
	return current, nil
}
