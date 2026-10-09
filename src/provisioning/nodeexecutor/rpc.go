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
