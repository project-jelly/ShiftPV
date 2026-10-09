package executor

import (
	"context"
	"errors"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperauth"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func (n *Node) execute(ctx context.Context, name string) error {
	unlock, err := n.lockEffect(ctx, name)
	if err != nil {
		return err
	}
	defer unlock()
	return n.executeLocked(ctx, name)
}
func (n *Node) executeLocked(ctx context.Context, name string) error {
	state, err := n.readEffectState(ctx, name)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if state.Phase == volumeapi.PhaseNodeCreating && state.CreationExecutor != nil && *state.CreationExecutor == n.Identity && state.CreationReceipt == nil {
		return n.create(ctx, state)
	}
	if state.Phase != volumeapi.PhaseDeleting {
		return nil
	}
	approved, err := n.Cleanups.Get(ctx, cleanupapi.Authority{Kind: "ShiftPVVolume", Name: name, UID: state.UID})
	if err != nil {
		return err
	}
	if approved.Status.Phase != cleanupapi.PhaseRunning || approved.Status.Executor == nil || approved.Status.Executor.Kind != cleanupapi.ExecutorNode || nodeBinding(*approved.Status.Executor) != n.Identity {
		return nil
	}
	return n.reclaim(ctx, approved)
}
func (n *Node) poolRoot(ctx context.Context, copy volume.CopyIdentity) (string, error) {
	if copy.Validate() != nil || copy.NodeName != n.Identity.NodeName || !filepath.IsAbs(n.HostRoot) {
		return "", volumeapi.ErrStateConflict
	}
	pool, err := n.Volumes.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return "", err
	}
	return n.rootForPool(copy, pool)
}

func (n *Node) rootForPool(copy volume.CopyIdentity, pool volumeapi.Pool) (string, error) {
	if copy.Validate() != nil || copy.NodeName != n.Identity.NodeName || !filepath.IsAbs(n.HostRoot) ||
		pool.Name != copy.PoolName || pool.UID != copy.PoolUID || pool.NodeName != copy.NodeName {
		return "", volumeapi.ErrStateConflict
	}
	if !slices.Contains(pool.Finalizers, volumeapi.PoolProtectionFinalizer) || !filepath.IsAbs(pool.MountPath) || filepath.Clean(pool.MountPath) == "/" {
		return "", volumeapi.ErrStateConflict
	}
	return filepath.Join(n.HostRoot, strings.TrimPrefix(filepath.Clean(pool.MountPath), "/")), nil
}
func (n *Node) localAuthority(ctx context.Context, copy volume.CopyIdentity, root string) error {
	pool, err := n.Volumes.PoolForIdentity(ctx, copy.PoolName, copy.PoolUID, copy.NodeName)
	if err != nil {
		return err
	}
	return n.localAuthorityForPool(ctx, copy, root, pool)
}

func (n *Node) localAuthorityForPool(ctx context.Context, copy volume.CopyIdentity, root string, pool volumeapi.Pool) error {
	pod, err := n.Pods.Get(ctx, n.Identity.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pod.UID) != n.Identity.PodUID || pod.Spec.NodeName != n.Identity.NodeName || pod.DeletionTimestamp != nil {
		return volumeapi.ErrStateConflict
	}
	installation, err := n.Volumes.InstallationID(ctx)
	if err != nil {
		return err
	}
	if installation != copy.InstallationID {
		return volumeapi.ErrStateConflict
	}
	currentRoot, err := n.rootForPool(copy, pool)
	if err != nil {
		return err
	}
	if root != currentRoot {
		return volumeapi.ErrStateConflict
	}
	if n.VerifyPool != nil {
		return n.VerifyPool(n.HostRoot, pool)
	}
	return readiness.VerifyHostPool(n.HostRoot, pool)
}
func (n *Node) create(ctx context.Context, expected volumeapi.State) error {
	if expected.CurrentCopy == nil {
		return volumeapi.ErrStateConflict
	}
	copy := *expected.CurrentCopy
	defer n.observeStep("node_create_effect", copy.VolumeID)()
	observation := creationObservation{node: n, volumeID: copy.VolumeID, now: time.Now}
	rootDone := n.observeStep("node_create_pool_root", copy.VolumeID)
	root, err := n.poolRoot(ctx, copy)
	rootDone()
	if err != nil {
		return err
	}
	authority := func(ctx context.Context) error {
		return observation.authorize(func() error { return n.creationAuthority(ctx, copy, root) })
	}
	if err := observation.local("node_create_prepare_local", func() error {
		return ownership.PrepareServing(ctx, root, copy, authority)
	}); err != nil {
		return err
	}
	var digest string
	if err := observation.local("node_create_receipt_local", func() error {
		var err error
		digest, err = ownership.ServingReceipt(ctx, root, copy, authority)
		return err
	}); err != nil {
		return err
	}
	defer n.observeStep("node_create_receipt_record", copy.VolumeID)()
	return n.Volumes.RecordCreationReceipt(ctx, expected, volumeapi.CreationReceipt{OperationID: expected.CreationOperationID, ExecutorUID: n.Identity.PodUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), LocalReceiptDigest: digest})
}

func (n *Node) creationAuthority(ctx context.Context, copy volume.CopyIdentity, root string) error {
	current, err := n.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	if current.UID != copy.VolumeUID || current.Phase != volumeapi.PhaseNodeCreating || current.CurrentCopy == nil || *current.CurrentCopy != copy || current.CreationExecutor == nil || *current.CreationExecutor != n.Identity || current.CreationOperationID != "create-"+copy.VolumeUID || current.ActiveMove != "" || len(current.PublishedNodes) != 0 || !slices.Contains(current.Finalizers, volumeapi.VolumeProtectionFinalizer) {
		return volumeapi.ErrStateConflict
	}
	pool, err := n.Volumes.CreationPoolForIdentity(ctx, copy)
	if err != nil {
		return err
	}
	return n.localAuthorityForPool(ctx, copy, root, pool)
}
func (n *Node) reclaim(ctx context.Context, approved cleanupapi.Cleanup) error {
	root, err := n.poolRoot(ctx, approved.Spec.Target)
	if err != nil {
		return err
	}
	authority := func(ctx context.Context, _ bool) error {
		current, err := n.Cleanups.Get(ctx, approved.Spec.Authority)
		if err != nil {
			return err
		}
		if current.UID != approved.UID || current.Spec != approved.Spec || current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Executor == nil || *current.Status.Executor != *approved.Status.Executor {
			return cleanupapi.ErrConflict
		}
		if err := helperauth.VerifyCleanupAuthority(ctx, n.Volumes, approved); err != nil {
			return err
		}
		return n.localAuthority(ctx, approved.Spec.Target, root)
	}
	receipt, digest, err := ownership.ReclaimWithResume(ctx, root, approved.Spec.Target, approved.Spec.OperationID, authority)
	if err != nil {
		if errors.Is(err, ownership.ErrNeedsReview) || errors.Is(err, ownership.ErrIdentity) {
			next := approved.Status
			next.Phase, next.Reason, next.Message = cleanupapi.PhaseNeedsReview, "CopyEvidenceInvalid", err.Error()
			if writeErr := n.Cleanups.UpdateStatus(ctx, approved, next); writeErr != nil {
				return writeErr
			}
		}
		return err
	}
	if !receipt.Retired || !receipt.Purged {
		return fmt.Errorf("local cleanup receipt is incomplete")
	}
	next := approved.Status
	next.Phase = cleanupapi.PhaseVerifying
	next.Receipt = &cleanupapi.Receipt{OperationID: approved.Spec.OperationID, ExecutorUID: n.Identity.PodUID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Retired: true, Purged: true, LocalReceiptDigest: digest}
	return n.Cleanups.UpdateStatus(ctx, approved, next)
}
