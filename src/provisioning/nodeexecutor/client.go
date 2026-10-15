package nodeexecutor

import (
	"context"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/helperpod"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

// Client binds before effects and only waits after binding. Timeout never
// changes the backend. Durable receipts survive controller/Node restarts.
type Client struct {
	Discovery Discovery
	Volumes   *volumeapi.Registry
	Fallback  *helperpod.Runner
	Timeout   time.Duration
	RPC       *connection.Client
}

func (c *Client) CreateCopy(ctx context.Context, copy volume.CopyIdentity) (result error) {
	defer func() { result = classifyError(result) }()
	state, err := c.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	if state.UID != copy.VolumeUID || state.CurrentCopy == nil || *state.CurrentCopy != copy {
		return volumeapi.ErrStateConflict
	}
	legacy, err := c.prepareCreation(ctx, state, copy)
	if err != nil {
		return err
	}
	if legacy {
		return c.Fallback.CreateCopy(ctx, copy)
	}
	state, err = c.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	done, err := c.createDirect(ctx, copy, state)
	if done || err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, c.Timeout, true, func(ctx context.Context) (bool, error) {
		current, err := c.Volumes.Get(ctx, copy.VolumeID)
		if err != nil {
			return false, err
		}
		if current.UID != copy.VolumeUID || current.CurrentCopy == nil || *current.CurrentCopy != copy || current.CreationExecutor == nil {
			return false, volumeapi.ErrStateConflict
		}
		return volumeapi.ValidCreationReceipt(current), nil
	})
}
func (c *Client) rebindCreation(ctx context.Context, state volumeapi.State) error {
	retired, err := c.Discovery.Retired(ctx, *state.CreationExecutor)
	if err != nil || !retired {
		return err
	}
	executor, err := c.Discovery.Find(ctx, state.OwnerNode)
	if err != nil {
		return err
	}
	if executor == nil {
		return retryError{fmt.Errorf("resident creation executor is unavailable")}
	}
	return c.Volumes.BindCreation(ctx, state, *executor)
}
func (c *Client) FinalizeCreate(ctx context.Context, copy volume.CopyIdentity) error {
	state, err := c.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	if state.CreationExecutor == nil {
		return c.Fallback.FinalizeCreate(ctx, copy)
	}
	if state.UID != copy.VolumeUID || state.CurrentCopy == nil || *state.CurrentCopy != copy || state.Phase != volumeapi.PhaseReady || !volumeapi.ValidCreationReceipt(state) {
		return volumeapi.ErrStateConflict
	}
	return nil
}

func (c *Client) Reclaim(ctx context.Context, expected cleanupapi.Cleanup, store helperpod.CleanupJournal) (result cleanupapi.Cleanup, resultErr error) {
	defer func() { resultErr = classifyError(resultErr) }()
	current, err := store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return current, err
	}
	if done, err := cleanupDone(expected, current); done || err != nil {
		return current, err
	}
	legacy, err := c.legacyCleanup(ctx, current)
	if err != nil {
		return current, err
	}
	if legacy {
		return c.Fallback.Reclaim(ctx, current, store)
	}
	if _, err := c.Volumes.CleanupPoolForIdentity(ctx, current.Spec.Target.PoolName, current.Spec.Target.PoolUID, current.Spec.Target.NodeName); err != nil {
		return current, err
	}
	wasNodeBound := current.Status.Executor != nil
	if err := c.bindCleanup(ctx, current, store); err != nil {
		return current, err
	}
	// Binding and discovery may race a legacy helper or completion. Adopt only
	// a fresh observation of the same journal before choosing the effect path.
	current, err = store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return current, err
	}
	if wasNodeBound && (current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode) {
		return current, cleanupapi.ErrConflict
	}
	if done, err := cleanupDone(expected, current); done || err != nil {
		return current, err
	}
	if current.Status.Executor == nil || current.Status.Executor.Kind == "" {
		return c.Fallback.Reclaim(ctx, current, store)
	}
	direct, err := c.invoke(ctx, nodeBinding(*current.Status.Executor), protocol.Operation_RECLAIM, cleanupRequest(current))
	if err != nil {
		return current, err
	}
	if direct {
		return c.cleanupResult(ctx, expected, store)
	}
	return c.awaitCleanup(ctx, expected, store, current)
}
func (c *Client) awaitCleanup(ctx context.Context, expected cleanupapi.Cleanup, store helperpod.CleanupJournal, current cleanupapi.Cleanup) (cleanupapi.Cleanup, error) {
	var err error
	err = wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, c.Timeout, true, func(ctx context.Context) (bool, error) {
		current, err = store.Get(ctx, expected.Spec.Authority)
		if err != nil {
			return false, err
		}
		if current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode {
			return false, cleanupapi.ErrConflict
		}
		return cleanupDone(expected, current)
	})
	return current, err
}
func nodeBinding(e cleanupapi.Executor) volumeapi.NodeExecutor {
	return volumeapi.NodeExecutor{Namespace: e.Namespace, PodName: e.PodName, PodUID: e.PodUID, NodeName: e.NodeName}
}
func (c *Client) bindCleanup(ctx context.Context, current cleanupapi.Cleanup, store helperpod.CleanupJournal) error {
	if current.Status.Executor != nil {
		if current.Status.Phase != cleanupapi.PhaseRunning || current.Status.Executor.Kind != cleanupapi.ExecutorNode || !current.Status.Executor.Valid() {
			return cleanupapi.ErrConflict
		}
		retired, err := c.Discovery.Retired(ctx, nodeBinding(*current.Status.Executor))
		if err != nil || !retired {
			return err
		}
	}
	find := c.Discovery.Find
	if current.Spec.Authority.Kind == "ShiftPVMove" {
		find = c.Discovery.FindMoveCleanup
	}
	executor, err := find(ctx, current.Spec.Target.NodeName)
	if err != nil {
		return err
	}
	if executor == nil {
		if current.Status.Executor != nil {
			return retryError{fmt.Errorf("resident cleanup executor is unavailable")}
		}
		return nil
	}
	next := current.Status
	next.Phase = cleanupapi.PhaseRunning
	next.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: executor.Namespace, PodName: executor.PodName, PodUID: executor.PodUID, NodeName: executor.NodeName}
	return store.UpdateStatus(ctx, current, next)
}

func (c *Client) legacyCleanup(ctx context.Context, current cleanupapi.Cleanup) (bool, error) {
	if current.Status.Executor != nil {
		return current.Status.Executor.Kind != cleanupapi.ExecutorNode, nil
	}
	return c.Fallback.CleanupExists(ctx, current)
}

// cleanupDone validates the re-read journal before any executor action. A
// stored receipt remains usable after the Move has released Volume.ActiveMove.
func cleanupDone(expected, current cleanupapi.Cleanup) (bool, error) {
	if current.UID == "" || current.UID != expected.UID || current.Name != expected.Name || current.Spec != expected.Spec || current.Spec.Validate() != nil {
		return false, cleanupapi.ErrConflict
	}
	switch current.Status.Phase {
	case cleanupapi.PhaseVerifying, cleanupapi.PhaseConfirmingAbsence, cleanupapi.PhaseCompleted:
		if !cleanupReceiptValid(current) {
			return false, cleanupapi.ErrConflict
		}
		// A Job receipt precedes process termination; the helper still has to
		// observe that bound Job finish before handing back Verifying.
		return current.Status.Phase != cleanupapi.PhaseVerifying || current.Status.Executor.Kind == cleanupapi.ExecutorNode, nil
	case "", cleanupapi.PhasePending:
		if current.Status.Receipt != nil || current.Status.Executor != nil {
			return false, cleanupapi.ErrConflict
		}
	case cleanupapi.PhaseRunning:
		if current.Status.Receipt != nil || current.Status.Executor == nil || !current.Status.Executor.Valid() || current.Status.Executor.NodeName != current.Spec.Target.NodeName {
			return false, cleanupapi.ErrConflict
		}
	default:
		return false, cleanupapi.ErrConflict
	}
	return false, nil
}

func cleanupReceiptValid(current cleanupapi.Cleanup) bool {
	executor, receipt := current.Status.Executor, current.Status.Receipt
	return executor != nil && executor.Valid() && executor.NodeName == current.Spec.Target.NodeName && receipt != nil &&
		receipt.OperationID == current.Spec.OperationID && receipt.ExecutorUID == executor.ExecutionUID() && receipt.Retired && receipt.Purged
}

func cleanupRequest(current cleanupapi.Cleanup) *protocol.EffectRequest {
	return &protocol.EffectRequest{ExecutorUid: current.Status.Executor.PodUID, NodeName: current.Spec.Target.NodeName,
		VolumeName: current.Spec.Target.VolumeID, VolumeUid: current.Spec.Target.VolumeUID, OperationId: current.Spec.OperationID}
}

func (c *Client) prepareCreation(ctx context.Context, state volumeapi.State, copy volume.CopyIdentity) (bool, error) {
	if state.CreationExecutor != nil {
		if state.Phase != volumeapi.PhaseNodeCreating && state.Phase != volumeapi.PhaseReady {
			return false, volumeapi.ErrStateConflict
		}
		if state.CreationReceipt == nil {
			return false, c.rebindCreation(ctx, state)
		}
		return false, nil
	}
	if state.Phase != volumeapi.PhasePending && state.Phase != volumeapi.PhaseReady {
		return false, volumeapi.ErrStateConflict
	}
	if state.Phase == volumeapi.PhaseReady {
		return true, nil
	}
	exists, err := c.Fallback.CreationExists(ctx, copy)
	if err != nil || exists {
		return exists, err
	}
	executor, err := c.Discovery.Find(ctx, copy.NodeName)
	if err != nil {
		return false, err
	}
	if executor == nil {
		return true, nil
	}
	return false, c.Volumes.BindCreation(ctx, state, *executor)
}
