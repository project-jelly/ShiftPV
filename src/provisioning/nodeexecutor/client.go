package nodeexecutor

import (
	"context"
	"errors"
	"fmt"
	"github.com/project-jelly/ShiftPV/src/kubernetes/cleanupapi"
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/connection"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/volume"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

// ErrLegacyExecution requires the previous writer to settle its execution
// responsibility. The resident client never adopts, resumes or deletes it.
var ErrLegacyExecution = errors.New("legacy helper execution requires compatibility settlement")

type LegacyInspector interface {
	CreationExists(context.Context, volume.CopyIdentity) (bool, error)
	CleanupExists(context.Context, cleanupapi.Cleanup) (bool, error)
	CompletedCleanupSettled(context.Context, cleanupapi.Cleanup) (bool, error)
}

// Client binds before effects and only waits after binding. Timeout never
// changes the backend. Durable receipts survive controller/Node restarts.
type Client struct {
	Discovery Discovery
	Volumes   *volumeapi.Registry
	Inspector LegacyInspector
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
	if state.Phase == volumeapi.PhaseReady {
		return c.readyCreation(ctx, state)
	}
	if err := c.prepareCreation(ctx, state, copy); err != nil {
		return err
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
func (c *Client) FinalizeCreate(ctx context.Context, copy volume.CopyIdentity) (result error) {
	defer func() { result = classifyError(result) }()
	state, err := c.Volumes.Get(ctx, copy.VolumeID)
	if err != nil {
		return err
	}
	if state.UID != copy.VolumeUID || state.CurrentCopy == nil || *state.CurrentCopy != copy {
		return volumeapi.ErrStateConflict
	}
	return c.readyCreation(ctx, state)
}

func (c *Client) Reclaim(ctx context.Context, expected cleanupapi.Cleanup, store cleanupapi.Journal) (result cleanupapi.Cleanup, resultErr error) {
	defer func() { resultErr = classifyError(resultErr) }()
	current, ready, err := c.prepareCleanup(ctx, expected, store)
	if err != nil || ready {
		return current, err
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

// AdvanceReclaim binds the executor and observes durable progress once. Resident
// effects are driven by the Node watch; this path never waits for an RPC effect.
func (c *Client) AdvanceReclaim(ctx context.Context, expected cleanupapi.Cleanup, store cleanupapi.Journal) (current cleanupapi.Cleanup, ready bool, resultErr error) {
	defer func() { resultErr = classifyError(resultErr) }()
	current, ready, err := c.prepareCleanup(ctx, expected, store)
	if err != nil || ready {
		return current, ready, err
	}
	return current, false, nil
}

// prepareCleanup shares compatibility observation and Node binding with the
// synchronous API. Retries preserve the journal's recorded execution authority.
func (c *Client) prepareCleanup(ctx context.Context, expected cleanupapi.Cleanup, store cleanupapi.Journal) (current cleanupapi.Cleanup, ready bool, err error) {
	current, err = store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return current, false, err
	}
	if ready, err := c.inspectCleanup(ctx, expected, current); ready || err != nil {
		return current, ready, err
	}
	if _, err := c.Volumes.CleanupPoolForIdentity(ctx, current.Spec.Target.PoolName, current.Spec.Target.PoolUID, current.Spec.Target.NodeName); err != nil {
		return current, false, err
	}
	wasNodeBound := current.Status.Executor != nil
	if err := c.bindCleanup(ctx, current, store); err != nil {
		return current, false, err
	}
	// Binding and discovery may race a legacy helper or completion. Adopt only
	// a fresh observation of the same journal before choosing the effect path.
	current, err = store.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return current, false, err
	}
	if wasNodeBound && (current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode) {
		return current, false, cleanupapi.ErrConflict
	}
	if ready, err := c.inspectCleanup(ctx, expected, current); ready || err != nil {
		return current, ready, err
	}
	if current.Status.Executor == nil || current.Status.Executor.Kind != cleanupapi.ExecutorNode {
		return current, false, cleanupapi.ErrConflict
	}
	return current, false, nil
}

func (c *Client) awaitCleanup(ctx context.Context, expected cleanupapi.Cleanup, store cleanupapi.Journal, current cleanupapi.Cleanup) (cleanupapi.Cleanup, error) {
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
func (c *Client) bindCleanup(ctx context.Context, current cleanupapi.Cleanup, store cleanupapi.Journal) error {
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
		return retryError{fmt.Errorf("resident cleanup executor is unavailable")}
	}
	next := current.Status
	next.Phase = cleanupapi.PhaseRunning
	next.Executor = &cleanupapi.Executor{Kind: cleanupapi.ExecutorNode, Namespace: executor.Namespace, PodName: executor.PodName, PodUID: executor.PodUID, NodeName: executor.NodeName}
	return store.UpdateStatus(ctx, current, next)
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
		// A Job receipt alone is not settlement. Its Completed history still
		// passes independent proof and exact executor checks in inspectCleanup.
		return current.Status.Phase == cleanupapi.PhaseCompleted || current.Status.Executor.Kind == cleanupapi.ExecutorNode, nil
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

func (c *Client) prepareCreation(ctx context.Context, state volumeapi.State, copy volume.CopyIdentity) error {
	if state.CreationExecutor != nil {
		if state.Phase != volumeapi.PhaseNodeCreating && state.Phase != volumeapi.PhaseReady {
			return volumeapi.ErrStateConflict
		}
		if state.CreationReceipt == nil {
			return c.rebindCreation(ctx, state)
		}
		return nil
	}
	if state.Phase != volumeapi.PhasePending {
		return volumeapi.ErrStateConflict
	}
	if c.Inspector == nil {
		return fmt.Errorf("legacy execution inspector is not configured")
	}
	exists, err := c.Inspector.CreationExists(ctx, copy)
	if err != nil {
		return err
	}
	if exists {
		return legacyExecution("creation helper Pod is present")
	}
	executor, err := c.Discovery.Find(ctx, copy.NodeName)
	if err != nil {
		return err
	}
	if executor == nil {
		return retryError{fmt.Errorf("resident creation executor is unavailable")}
	}
	return c.Volumes.BindCreation(ctx, state, *executor)
}

func (c *Client) readyCreation(ctx context.Context, state volumeapi.State) error {
	if !volumeapi.ValidReadyCreationHistory(state) {
		return volumeapi.ErrStateConflict
	}
	if state.CreationExecutor == nil {
		if c.Inspector == nil {
			return fmt.Errorf("legacy execution inspector is not configured")
		}
		exists, err := c.Inspector.CreationExists(ctx, *state.CurrentCopy)
		if err != nil {
			return err
		}
		if exists {
			return legacyExecution("Ready volume still has a creation helper Pod")
		}
	}
	return nil
}

// ObserveCompletedCleanup checks independent completion evidence and exact
// executor quiescence without requiring a protected parent or writing a journal.
func (c *Client) ObserveCompletedCleanup(ctx context.Context, current cleanupapi.Cleanup) (ready bool, resultErr error) {
	defer func() { resultErr = classifyError(resultErr) }()
	if err := cleanupapi.ValidateSettled(current); err != nil {
		return false, err
	}
	if current.Status.Executor.Kind == cleanupapi.ExecutorNode {
		return true, nil
	}
	if c.Inspector == nil {
		return false, fmt.Errorf("legacy execution inspector is not configured")
	}
	settled, err := c.Inspector.CompletedCleanupSettled(ctx, current)
	if err != nil {
		return false, err
	}
	if !settled {
		return false, legacyExecution("completed cleanup still has a live Job executor")
	}
	return true, nil
}

func (c *Client) inspectCleanup(ctx context.Context, expected, current cleanupapi.Cleanup) (bool, error) {
	done, err := cleanupDone(expected, current)
	if err != nil {
		return false, err
	}
	if executor := current.Status.Executor; executor != nil && executor.Kind == "" {
		if current.Status.Phase != cleanupapi.PhaseCompleted {
			return false, legacyExecution("Job-bound cleanup is not independently settled")
		}
		return c.ObserveCompletedCleanup(ctx, current)
	}
	if done || current.Status.Executor != nil {
		return done, nil
	}
	if c.Inspector == nil {
		return false, fmt.Errorf("legacy execution inspector is not configured")
	}
	exists, err := c.Inspector.CleanupExists(ctx, current)
	if err != nil {
		return false, err
	}
	if exists {
		return false, legacyExecution("unbound cleanup Job is present")
	}
	return false, nil
}

func legacyExecution(message string) error {
	return retryError{fmt.Errorf("%w: %s", ErrLegacyExecution, message)}
}
