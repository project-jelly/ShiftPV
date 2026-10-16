// Observation snapshot construction, ordered collection, and eligibility decisions.
package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
)

type observation struct {
	FSM             fsm.Observation
	Volume          volumeapi.State
	Pools           poolIndex
	VolumeMissing   bool
	PV              *corev1.PersistentVolume
	Claim           *corev1.PersistentVolumeClaim
	Consumer        *corev1.Pod
	Replacement     *corev1.Pod
	Placement       *corev1.Pod
	DestinationNode string
	CandidateNodes  []string
	Names           resourceNames
	SourceCordoned  bool
}

// observe builds the immutable snapshot the reconciler decides on. Each step
// below either fills part of the snapshot or reports done, meaning the partial
// snapshot it has already filled is the final diagnosis.
func (r *Reconciler) observe(ctx context.Context, move volumeapi.Move) (observation, error) {
	result := observation{Names: namesFor(move.Name)}
	pools, done, err := r.observeEligibility(ctx, move, &result)
	if done || err != nil {
		return result, err
	}
	observeMoveState(move, &result)
	observeReplacement(move, &result)
	if err := r.observePlacement(ctx, move, &result); err != nil {
		return result, err
	}
	repaired, err := r.observeDestination(ctx, move, pools, &result)
	if err != nil {
		return result, err
	}
	if err := r.observeJobs(ctx, move, repaired, &result); err != nil {
		return result, err
	}
	observeDestinationPublish(move, pools, &result)
	// Evaluate the cleanup step before the return operands are read: the Go
	// spec leaves the order between a plain operand and a call unspecified.
	if err := r.observeCleanup(ctx, move, &result); err != nil {
		return result, err
	}
	return result, nil
}

// preflightVolume judges whether a volume with no transaction yet may safely get
// one, which is all discovery asks. It returns only the verdict: every flag that
// observe derives from a Move's persisted status is meaningless here, and every
// Move-scoped child resource — the placement reservation, the transfer Jobs, the
// cleanup journal — is named after a Move that does not exist.
func (r *Reconciler) preflightVolume(ctx context.Context, volumeID, sourceNode string) (valid bool, reason string, err error) {
	var result observation
	candidate := volumeapi.Move{Spec: volumeapi.MoveSpec{VolumeID: volumeID, SourceNode: sourceNode, ProtocolVersion: volumeapi.MoveProtocolPreparedV1}}
	if _, _, err := r.observeEligibility(ctx, candidate, &result); err != nil {
		return false, "", err
	}
	return result.FSM.PreconditionsValid, result.FSM.UnsafeReason, nil
}

// observeEligibility reads the facts that do not depend on transaction progress:
// volume authority, Pools, source health, destination candidates, PV/PVC
// binding, consumers and the preflight verdict. It returns the Pool snapshot its
// later siblings reuse so readiness is read once per observation.
func (r *Reconciler) observeEligibility(ctx context.Context, move volumeapi.Move, result *observation) (poolIndex, bool, error) {
	if done, err := r.observeVolume(ctx, move, result); done || err != nil {
		return poolIndex{}, true, err
	}
	pools, err := r.observePools(ctx)
	if err != nil {
		return pools, true, err
	}
	if done, err := r.observeSource(ctx, move, pools, result); done || err != nil {
		return pools, true, err
	}
	if err := r.observeCandidates(ctx, move, pools, result); err != nil {
		return pools, true, err
	}
	if done, err := r.observeBinding(ctx, move, result); done || err != nil {
		return pools, true, err
	}
	if done, err := r.observeConsumers(ctx, move, result); done || err != nil {
		return pools, true, err
	}
	if err := r.observePreconditions(ctx, move, result); err != nil {
		return pools, true, err
	}
	return pools, false, nil
}

func (r *Reconciler) observePreconditions(ctx context.Context, move volumeapi.Move, result *observation) error {
	sourceHealthy := result.FSM.SourceHealthy
	sourceCordoned := result.SourceCordoned
	preconditions := sourceHealthy && !result.FSM.SourceAuthorityInvalid && sourceCordoned && len(result.CandidateNodes) > 0 && result.Consumer != nil && metav1.GetControllerOf(result.Consumer) != nil
	// The original consumer is expected to disappear after eviction. Eligibility
	// diagnostics must not mask CopyFailed/PromotionFailed/CleanupFailed later.
	preflight := move.Status.Phase == "" || move.Status.Phase == string(fsm.PhasePending)
	if preflight && !preconditions && result.FSM.UnsafeReason == "" {
		if reason := unmetPreconditionReason(result, sourceCordoned); reason != "" {
			result.FSM.UnsafeReason = reason
		}
	}
	result.FSM.PreconditionsValid = preconditions
	if preEviction(move) && result.Consumer != nil && sourceHealthy {
		return r.deferOnPreflight(ctx, result, preconditions)
	}
	return nil
}

// unmetPreconditionReason names the first unmet eligibility precondition, or
// "" when none of the diagnosable ones is the cause.
func unmetPreconditionReason(result *observation, sourceCordoned bool) string {
	switch {
	case result.Consumer == nil:
		return "ControlledConsumerMissing"
	case metav1.GetControllerOf(result.Consumer) == nil:
		return "BarePodUnsupported"
	case len(result.CandidateNodes) == 0:
		return "DestinationUnavailable"
	case !sourceCordoned:
		return "SourceNotCordoned"
	}
	return ""
}

// deferOnPreflight runs preflight on a transaction that has not evicted yet and
// records the deferral it reports. A failing precondition with no preflight
// reason of its own still defers, under the eligibility reason already
// diagnosed or the generic one.
func (r *Reconciler) deferOnPreflight(ctx context.Context, result *observation, preconditions bool) error {
	reason, err := r.preflight(ctx, result)
	if err != nil {
		return err
	}
	if reason == "" && !preconditions {
		reason = result.FSM.UnsafeReason
		if reason == "" {
			reason = "PreconditionFailed"
		}
	}
	if reason != "" {
		result.FSM.PreconditionsValid = false
		result.FSM.PreflightDeferred = true
		result.FSM.UnsafeReason = reason
	}
	return nil
}
