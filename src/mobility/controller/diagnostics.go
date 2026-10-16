package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/mobility/fsm"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var errMoveStatusPersistence = errors.New("Move status persistence failed; re-observation required")

type progressSnapshot struct {
	Phase                string
	RecoveryPhase        string
	PersistentVolumeName string
	ClaimNamespace       string
	ClaimName            string
	ConsumerName         string
	ConsumerUID          string
	ReplacementName      string
	ReplacementUID       string
	DestinationNode      string
	DestinationPoolUID   string
	EvictionRequested    bool
	CopyJobName          string
	PromotionJobName     string
	RecoveryOwner        string
	CopyOperationID      string
	PromotionOperationID string
	SourceCopyID         string
	IncomingCopyID       string
	DestinationCopyID    string
}

func (r *Reconciler) persistMoveStatus(ctx context.Context, move *volumeapi.Move, previous volumeapi.MoveStatus) error {
	if ctx.Err() != nil {
		return errors.Join(errMoveStatusPersistence, ctx.Err())
	}
	if reflect.DeepEqual(previous, move.Status) {
		return nil
	}
	now := r.now().UTC().Format(time.RFC3339Nano)
	if move.Status.LastTransitionTime == "" || previous.Phase != move.Status.Phase || previous.RecoveryPhase != move.Status.RecoveryPhase {
		move.Status.LastTransitionTime = now
	}
	if move.Status.LastProgressTime == "" || progressOf(previous) != progressOf(move.Status) {
		move.Status.LastProgressTime = now
	}
	if move.Spec.ProtocolVersion == volumeapi.MoveProtocolPreparedV1 {
		stored, err := r.Repository.CompareAndSetMoveStatus(ctx, *move, move.Status)
		if err != nil {
			return errors.Join(errMoveStatusPersistence, err)
		}
		*move = stored
	} else {
		if err := r.Repository.SetMoveStatus(ctx, move.Name, move.UID, move.Status); err != nil {
			return errors.Join(errMoveStatusPersistence, err)
		}
	}
	r.emitMoveEvent(*move, previous)
	return nil
}

func (r *Reconciler) recordMoveError(ctx context.Context, move *volumeapi.Move, previous volumeapi.MoveStatus, reason, operation string, cause error) error {
	if ctx.Err() != nil {
		return errors.Join(cause, ctx.Err())
	}
	if errors.Is(cause, errMoveStatusPersistence) || errors.Is(cause, errPoolBusy) || errors.Is(cause, ErrCapacityProbePending) {
		return cause
	}
	move.Status.Reason = reason
	move.Status.Message = fmt.Sprintf("automatic retry; %s: %v", operation, cause)
	statusErr := r.persistMoveStatus(ctx, move, previous)
	if statusErr != nil {
		return errors.Join(cause, fmt.Errorf("record move diagnostic: %w", statusErr))
	}
	return cause
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func progressOf(status volumeapi.MoveStatus) progressSnapshot {
	copyID := func(identity *volume.CopyIdentity) string {
		if identity == nil {
			return ""
		}
		return identity.CopyID
	}
	return progressSnapshot{
		Phase: status.Phase, RecoveryPhase: status.RecoveryPhase,
		PersistentVolumeName: status.PersistentVolumeName, ClaimNamespace: status.ClaimNamespace,
		ClaimName: status.ClaimName, ConsumerName: status.ConsumerName, ConsumerUID: status.ConsumerUID,
		ReplacementName: status.ReplacementName, ReplacementUID: status.ReplacementUID, DestinationNode: status.DestinationNode,
		DestinationPoolUID: status.DestinationPoolUID,
		EvictionRequested:  status.EvictionRequested, CopyJobName: status.CopyJobName,
		PromotionJobName: status.PromotionJobName,
		RecoveryOwner:    status.RecoveryOwner,
		CopyOperationID:  status.CopyOperationID, PromotionOperationID: status.PromotionOperationID,
		SourceCopyID: copyID(status.SourceCopy), IncomingCopyID: copyID(status.IncomingCopy), DestinationCopyID: copyID(status.DestinationCopy),
	}
}

func mobilityMessage(phase fsm.Phase, reason string) string {
	if phase == fsm.PhaseBlocked {
		return "operator action required; automatic rollback is disabled; inspect the reason and authoritative owner, restore the failed prerequisite, then use ResumeOwner only when its recovery checks can pass"
	}
	if phase == fsm.PhaseCanceling || phase == fsm.PhaseCancelled {
		return mobilityMessages[phase]
	}
	if reason != "" {
		return "automatic retry; " + waitReasonMessage(reason)
	}
	if message, exists := mobilityMessages[phase]; exists {
		return message
	}
	return "automatic retry; reconciling mobility state"
}

var mobilityMessages = map[fsm.Phase]string{
	fsm.PhasePending:                      "automatic retry; evaluating mobility preconditions",
	fsm.PhasePreparing:                    "automatic retry; reserving a destination while the source remains publishable",
	fsm.PhaseStartingDisruption:           "automatic retry; starting disruption from the durable preparation intent",
	fsm.PhaseCanceling:                    "cancelling preparation before destination execution approval",
	fsm.PhaseCancelled:                    "preparation cancelled; source authority preserved",
	fsm.PhaseLocking:                      "automatic retry; acquiring the volume mobility lock",
	fsm.PhaseEvicting:                     "automatic retry; requesting consumer eviction without bypassing its disruption budget",
	fsm.PhaseWaitingForUnpublish:          "automatic retry; waiting for the source node to report the volume unpublished",
	fsm.PhaseWaitingForReplacement:        "automatic retry; waiting for the workload controller to create a replacement Pod",
	fsm.PhaseWaitingForDestination:        "automatic retry; waiting for the scheduler to select a registered destination",
	fsm.PhaseCopying:                      "automatic retry; waiting for the destination staging copy Job",
	fsm.PhasePromoting:                    "automatic retry; waiting for destination promotion",
	fsm.PhaseCommitting:                   "automatic retry; committing the destination as authoritative owner",
	fsm.PhaseReleasingDestination:         "automatic retry; releasing the destination reservation after owner commit",
	fsm.PhaseWaitingForDestinationPublish: "automatic retry; waiting for the destination node to publish the volume",
	fsm.PhaseCleaningSource:               "automatic retry; waiting for source cleanup",
	fsm.PhaseCompleting:                   "automatic retry; finalizing confirmed cleanup and releasing the move lock",
	fsm.PhaseSucceeded:                    "mobility completed; no operator action is required",
}

func waitReasonMessage(reason string) string {
	switch reason {
	case "CompletionAuthorityMismatch":
		return "cleanup is confirmed but volume authority changed; inspect the current owner and active Move"
	case "DisruptionBudgetDenied":
		return "the PodDisruptionBudget does not currently allow eviction; wait for budget capacity or update the budget"
	case "NoCompatibleDestination", "DestinationUnavailable":
		return "no Ready registered destination satisfies the workload constraints; restore a compatible Pool node or change the hard scheduling constraints"
	case "AdmissionNotEnabled":
		return "the workload namespace is not opted in to ShiftPV mobility; restore the namespace admission label"
	case "MultipleConsumers", "MultiplePVCsUnsupported":
		return "the workload shape is outside the supported single-consumer, single-ShiftPV-PVC contract"
	case "BarePodUnsupported", "UnsupportedWorkloadController", "CustomSchedulerUnsupported":
		return "the workload controller or scheduler is outside the supported mobility contract"
	case "SchedulingGateUnsupported", "InterPodAffinityUnsupported", "TopologySpreadUnsupported", "ResourceClaimUnsupported", "SchedulerVolumeUnsupported", "ExplicitNodeNameUnsupported":
		return "a hard scheduling constraint cannot be evaluated safely by ShiftPV; remove it or move the workload explicitly"
	default:
		return "mobility is waiting because " + reason
	}
}

func recoveryPhaseMessage(phase, owner string) string {
	if phase == recoveryRecovered {
		return fmt.Sprintf("owner recovery completed on %s; the original Blocked reason is retained as history; no operator action is required", owner)
	}
	return fmt.Sprintf("automatic retry; ResumeOwner recovery is in %s on authoritative owner %s", phase, owner)
}

func recoveryErrorMessage(reason string, cause error) string {
	if reason == "ObservationFailed" {
		return fmt.Sprintf("automatic retry; ResumeOwner recovery could not observe current state: %v", cause)
	}
	return fmt.Sprintf("operator action required; ResumeOwner recovery cannot continue: %v", cause)
}

func (r *Reconciler) emitMoveEvent(move volumeapi.Move, previous volumeapi.MoveStatus) {
	if r.Recorder == nil || (previous.Phase == move.Status.Phase && previous.Reason == move.Status.Reason &&
		previous.RecoveryPhase == move.Status.RecoveryPhase && previous.RecoveryReason == move.Status.RecoveryReason) {
		return
	}
	eventType := corev1.EventTypeNormal
	reason := "Mobility" + move.Status.Phase
	message := move.Status.Message
	if move.Status.RecoveryReason != "" {
		eventType, reason, message = corev1.EventTypeWarning, move.Status.RecoveryReason, move.Status.RecoveryMessage
	} else if previous.RecoveryPhase != move.Status.RecoveryPhase && move.Status.RecoveryPhase != "" {
		reason = "Recovery" + move.Status.RecoveryPhase
		message = "ResumeOwner recovery entered " + move.Status.RecoveryPhase
	} else if move.Status.Reason != "" {
		reason = move.Status.Reason
		if move.Status.Phase == string(fsm.PhaseBlocked) || reason == "ObservationFailed" || reason == "ActionFailed" {
			eventType = corev1.EventTypeWarning
		}
	}
	object := &unstructured.Unstructured{}
	object.SetAPIVersion("shiftpv.io/v1alpha1")
	object.SetKind("ShiftPVMove")
	object.SetName(move.Name)
	object.SetUID(types.UID(move.UID))
	r.Recorder.Event(object, eventType, reason, message)
}
