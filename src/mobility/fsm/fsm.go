package fsm

import "fmt"

type Phase string

const (
	PhasePending                      Phase = "Pending"
	PhasePreparing                    Phase = "Preparing"
	PhaseStartingDisruption           Phase = "StartingDisruption"
	PhaseCanceling                    Phase = "Canceling"
	PhaseCancelled                    Phase = "Cancelled"
	PhaseWaitingForUnpublish          Phase = "WaitingForUnpublish"
	PhaseWaitingForReplacement        Phase = "WaitingForReplacement"
	PhaseWaitingForDestination        Phase = "WaitingForDestination"
	PhaseWaitingForCapacity           Phase = "WaitingForCapacity"
	PhaseCopying                      Phase = "Copying"
	PhasePromoting                    Phase = "Promoting"
	PhaseCommitting                   Phase = "Committing"
	PhaseReleasingDestination         Phase = "ReleasingDestination"
	PhaseWaitingForDestinationPublish Phase = "WaitingForDestinationPublish"
	PhaseCleaningSource               Phase = "CleaningSource"
	PhaseCompleting                   Phase = "Completing"
	PhaseSucceeded                    Phase = "Succeeded"
	PhaseBlocked                      Phase = "Blocked"
)

type Action string

const (
	ActionWait                  Action = "Wait"
	ActionEnsurePlacement       Action = "EnsurePlacement"
	ActionEnsureCapacity        Action = "EnsureCapacity"
	ActionDeletePlacement       Action = "DeletePlacement"
	ActionReleasePlacement      Action = "ReleasePlacement"
	ActionEnsureCopy            Action = "EnsureCopy"
	ActionEnsurePromotion       Action = "EnsurePromotion"
	ActionCommitOwner           Action = "CommitOwner"
	ActionEnsureCleanup         Action = "EnsureCleanup"
	ActionConfirmCleanup        Action = "ConfirmCleanup"
	ActionMarkSucceeded         Action = "MarkSucceeded"
	ActionMarkBlocked           Action = "MarkBlocked"
	ActionProtectMove           Action = "ProtectMove"
	ActionInitializePreparation Action = "InitializePreparation"
	ActionClaimSource           Action = "ClaimSource"
	ActionReservePreparation    Action = "ReservePreparation"
	ActionStartDisruption       Action = "StartDisruption"
	ActionRestoreSource         Action = "RestoreSource"
	ActionReleasePreparation    Action = "ReleasePreparation"
	ActionReleaseSource         Action = "ReleaseSource"
	ActionFinalizeHistory       Action = "FinalizeHistory"
	ActionFinalizeSourceUsage   Action = "FinalizeSourceUsage"
	ActionBeginRecovery         Action = "BeginRecovery"
	ActionQuiesceRecovery       Action = "QuiesceRecovery"
	ActionVerifyRecovery        Action = "VerifyRecovery"
	ActionRetireRecovery        Action = "RetireRecovery"
	ActionResumeRecovery        Action = "ResumeRecovery"
	ActionCompleteRecovery      Action = "CompleteRecovery"
	ActionRequestSettlement     Action = "RequestSettlement"
)

const (
	RecoveryQuiescing  = "Quiescing"
	RecoveryVerifying  = "Verifying"
	RecoveryRetiring   = "Retiring"
	RecoveryResuming   = "Resuming"
	RecoveryCompleting = "Completing"
	RecoveryRecovered  = "Recovered"
)

// State retains the failed transaction phase while recovery settles its owner.
// Recovery is not a second attempt to move that owner to another node.
type State struct {
	Phase         Phase
	RecoveryPhase string
}

type Observation struct {
	WorkloadRetired            bool
	SettlementOnly             bool
	ProtectionPresent          bool
	SourcePlanRecorded         bool
	SourceClaimed              bool
	PreparationReserved        bool
	PreparationChangedReason   string
	SourceMoving               bool
	PlacementAbsent            bool
	DestinationIntentRecorded  bool
	RecoveryRequested          bool
	RecoveryOwnerIsDestination bool
	// RecoveryUnlockObserved means the exact recorded recovery owner is Ready
	// with ActiveMove cleared. It is consumed only in RecoveryCompleting.
	RecoveryUnlockObserved bool
	PreconditionsValid     bool
	PreflightDeferred      bool
	UnsafeReason           string
	SourceHealthy          bool
	SourceAuthorityInvalid bool
	ConsumerExists         bool
	ConsumersAmbiguous     bool
	PublishedOnSource      bool
	ReplacementExists      bool
	ReplacementHeld        bool
	PlacementExists        bool
	DestinationScheduled   bool
	DestinationBlocked     bool
	DestinationUnavailable bool
	CapacityApproved       bool
	CapacityBlocked        bool
	CopyComplete           bool
	CopyFailed             bool
	PromotionComplete      bool
	PromotionFailed        bool
	OwnerCommitted         bool
	PublishedOnDestination bool
	CleanupComplete        bool
	CleanupFailed          bool
	CompletionReady        bool
}

type Decision struct {
	Next   State
	Action Action
	Reason string
}

// Decide selects one lifecycle step. Callers apply Next only after the action
// completes; pending or ambiguous writes require re-observation, not advancement.
func Decide(current State, observation Observation) (Decision, error) {
	if current.Phase == "" && current.RecoveryPhase == "" {
		// A new prepared Move may already have a protected, fully observed
		// source plan. Normalization must not add a metadata-only pass.
		current.Phase = PhasePending
	}
	if err := validateState(current, observation); err != nil {
		return Decision{}, err
	}
	if decision, handled, err := decideLifecycle(current, observation); handled {
		return decision, err
	}
	decision, err := decideExecution(current.Phase, observation)
	if err != nil {
		return Decision{}, err
	}
	if err := ValidateTransition(current.Phase, decision.Next.Phase); err != nil {
		return Decision{}, err
	}
	return decision, nil
}

func decideExecution(current Phase, observation Observation) (Decision, error) {
	// These guards run in this order before any phase-specific rule,
	// and each one owns the decision outright when it fires.
	if decision, guarded, err := lifecycleGuard(current, observation); guarded {
		return decision, err
	}
	if decision, guarded, err := authorityGuard(current, observation); guarded {
		return decision, err
	}

	switch current {
	case PhaseWaitingForUnpublish:
		return decideWaitingForUnpublish(current, observation)
	case PhaseWaitingForReplacement:
		return decideWaitingForReplacement(current, observation)
	case PhaseWaitingForDestination:
		return decideWaitingForDestination(current, observation)
	case PhaseWaitingForCapacity:
		return decideWaitingForCapacity(current, observation)
	case PhaseCopying:
		return decideCopying(current, observation)
	case PhasePromoting:
		return decidePromoting(current, observation)
	case PhaseCommitting:
		return decideCommitting(current, observation)
	case PhaseReleasingDestination:
		return decideReleasingDestination(current, observation)
	case PhaseWaitingForDestinationPublish:
		return decideWaitingForDestinationPublish(current, observation)
	case PhaseCleaningSource:
		return decideCleaningSource(current, observation)
	default:
		return Decision{}, fmt.Errorf("mobility phase %q has no decision rule", current)
	}
}

func ValidateTransition(from, to Phase) error {
	if !known(from) || !known(to) {
		return fmt.Errorf("transition contains unknown phase: %q -> %q", from, to)
	}
	if terminal(from) && from != to {
		return fmt.Errorf("terminal phase %q cannot transition to %q", from, to)
	}
	for _, allowed := range allowedTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("illegal mobility transition %q -> %q", from, to)
}

var allowedTransitions = map[Phase][]Phase{
	PhasePending:                      {PhasePending, PhasePreparing, PhaseBlocked, PhaseCanceling},
	PhasePreparing:                    {PhasePreparing, PhaseStartingDisruption, PhaseCanceling},
	PhaseStartingDisruption:           {PhaseStartingDisruption, PhaseWaitingForUnpublish, PhaseCanceling},
	PhaseCanceling:                    {PhaseCanceling, PhaseCancelled},
	PhaseCancelled:                    {PhaseCancelled},
	PhaseWaitingForUnpublish:          {PhaseWaitingForUnpublish, PhaseWaitingForReplacement, PhaseBlocked, PhaseCanceling},
	PhaseWaitingForReplacement:        {PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseBlocked, PhaseCanceling},
	PhaseWaitingForDestination:        {PhaseWaitingForDestination, PhaseWaitingForCapacity, PhaseBlocked, PhaseCanceling},
	PhaseWaitingForCapacity:           {PhaseWaitingForCapacity, PhaseCopying, PhaseBlocked, PhaseCanceling},
	PhaseCopying:                      {PhaseCopying, PhasePromoting, PhaseBlocked},
	PhasePromoting:                    {PhasePromoting, PhaseCommitting, PhaseBlocked},
	PhaseCommitting:                   {PhaseCommitting, PhaseReleasingDestination, PhaseBlocked},
	PhaseReleasingDestination:         {PhaseReleasingDestination, PhaseWaitingForDestinationPublish, PhaseBlocked},
	PhaseWaitingForDestinationPublish: {PhaseWaitingForDestinationPublish, PhaseCleaningSource, PhaseBlocked},
	PhaseCleaningSource:               {PhaseCleaningSource, PhaseCompleting, PhaseBlocked},
	PhaseCompleting:                   {PhaseCompleting, PhaseSucceeded},
	PhaseSucceeded:                    {PhaseSucceeded},
	PhaseBlocked:                      {PhaseBlocked, PhaseCanceling},
}

func transition(from, to Phase, action Action, reason string) (Decision, error) {
	if err := ValidateTransition(from, to); err != nil {
		return Decision{}, err
	}
	return Decision{Next: State{Phase: to}, Action: action, Reason: reason}, nil
}

// destinationGuard is the prelude shared by every phase that still depends on a
// live destination while source authority is unchanged: an unavailable
// destination only pauses the transaction, a blocked one fails it closed. It
// reports whether it owns the decision so callers keep their phase-specific
// checks in their original order around it.
func destinationGuard(current Phase, observation Observation) (Decision, bool, error) {
	if observation.DestinationUnavailable {
		decision, err := transition(current, current, ActionWait, "DestinationUnavailable")
		return decision, true, err
	}
	if observation.DestinationBlocked {
		return blockedWith(observation, "InvalidDestination"), true, nil
	}
	return Decision{}, false, nil
}

// placementGuard re-drives the reservation Pod whenever a phase that needs a
// scheduled destination has lost it.
func placementGuard(current Phase, observation Observation) (Decision, bool, error) {
	if !observation.PlacementExists || !observation.DestinationScheduled {
		decision, err := transition(current, current, ActionEnsurePlacement, "")
		return decision, true, err
	}
	return Decision{}, false, nil
}

// consumerGuard protects actions that still choose a workload before owner
// commit. Destination publication and approved cleanup have their own evidence.
func consumerGuard(current Phase, observation Observation) (Decision, bool, error) {
	if observation.ConsumersAmbiguous {
		decision, err := transition(current, current, ActionWait, "MultipleConsumers")
		return decision, true, err
	}
	return Decision{}, false, nil
}

func blocked(reason string) Decision {
	return Decision{Next: State{Phase: PhaseBlocked}, Action: ActionMarkBlocked, Reason: reason}
}

// blockedWith prefers the observation's own diagnosis over the phase fallback.
func blockedWith(observation Observation, fallback string) Decision {
	if beforeExecutionApproval(observation) {
		return Decision{Next: State{Phase: PhaseCanceling}, Action: ActionWait, Reason: reasonOr(observation.UnsafeReason, fallback)}
	}
	return blocked(reasonOr(observation.UnsafeReason, fallback))
}

func known(phase Phase) bool {
	_, exists := allowedTransitions[phase]
	return exists
}

func terminal(phase Phase) bool {
	return phase == PhaseSucceeded || phase == PhaseCancelled
}

func beforeExecutionApproval(observation Observation) bool {
	return !observation.CapacityApproved && !observation.DestinationIntentRecorded
}

func beforeCommit(phase Phase) bool {
	switch phase {
	case PhasePending, PhaseWaitingForUnpublish,
		PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseWaitingForCapacity, PhaseCopying, PhasePromoting,
		PhaseCommitting:
		return true
	default:
		return false
	}
}

func reasonOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
