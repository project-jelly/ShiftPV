package fsm

import "fmt"

type Phase string

const (
	PhasePending                      Phase = "Pending"
	PhasePreparing                    Phase = "Preparing"
	PhaseStartingDisruption           Phase = "StartingDisruption"
	PhaseCanceling                    Phase = "Canceling"
	PhaseCancelled                    Phase = "Cancelled"
	PhaseLocking                      Phase = "Locking"
	PhaseEvicting                     Phase = "Evicting"
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
	ActionWait             Action = "Wait"
	ActionLockVolume       Action = "LockVolume"
	ActionEvictConsumer    Action = "EvictConsumer"
	ActionEnsurePlacement  Action = "EnsurePlacement"
	ActionEnsureCapacity   Action = "EnsureCapacity"
	ActionDeletePlacement  Action = "DeletePlacement"
	ActionReleasePlacement Action = "ReleasePlacement"
	ActionEnsureCopy       Action = "EnsureCopy"
	ActionEnsurePromotion  Action = "EnsurePromotion"
	ActionCommitOwner      Action = "CommitOwner"
	ActionEnsureCleanup    Action = "EnsureCleanup"
	ActionConfirmCleanup   Action = "ConfirmCleanup"
	ActionMarkSucceeded    Action = "MarkSucceeded"
	ActionMarkBlocked      Action = "MarkBlocked"
)

type Observation struct {
	PreconditionsValid     bool
	PreflightDeferred      bool
	UnsafeReason           string
	SourceHealthy          bool
	SourceAuthorityInvalid bool
	VolumeLocked           bool
	ConsumerExists         bool
	EvictionRequested      bool
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
	Next   Phase
	Action Action
	Reason string
}

func Decide(current Phase, observation Observation) (Decision, error) {
	if !known(current) {
		return Decision{}, fmt.Errorf("unknown mobility phase %q", current)
	}
	// These three preludes run in this order before any phase-specific rule,
	// and each one owns the decision outright when it fires.
	if decision, guarded, err := lifecycleGuard(current, observation); guarded {
		return decision, err
	}
	if decision, guarded, err := authorityGuard(current, observation); guarded {
		return decision, err
	}
	if decision, guarded, err := preflightGuard(current, observation); guarded {
		return decision, err
	}

	switch current {
	case PhasePending:
		return decidePending(current, observation)
	case PhaseLocking:
		return decideLocking(current, observation)
	case PhaseEvicting:
		return decideEvicting(current, observation)
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
	PhasePending:                      {PhasePending, PhaseLocking, PhaseBlocked},
	PhaseLocking:                      {PhaseLocking, PhaseEvicting, PhaseBlocked},
	PhaseEvicting:                     {PhaseEvicting, PhaseWaitingForUnpublish, PhaseBlocked},
	PhaseWaitingForUnpublish:          {PhaseWaitingForUnpublish, PhaseWaitingForReplacement, PhaseBlocked},
	PhaseWaitingForReplacement:        {PhaseWaitingForReplacement, PhaseWaitingForDestination, PhaseBlocked},
	PhaseWaitingForDestination:        {PhaseWaitingForDestination, PhaseWaitingForCapacity, PhaseBlocked},
	PhaseWaitingForCapacity:           {PhaseWaitingForCapacity, PhaseCopying, PhaseBlocked},
	PhaseCopying:                      {PhaseCopying, PhasePromoting, PhaseBlocked},
	PhasePromoting:                    {PhasePromoting, PhaseCommitting, PhaseBlocked},
	PhaseCommitting:                   {PhaseCommitting, PhaseReleasingDestination, PhaseBlocked},
	PhaseReleasingDestination:         {PhaseReleasingDestination, PhaseWaitingForDestinationPublish, PhaseBlocked},
	PhaseWaitingForDestinationPublish: {PhaseWaitingForDestinationPublish, PhaseCleaningSource, PhaseBlocked},
	PhaseCleaningSource:               {PhaseCleaningSource, PhaseCompleting, PhaseBlocked},
	PhaseCompleting:                   {PhaseCompleting, PhaseSucceeded},
	PhaseSucceeded:                    {PhaseSucceeded},
	PhaseBlocked:                      {PhaseBlocked},
}

func transition(from, to Phase, action Action, reason string) (Decision, error) {
	if err := ValidateTransition(from, to); err != nil {
		return Decision{}, err
	}
	return Decision{Next: to, Action: action, Reason: reason}, nil
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

func blocked(reason string) Decision {
	return Decision{Next: PhaseBlocked, Action: ActionMarkBlocked, Reason: reason}
}

// blockedWith prefers the observation's own diagnosis over the phase fallback.
func blockedWith(observation Observation, fallback string) Decision {
	return blocked(reasonOr(observation.UnsafeReason, fallback))
}

func known(phase Phase) bool {
	_, exists := allowedTransitions[phase]
	return exists
}

func terminal(phase Phase) bool {
	return phase == PhaseSucceeded || phase == PhaseBlocked
}

func beforeCommit(phase Phase) bool {
	switch phase {
	case PhasePending, PhaseLocking, PhaseEvicting, PhaseWaitingForUnpublish,
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
