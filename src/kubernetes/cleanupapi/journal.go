package cleanupapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

var (
	ErrConflict = errors.New("cleanup journal state precondition failed")
	// ErrNoJournal reports that the exact parent carries no cleanup journal.
	ErrNoJournal = errors.New("cleanup journal not found")
)

const (
	PhasePending           = "Pending"
	PhaseRunning           = "Running"
	PhaseVerifying         = "Verifying"
	PhaseConfirmingAbsence = "ConfirmingAbsence"
	PhaseCompleted         = "Completed"
	PhaseNeedsReview       = "NeedsReview"
)

type CopyIdentity = volume.CopyIdentity

// Authority identifies the exact durable parent that owns a cleanup journal.
// The parent's UID, rather than its reusable name, is the authority boundary.
type Authority struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type Spec struct {
	OperationID string       `json:"operationID"`
	Target      CopyIdentity `json:"target"`
	Reason      string       `json:"reason"`
	Authority   Authority    `json:"authority"`
}

type Executor struct {
	Kind      string `json:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	PodName   string `json:"podName,omitempty"`
	JobName   string `json:"jobName,omitempty"`
	JobUID    string `json:"jobUID,omitempty"`
	PodUID    string `json:"podUID,omitempty"`
	NodeName  string `json:"nodeName"`
}

type Receipt struct {
	OperationID        string `json:"operationID"`
	ExecutorUID        string `json:"executorUID"`
	ObservedAt         string `json:"observedAt"`
	Retired            bool   `json:"retired"`
	Purged             bool   `json:"purged"`
	LocalReceiptDigest string `json:"localReceiptDigest,omitempty"`
}

// AbsenceProof records the generation fence and the later exact negative
// observation used to close cleanup. ConfirmedAt is diagnostic only; the
// generation fence, validity, completeness, and exact absence are authority.
type AbsenceProof struct {
	RequestID          string `json:"requestID"`
	PoolName           string `json:"poolName"`
	PoolUID            string `json:"poolUID"`
	RequiredGeneration int64  `json:"requiredGeneration"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Valid              bool   `json:"valid,omitempty"`
	Complete           bool   `json:"complete,omitempty"`
	Absent             bool   `json:"absent,omitempty"`
	ConfirmedAt        string `json:"confirmedAt,omitempty"`
}

type Status struct {
	Phase              string        `json:"phase,omitempty"`
	ObservedGeneration int64         `json:"observedGeneration,omitempty"`
	Reason             string        `json:"reason,omitempty"`
	Message            string        `json:"message,omitempty"`
	LastTransitionTime string        `json:"lastTransitionTime,omitempty"`
	Executor           *Executor     `json:"executor,omitempty"`
	Receipt            *Receipt      `json:"receipt,omitempty"`
	AbsenceProof       *AbsenceProof `json:"absenceProof,omitempty"`
	SettledAt          string        `json:"settledAt,omitempty"`
}

// Cleanup is a synthesized view of a journal embedded at status.cleanup on an
// exact ShiftPVVolume or ShiftPVMove. UID is the parent's identity; Name is only
// a deterministic executor name component.
type Cleanup struct {
	Name   string
	UID    string
	Spec   Spec
	Status Status
}

type journal struct {
	Spec   Spec   `json:"spec"`
	Status Status `json:"status"`
}

func cleanupName(target CopyIdentity) string {
	encoded, _ := json.Marshal(target)
	sum := sha256.Sum256(encoded)
	return "shiftpv-cleanup-" + hex.EncodeToString(sum[:16])
}

func (a Authority) Validate() error {
	if !volume.ValidObjectName(a.Name) || !volume.ValidIdentityToken(a.UID) {
		return fmt.Errorf("invalid cleanup parent identity")
	}
	switch a.Kind {
	case "ShiftPVVolume", "ShiftPVMove":
		return nil
	default:
		return fmt.Errorf("invalid cleanup authority %q", a.Kind)
	}
}

func (s Spec) Validate() error {
	if !volume.ValidIdentityToken(s.OperationID) || s.Target.Validate() != nil {
		return fmt.Errorf("invalid cleanup identity")
	}
	if err := s.Authority.Validate(); err != nil {
		return err
	}
	switch s.Reason {
	case "VolumeDelete":
		if s.Authority.Kind != "ShiftPVVolume" || s.Authority.Name != s.Target.VolumeID || s.Authority.UID != s.Target.VolumeUID {
			return fmt.Errorf("volume cleanup authority does not match the target")
		}
	case "MoveSource", "MoveRollback":
		if s.Authority.Kind != "ShiftPVMove" {
			return fmt.Errorf("move cleanup requires ShiftPVMove authority")
		}
	default:
		return fmt.Errorf("invalid cleanup reason %q", s.Reason)
	}
	return nil
}

func (s *Store) parentResource(authority Authority) (dynamic.ResourceInterface, string, error) {
	if err := authority.Validate(); err != nil {
		return nil, "", err
	}
	switch authority.Kind {
	case "ShiftPVVolume":
		return s.Client.Resource(volumeapi.VolumeResource), volumeapi.VolumeProtectionFinalizer, nil
	case "ShiftPVMove":
		return s.Client.Resource(volumeapi.MoveResource), volumeapi.MoveProtectionFinalizer, nil
	default:
		return nil, "", fmt.Errorf("invalid cleanup authority %q", authority.Kind)
	}
}

func validateParent(object *unstructured.Unstructured, authority Authority, finalizer string) error {
	if object == nil || string(object.GetUID()) != authority.UID || object.GetName() != authority.Name {
		return fmt.Errorf("%w: cleanup parent identity changed", ErrConflict)
	}
	if hasFinalizer(object, finalizer) {
		return nil
	}
	return fmt.Errorf("%w: cleanup parent lacks %q", ErrConflict, finalizer)
}

func hasFinalizer(object *unstructured.Unstructured, finalizer string) bool {
	if object == nil {
		return false
	}
	for _, current := range object.GetFinalizers() {
		if current == finalizer {
			return true
		}
	}
	return false
}

func journalFromParent(object *unstructured.Unstructured) (Cleanup, bool, error) {
	value, found, err := unstructured.NestedMap(object.Object, "status", "cleanup")
	if err != nil || !found {
		return Cleanup{}, found, err
	}
	var stored journal
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(value, &stored); err != nil {
		return Cleanup{}, true, fmt.Errorf("decode cleanup journal on %s %q: %w", object.GetKind(), object.GetName(), err)
	}
	if err := stored.Spec.Validate(); err != nil {
		return Cleanup{}, true, err
	}
	if stored.Spec.Authority.Kind != object.GetKind() || stored.Spec.Authority.Name != object.GetName() || stored.Spec.Authority.UID != string(object.GetUID()) {
		return Cleanup{}, true, fmt.Errorf("%w: embedded cleanup authority does not match its parent", ErrConflict)
	}
	return Cleanup{
		Name: cleanupName(stored.Spec.Target), UID: string(object.GetUID()),
		Spec: stored.Spec, Status: stored.Status,
	}, true, nil
}

func setJournal(object *unstructured.Unstructured, stored journal) error {
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&stored)
	if err != nil {
		return err
	}
	return unstructured.SetNestedMap(object.Object, encoded, "status", "cleanup")
}
