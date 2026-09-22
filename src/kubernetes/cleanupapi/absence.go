package cleanupapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// ReconcileAbsence advances a receipt-bearing journal through a causal Pool
// scan fence. The first call after Verifying bumps spec.scanEpoch and records
// the generation returned by the API. Later calls complete only when the exact
// current Pool generation has a valid, complete, non-truncated inventory whose
// per-copy evidence is internally consistent and shows the target neither
// present nor published. A failed or ambiguous fence write is safe to retry: an
// unrecorded bump is followed by another bump.
func (s *Store) ReconcileAbsence(ctx context.Context, expected Cleanup) (Cleanup, bool, error) {
	current, err := s.Get(ctx, expected.Spec.Authority)
	if err != nil {
		return Cleanup{}, false, err
	}
	if current.UID != expected.UID || current.Name != expected.Name || !reflect.DeepEqual(current.Spec, expected.Spec) {
		return Cleanup{}, false, ErrConflict
	}
	switch current.Status.Phase {
	case PhaseCompleted:
		return settledAbsence(current)
	case PhaseVerifying:
		return s.openAbsenceFence(ctx, current)
	case PhaseConfirmingAbsence:
		if !validPurgedReceipt(current, current.Status) {
			return Cleanup{}, false, fmt.Errorf("%w: absence confirmation requires a purged API receipt", ErrConflict)
		}
	default:
		return Cleanup{}, false, fmt.Errorf("%w: cleanup is phase=%q", ErrConflict, current.Status.Phase)
	}

	observedGeneration, proven, err := s.scanProvesAbsence(ctx, current)
	if err != nil {
		return Cleanup{}, false, err
	}
	if !proven {
		return current, false, nil
	}
	return s.settleAbsence(ctx, current, observedGeneration)
}

// settledAbsence re-checks an already Completed journal: closing cleanup is
// only reportable while the recorded proof still stands on its own.
func settledAbsence(current Cleanup) (Cleanup, bool, error) {
	if !validPurgedReceipt(current, current.Status) || !validAbsenceFence(current.Spec.Target, current.Status.AbsenceProof) ||
		!confirmedAbsence(current.Status.AbsenceProof) || current.Status.SettledAt == "" {
		return Cleanup{}, false, fmt.Errorf("%w: completed cleanup proof is invalid", ErrConflict)
	}
	return current, true, nil
}

// openAbsenceFence bumps spec.scanEpoch on the target Pool and records the
// generation that write returned as the fence the later scan must reach.
func (s *Store) openAbsenceFence(ctx context.Context, current Cleanup) (Cleanup, bool, error) {
	if !validPurgedReceipt(current, current.Status) {
		return Cleanup{}, false, fmt.Errorf("%w: absence scan requires a purged API receipt", ErrConflict)
	}
	generation, err := s.requestPoolScan(ctx, current.Spec.Target)
	if err != nil {
		return Cleanup{}, false, err
	}
	next := current.Status
	next.Phase = PhaseConfirmingAbsence
	next.AbsenceProof = &AbsenceProof{
		RequestID:          absenceRequestID(current, generation),
		PoolName:           current.Spec.Target.PoolName,
		PoolUID:            current.Spec.Target.PoolUID,
		RequiredGeneration: generation,
	}
	if err := s.UpdateStatus(ctx, current, next); err != nil {
		return Cleanup{}, false, err
	}
	current, err = s.Get(ctx, current.Spec.Authority)
	return current, false, err
}

// scanProvesAbsence reads the fenced Pool and reports the observed generation
// only when it is the exact current one, at or past the fence, and carries
// complete evidence that the target copy is neither present nor published.
// A false result without an error is an ambiguous or stale observation, which
// leaves the journal where it is and is retried later.
func (s *Store) scanProvesAbsence(ctx context.Context, current Cleanup) (int64, bool, error) {
	pool, err := s.Client.Resource(volumeapi.PoolResource).Get(ctx, current.Spec.Target.PoolName, metav1.GetOptions{})
	if err != nil {
		return 0, false, err
	}
	if string(pool.GetUID()) != current.Spec.Target.PoolUID || !hasFinalizer(pool, volumeapi.PoolProtectionFinalizer) {
		return 0, false, fmt.Errorf("%w: cleanup Pool identity or protection changed", ErrConflict)
	}
	proof := current.Status.AbsenceProof
	if !validAbsenceFence(current.Spec.Target, proof) {
		return 0, false, fmt.Errorf("%w: cleanup absence fence is invalid", ErrConflict)
	}
	observedGeneration, _, err := unstructured.NestedInt64(pool.Object, "status", "observedGeneration")
	if err != nil {
		return 0, false, err
	}
	if observedGeneration != pool.GetGeneration() || observedGeneration < proof.RequiredGeneration {
		return 0, false, nil
	}
	inventory, found, err := volumeapi.PoolInventoryFrom(pool)
	if err != nil {
		return 0, false, err
	}
	if !found || !inventory.Valid || inventory.Truncated || inventory.Message != "" {
		return 0, false, nil
	}
	poolNode, found, err := unstructured.NestedString(pool.Object, "spec", "nodeName")
	if err != nil {
		return 0, false, err
	}
	if !found || !inventoryProvesAbsence(inventory, pool.GetName(), string(pool.GetUID()), poolNode, current.Spec.Target) {
		return 0, false, nil
	}
	return observedGeneration, true, nil
}

// settleAbsence records the confirmed proof and closes the journal.
func (s *Store) settleAbsence(ctx context.Context, current Cleanup, observedGeneration int64) (Cleanup, bool, error) {
	proof := current.Status.AbsenceProof
	next := current.Status
	next.Phase = PhaseCompleted
	next.AbsenceProof = &AbsenceProof{
		RequestID:          proof.RequestID,
		PoolName:           proof.PoolName,
		PoolUID:            proof.PoolUID,
		RequiredGeneration: proof.RequiredGeneration,
		ObservedGeneration: observedGeneration,
		Valid:              true,
		Complete:           true,
		Absent:             true,
		ConfirmedAt:        s.now().Format(time.RFC3339Nano),
	}
	next.SettledAt = s.now().Format(time.RFC3339Nano)
	if err := s.UpdateStatus(ctx, current, next); err != nil {
		return Cleanup{}, false, err
	}
	current, err := s.Get(ctx, current.Spec.Authority)
	if err != nil {
		return Cleanup{}, false, err
	}
	return current, current.Status.Phase == PhaseCompleted, nil
}

func inventoryProvesAbsence(inventory volumeapi.PoolInventory, poolName, poolUID, nodeName string, target CopyIdentity) bool {
	for _, observed := range inventory.Copies {
		if observed.Marker == "" || observed.Problem != "" || observed.Identity == nil || observed.Identity.Validate() != nil {
			return false
		}
		identity := *observed.Identity
		if identity.InstallationID != target.InstallationID || identity.PoolName != poolName || identity.PoolUID != poolUID || identity.NodeName != nodeName ||
			observed.Marker != "placement-"+identity.CopyID+".json" || observed.Published && !observed.Present {
			return false
		}
		if reflect.DeepEqual(identity, target) && (observed.Present || observed.Published) {
			return false
		}
	}
	return true
}

func (s *Store) requestPoolScan(ctx context.Context, target CopyIdentity) (int64, error) {
	generation, err := (&volumeapi.Registry{Client: s.Client}).RequestPoolScan(ctx, target)
	if errors.Is(err, volumeapi.ErrStateConflict) {
		return 0, fmt.Errorf("%w: %v", ErrConflict, err)
	}
	return generation, err
}

func absenceRequestID(cleanup Cleanup, generation int64) string {
	encoded := cleanup.Spec.Authority.UID + "\x00" + cleanup.Spec.OperationID + "\x00" + fmt.Sprint(generation)
	sum := sha256.Sum256([]byte(encoded))
	return "scan-" + hex.EncodeToString(sum[:16])
}
