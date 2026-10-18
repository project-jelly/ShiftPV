package model_test

import "testing"

// A companion to the normal mobility model: the original PVC incarnation has
// ended, so settlement must retain the owner without requiring service restart.
// Starts after source unpublish with both recorded transaction artifacts.
// Fenced is the API publication gate, which clears on unlock, not a permanent
// local filesystem fence. New lifecycles after unlock are outside this model.
// No production FSM, Registry or cleanup predicate is imported here.
type retirementState struct {
	Owner, FencedOwner                          owner
	ClaimEnded, ClaimProof, Fenced, Requested   bool
	Available, Contradiction                    bool
	PublishInFlight, Mounted, Quiesced          bool
	Verifier, Verified, OwnerScan, OwnerProof   bool
	CleanupIntent, OtherPresent                 bool
	LocalReceipt, APIReceipt, Scan, Absence     bool
	Hold, ActiveMove, Unlocked, MetadataSettled bool
}

type retirementEvent uint8

const (
	retireClaim     retirementEvent = iota
	observeClaimEnd                 // NotFound or a different nonempty UID, never a timeout/DT.
	observeUnknownClaim
	commitBeforeFence
	fenceOwner
	persistSettlement
	beginPublication
	finishPublication
	unpublishOwner
	quiesceEffects
	createVerifier
	verifyOwner
	requestOwnerScan
	observeUnpublishedOwner
	startRetirementCleanup
	removeOtherCopy // The effect may succeed before its response/receipt.
	recordLocalPurge
	reportPurge
	requestAbsenceScan
	observeFreshAbsence
	releaseTemporaryHold
	removeVerifier
	unlockVolume
	finishSettlement
	endpointDown
	endpointUp
	identityContradiction
	retirementEventCount
)

func initialRetirementState() retirementState {
	return retirementState{Owner: ownerSource, Available: true, OtherPresent: true, Hold: true, ActiveMove: true}
}

func retirementStep(s retirementState, event retirementEvent) (retirementState, bool) {
	if s.Contradiction || s.MetadataSettled {
		return s, false
	}
	n := s
	switch event {
	case retireClaim:
		n.ClaimEnded = true
	case observeClaimEnd:
		if !s.ClaimEnded {
			return s, false
		}
		n.ClaimProof = true
	case observeUnknownClaim:
		n.ClaimProof = false
	case commitBeforeFence:
		if s.Fenced || s.Unlocked || s.Owner != ownerSource {
			return s, false
		}
		n.Owner = ownerDestination
	case fenceOwner:
		if !s.ClaimProof || s.Fenced || s.Unlocked {
			return s, false
		}
		// Both this write and commit compare the same exact current copy.
		n.Fenced, n.FencedOwner = true, s.Owner
	case persistSettlement:
		if !s.Fenced {
			return s, false
		}
		n.Requested = true
	case beginPublication:
		if s.Fenced || s.Unlocked || s.Owner != ownerDestination || s.Mounted || !s.Available {
			return s, false
		}
		n.PublishInFlight = true
	case finishPublication:
		if !s.PublishInFlight || !s.Available {
			return s, false
		}
		// An already approved call can finish after the API fence was stored.
		n.PublishInFlight, n.Mounted = false, true
	case unpublishOwner:
		if !s.ClaimEnded || !s.Available || s.PublishInFlight {
			return s, false
		}
		n.Mounted = false
	case quiesceEffects:
		if !s.Requested || !s.Available || s.PublishInFlight || s.Mounted {
			return s, false
		}
		n.Quiesced = true
	case createVerifier:
		if !s.Quiesced || !s.Available || s.Verified {
			return s, false
		}
		n.Verifier = true
	case verifyOwner:
		if !s.Verifier || !s.Available || s.Mounted || s.PublishInFlight {
			return s, false
		}
		n.Verified = true
		if s.Owner == ownerSource {
			n.OwnerProof = true
		}
	case requestOwnerScan:
		if !s.Quiesced || !s.Verified || s.Owner != ownerDestination {
			return s, false
		}
		n.OwnerScan = true
	case observeUnpublishedOwner:
		if !s.OwnerScan || !s.Available || s.Mounted || s.PublishInFlight {
			return s, false
		}
		n.OwnerProof = true
	case startRetirementCleanup:
		if !s.Fenced || !s.Requested || !s.Quiesced || !s.Verified || !s.OwnerProof {
			return s, false
		}
		n.CleanupIntent = true
	case removeOtherCopy:
		if !s.CleanupIntent || !s.Available {
			return s, false
		}
		n.OtherPresent = false
	case recordLocalPurge:
		if !s.CleanupIntent || !s.Available || s.OtherPresent {
			return s, false
		}
		n.LocalReceipt = true
	case reportPurge:
		if !s.LocalReceipt || !s.Available {
			return s, false
		}
		n.APIReceipt = true
	case requestAbsenceScan:
		if !s.LocalReceipt || !s.APIReceipt {
			return s, false
		}
		n.Scan = true
	case observeFreshAbsence:
		if !s.Scan || !s.Available || s.OtherPresent {
			return s, false
		}
		n.Absence = true
	case releaseTemporaryHold:
		if !s.APIReceipt || !s.Absence {
			return s, false
		}
		n.Hold = false
	case removeVerifier:
		if s.Hold || !s.Verified {
			return s, false
		}
		n.Verifier = false
	case unlockVolume:
		if s.Hold || s.Verifier || !s.APIReceipt || !s.Absence {
			return s, false
		}
		n.ActiveMove, n.Fenced, n.Unlocked = false, false, true
	case finishSettlement:
		if !s.Unlocked {
			return s, false
		}
		n.MetadataSettled = true
	case endpointDown:
		n.Available = false
	case endpointUp:
		n.Available = true
	case identityContradiction:
		n.Contradiction = true
	default:
		return s, false
	}
	return n, n != s
}

func assertRetirementSafety(t *testing.T, s retirementState) {
	t.Helper()
	if (s.Fenced || s.Unlocked) && s.Owner != s.FencedOwner {
		t.Fatal("owner changed after settlement CAS")
	}
	if s.Fenced && !s.ClaimEnded {
		t.Fatal("a live or unknown claim authorized retirement")
	}
	if s.Requested && !s.Fenced && !s.Unlocked {
		t.Fatal("settlement intent bypassed owner CAS")
	}
	if s.CleanupIntent && (!s.OwnerProof || !s.Verified || !s.Quiesced || !s.Requested) {
		t.Fatal("cleanup bypassed retained-owner proof")
	}
	if !s.Hold && (!s.APIReceipt || !s.Absence || s.OtherPresent) {
		t.Fatal("temporary hold released before cleanup settlement")
	}
	if s.Unlocked && (s.Fenced || s.Hold || s.ActiveMove || s.Verifier || !s.Verified) {
		t.Fatal("Volume unlocked before artifacts settled")
	}
	if s.OwnerProof && (s.Mounted || s.PublishInFlight) {
		t.Fatal("owner proof survived a new publication")
	}
	// The model never deletes the retained owner or its original reservation.
	// Explicit CSI Delete is a separate lifecycle after this Move unlocks.
}

func TestWorkloadSettlementModelSafetyAndRecoveredLiveness(t *testing.T) {
	queue := []retirementState{initialRetirementState()}
	seen := map[retirementState]bool{}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if seen[s] {
			continue
		}
		seen[s] = true
		assertRetirementSafety(t, s)
		for event := retirementEvent(0); event < retirementEventCount; event++ {
			if next, ok := retirementStep(s, event); ok {
				queue = append(queue, next)
			}
		}
		if s.Contradiction {
			continue
		} // Preserved for review, not settlement.
		// Once endpoints recover and the claim ends, replay fair work from every
		// reachable state, including an effect/write whose response was lost.
		recovered := s
		recovered.Available = true
		for range 64 {
			if recovered.MetadataSettled {
				break
			}
			advanced := false
			for _, event := range []retirementEvent{retireClaim, observeClaimEnd, fenceOwner, persistSettlement, finishPublication, unpublishOwner, quiesceEffects, createVerifier, verifyOwner, requestOwnerScan, observeUnpublishedOwner, startRetirementCleanup, removeOtherCopy, recordLocalPurge, reportPurge, requestAbsenceScan, observeFreshAbsence, releaseTemporaryHold, removeVerifier, unlockVolume, finishSettlement} {
				if next, ok := retirementStep(recovered, event); ok {
					recovered, advanced = next, true
					break
				}
			}
			if !advanced {
				break
			}
		}
		if !recovered.MetadataSettled || recovered.Mounted || recovered.OtherPresent || recovered.Hold || recovered.ActiveMove {
			t.Fatalf("retirement did not converge: start=%+v end=%+v", s, recovered)
		}
		assertRetirementSafety(t, recovered)
	}
	if len(seen) < 100 {
		t.Fatalf("insufficient interleavings: %d", len(seen))
	}
	t.Logf("checked %d reachable retirement states", len(seen))
}

func TestWorkloadSettlementModelRejectsMissingProof(t *testing.T) {
	s := initialRetirementState()
	for _, observation := range []retirementEvent{observeUnknownClaim, observeClaimEnd} {
		unknown, _ := retirementStep(s, observation)
		if _, ok := retirementStep(unknown, fenceOwner); ok {
			t.Fatal("unknown or still-live original claim authorized a fence")
		}
	}
	s.ClaimEnded = true
	for _, event := range []retirementEvent{observeClaimEnd, fenceOwner, persistSettlement, quiesceEffects, createVerifier, verifyOwner, startRetirementCleanup, removeOtherCopy, recordLocalPurge, reportPurge, requestAbsenceScan, observeFreshAbsence} {
		var ok bool
		s, ok = retirementStep(s, event)
		if !ok {
			t.Fatalf("fixture could not apply event %d", event)
		}
	}
	for _, missing := range []string{"API receipt", "fresh absence"} {
		incomplete := s
		if missing == "API receipt" {
			incomplete.APIReceipt = false
		} else {
			incomplete.Absence = false
		}
		if _, ok := retirementStep(incomplete, releaseTemporaryHold); ok {
			t.Fatalf("released hold without %s", missing)
		}
	}
	if _, ok := retirementStep(s, commitBeforeFence); ok {
		t.Fatal("late commit crossed exact owner fence")
	}
	if _, ok := retirementStep(s, beginPublication); ok {
		t.Fatal("settlement restarted service")
	}
}
