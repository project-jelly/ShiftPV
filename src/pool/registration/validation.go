package registration

import (
	"path/filepath"
	"strings"

	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

// Independent isolates rejected candidates, but never hides an approved peer
// whose live backing is now unknown. Conflicting candidates are all rejected.
func Independent(target Allocation, allocations []Allocation) Check {
	if target.Retired && !target.Deleting {
		return failed("PoolRegistrationRetired", "Pool registration no longer participates in allocation")
	}
	if target.Policy != "" && target.Policy != FixedBlock {
		return failed("CapacityPolicyInvalid", "unsupported capacity policy")
	}
	if target.Policy == FixedBlock && (!target.Evidence.Known || !target.Evidence.OK || target.Unit.Validate() != nil) {
		if target.Evidence.Reason != "" && !target.Evidence.OK {
			return target.Evidence
		}
		return failed("CapacityIsolationUnproven", "fixed capacity evidence is missing or invalid")
	}
	if target.Deleting {
		return verified()
	}
	for _, peer := range allocations {
		if peer.NodeName != target.NodeName || peer.UID == target.UID || (target.Approved && !peer.Approved) {
			continue
		}
		if peer.Retired {
			if PathsOverlap(target.MountPath, peer.MountPath) {
				return failed("CapacityPathsOverlap", "retired Pool paths remain reserved until physical identity release")
			}
			continue
		}
		if check := independentPair(target, peer); !check.OK {
			return check
		}
	}
	return verified()
}

func independentPair(target, peer Allocation) Check {
	if target.Policy != FixedBlock {
		return failed("CapacityIsolationRequired", "multiple approved Pools on one node require capacityPolicy FixedBlock")
	}
	if PathsOverlap(target.MountPath, peer.MountPath) {
		return failed("CapacityPathsOverlap", "Pool paths overlap or are nested")
	}
	if peer.Policy != FixedBlock || !peer.Evidence.Known || !peer.Evidence.OK || peer.Unit.Validate() != nil {
		return failed("CapacityPeerUnproven", "a peer has no current fixed capacity evidence")
	}
	shared, err := capacityunit.Overlap(target.Unit.Extents, peer.Unit.Extents)
	if err != nil || shared {
		return failed("CapacityBackingOverlap", "Pools have overlapping or unproven backing allocations")
	}
	return verified()
}

func PathsOverlap(left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	return left == right || strings.HasPrefix(left, strings.TrimSuffix(right, "/")+"/") || strings.HasPrefix(right, strings.TrimSuffix(left, "/")+"/")
}

func failed(reason, message string) Check {
	return Check{Known: true, Reason: reason, Message: message}
}

func verified() Check {
	return Check{OK: true, Known: true, Reason: "CapacityAllocationVerified", Message: "Pool capacity is independent of its peers"}
}
