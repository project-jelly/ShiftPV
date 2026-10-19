package readiness

import (
	"context"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func invalidRegistrationCandidate(pool volumeapi.Pool) bool {
	return pool.DeletionTimestamp == nil && !pool.RegistrationApproved() && !pool.ConfigurationCheck().OK
}

// Rejected registrations and cancellation never enter the writable probe.
func skipRegistrationProbe(pool volumeapi.Pool, allocation allocationObservation) bool {
	return pool.DeletingUnapprovedFixedBlock() || invalidRegistrationCandidate(pool) ||
		(allocation.backing.Reason != "" && !allocation.backing.OK) ||
		(!pool.RegistrationApproved() && allocation.independent.Reason != "" && !allocation.independent.OK)
}

func (r *Reconciler) inspectPool(pool volumeapi.Pool, allocation allocationObservation) Result {
	result := Result{Backing: allocation.backing, Independent: allocation.independent, Accessible: skipped(), Writable: skipped(), CapacityReadable: skipped()}
	if !skipRegistrationProbe(pool, allocation) {
		candidate := pool
		if candidate.Status.CapacityUnit == nil {
			candidate.Status.CapacityUnit = allocation.unit
		}
		result = r.Inspector.Inspect(candidate)
		result.Independent = allocation.independent
		if allocation.unit != nil && !volumeapi.SameCapacityUnit(allocation.unit, result.CapacityUnit) && result.Backing.OK {
			result.Backing = capacityFailure("CapacityIdentityChanged", fmt.Errorf("capacity allocation changed during node observation"))
		}
	}
	if pool.CapacityPolicy != "" {
		if result.Backing.Reason == "" {
			result.Backing = skipped()
		}
		if !result.Backing.OK {
			result.Independent = result.Backing
		}
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && !result.Mounted.Known && result.Mounted.Reason == "" {
		result.Mounted = skipped()
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint && result.Mounted.OK && result.MountIdentity == nil {
		result.Mounted = Check{Known: true, Reason: "MountIdentityMissing", Message: "mount probe did not return an identity"}
	}
	return result
}

// New registration approvals are serialized independently of existing Pool
// observations, with a fresh registration snapshot and live backing read.
// Controller placement rechecks the whole approved set, including conflicts.
func (r *Reconciler) recheckRegistration(ctx context.Context, pool volumeapi.Pool, result *Result) error {
	pools, err := r.Pools.ListPoolRegistrations(ctx)
	if err != nil {
		return err
	}
	current := false
	for _, peer := range pools {
		if peer.Name == pool.Name {
			current = peer.UID == pool.UID && peer.NodeName == pool.NodeName && peer.Generation == pool.Generation && peer.DeletionTimestamp == nil && peer.ConfigurationCheck().OK
			break
		}
	}
	if !current {
		return fmt.Errorf("%w: Pool registration changed during observation", volumeapi.ErrStateConflict)
	}
	allocation := r.capacityIsolation(ctx, pools, pool.UID)[pool.UID]
	result.Independent = allocation.independent
	if pool.CapacityPolicy == volumeapi.PoolCapacityPolicyFixedBlock {
		if !allocation.backing.OK {
			result.Backing = allocation.backing
		} else if !volumeapi.SameCapacityUnit(allocation.unit, result.CapacityUnit) {
			result.Backing = capacityFailure("CapacityIdentityChanged", fmt.Errorf("capacity allocation changed before registration approval"))
		}
		if !result.Backing.OK {
			result.Independent = result.Backing
		}
	}
	return nil
}
