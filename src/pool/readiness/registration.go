package readiness

import (
	"context"
	"fmt"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func invalidRegistrationCandidate(pool volumeapi.Pool) bool {
	return pool.DeletionTimestamp == nil && !pool.RegistrationApproved() && !pool.ConfigurationCheck().OK
}

// Reconcile serializes this node worker's observations. A candidate's approval
// additionally requires a fresh registration snapshot and live backing read.
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
	allocation := r.capacityIsolation(pools)[pool.UID]
	if !allocation.check.OK && allocation.check.Reason != "" {
		result.Independent = allocation.check
	} else if pool.CapacityPolicy == volumeapi.PoolCapacityPolicyFixedBlock && !volumeapi.SameCapacityUnit(allocation.unit, result.CapacityUnit) {
		result.Independent = capacityFailure("CapacityIdentityChanged", fmt.Errorf("capacity allocation changed before registration approval"))
	}
	return nil
}
