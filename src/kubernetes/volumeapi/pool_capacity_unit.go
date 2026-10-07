package volumeapi

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/pool/registration"
)

const PoolCapacityPolicyFixedBlock = registration.FixedBlock

type PoolCapacityUnit = registration.CapacityUnit

func (p Pool) ConfigurationCheck() registration.Check {
	return registration.ValidateConfiguration(p.registrationConfiguration())
}

func (p Pool) TopologyConfigurationCheck() registration.Check {
	return registration.ValidateTopologyConfiguration(p.registrationConfiguration())
}

func (p Pool) BackingConfigurationCheck() registration.Check {
	return registration.ValidateBackingConfiguration(p.registrationConfiguration())
}

func (p Pool) registrationConfiguration() registration.Configuration {
	return registration.Configuration{
		NodeName: p.NodeName, PoolGroup: p.PoolGroup, MountPath: p.MountPath,
		MountPolicy: p.MountPolicy, CapacityPolicy: p.CapacityPolicy, CapacityLimit: p.CapacityLimit,
	}
}

func SameCapacityUnit(left, right *PoolCapacityUnit) bool {
	return registration.SameCapacityUnit(left, right)
}

func PoolPathsOverlap(left, right string) bool {
	return registration.PathsOverlap(left, right)
}

// RegistrationApproved preserves existing anchors and successful legacy
// observations across upgrade. New approvals are persisted independently of Ready.
func (p Pool) RegistrationApproved() bool {
	if p.Status.RegistrationApproved || p.Status.CapacityUnit != nil || p.Status.MountIdentity != nil {
		return true
	}
	ready := meta.FindStatusCondition(p.Status.Conditions, PoolConditionReady)
	inventory := p.Status.Inventory
	return ready != nil && ready.Status == metav1.ConditionTrue && inventory != nil && inventory.Valid && !inventory.Truncated && inventory.Message == ""
}

// PoolCapacityIndependent uses the same policy as the node, with API evidence.
func PoolCapacityIndependent(pool Pool, pools []Pool) (bool, string) {
	allocations := make([]registration.Allocation, 0, len(pools))
	for _, peer := range pools {
		allocations = append(allocations, peer.registrationAllocation())
	}
	check := registration.Independent(pool.registrationAllocation(), allocations)
	return check.OK, check.Reason
}

func (p Pool) registrationAllocation() registration.Allocation {
	verified, _ := p.capacityReady()
	return registration.Allocation{
		UID: p.UID, NodeName: p.NodeName, MountPath: p.MountPath,
		Policy: p.CapacityPolicy, Approved: p.RegistrationApproved(), Deleting: p.DeletionTimestamp != nil,
		Unit: p.Status.CapacityUnit, Evidence: registration.Check{OK: verified, Known: true},
	}
}

func (p Pool) capacityReady() (bool, string) {
	if p.CapacityPolicy == "" {
		return true, ""
	}
	if p.CapacityPolicy != PoolCapacityPolicyFixedBlock || p.Status.CapacityUnit.Validate() != nil {
		return false, "CapacityIdentityMissing"
	}
	condition := meta.FindStatusCondition(p.Status.Conditions, PoolConditionCapacityIndependent)
	if condition == nil || condition.ObservedGeneration != p.Generation || condition.Status != metav1.ConditionTrue {
		if condition != nil && condition.Reason != "" {
			return false, condition.Reason
		}
		return false, "CapacityProbePending"
	}
	return true, ""
}
