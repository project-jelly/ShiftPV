package volumeapi

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

const PoolCapacityPolicyFixedBlock = "FixedBlock"

// PoolCapacityUnit anchors one filesystem to normalized fixed block allocations.
// Device numbers alone cannot distinguish thick from thin LVM allocations.
type PoolCapacityUnit struct {
	Device     string                `json:"device"`
	Source     string                `json:"source"`
	Filesystem string                `json:"filesystem"`
	Extents    []capacityunit.Extent `json:"extents"`
}

func (u *PoolCapacityUnit) Validate() error {
	if u == nil || capacityunit.ValidateDevice(u.Device) != nil || u.Source == "" || (u.Filesystem != "ext4" && u.Filesystem != "xfs") {
		return fmt.Errorf("fixed filesystem capacity identity is incomplete or unsupported")
	}
	normalized, err := capacityunit.Normalize(u.Extents)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalized, u.Extents) {
		return fmt.Errorf("capacity allocation evidence is not normalized")
	}
	return nil
}

func SameCapacityUnit(left, right *PoolCapacityUnit) bool {
	return left != nil && right != nil && left.Validate() == nil && right.Validate() == nil && reflect.DeepEqual(left, right)
}

// PoolCapacityIndependent requires complete node evidence for every registered
// capacity unit before permitting multiple Pools on that node.
func PoolCapacityIndependent(pool Pool, pools []Pool) (bool, string) {
	var peers []Pool
	for _, peer := range pools {
		if peer.NodeName == pool.NodeName {
			peers = append(peers, peer)
		}
	}
	if len(peers) <= 1 {
		return true, ""
	}
	if pool.CapacityPolicy != PoolCapacityPolicyFixedBlock || pool.Status.CapacityUnit.Validate() != nil {
		return false, "CapacityIsolationUnproven"
	}
	for _, peer := range peers {
		if peer.CapacityPolicy != PoolCapacityPolicyFixedBlock || peer.Status.CapacityUnit.Validate() != nil {
			return false, "CapacityPeerUnproven"
		}
		if verified, _ := peer.capacityReady(); !verified {
			return false, "CapacityPeerUnproven"
		}
		if peer.UID == pool.UID {
			continue
		}
		if PoolPathsOverlap(pool.MountPath, peer.MountPath) {
			return false, "CapacityPathsOverlap"
		}
		shared, err := capacityunit.Overlap(pool.Status.CapacityUnit.Extents, peer.Status.CapacityUnit.Extents)
		if err != nil || shared {
			return false, "CapacityBackingOverlap"
		}
	}
	return true, ""
}

// PoolPathsOverlap rejects identical and nested roots, including separate
// filesystems mounted inside a registered Pool's directory tree.
func PoolPathsOverlap(left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	return left == right || strings.HasPrefix(left, strings.TrimSuffix(right, "/")+"/") || strings.HasPrefix(right, strings.TrimSuffix(left, "/")+"/")
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
