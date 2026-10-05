package readiness

import (
	"fmt"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

type AllocationInspector interface {
	InspectCapacityUnit(volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check)
}

func (p *Probe) InspectCapacityUnit(pool volumeapi.Pool) (*volumeapi.PoolCapacityUnit, Check) {
	path, err := hostPath(p.HostRoot, pool.MountPath)
	if err != nil {
		return nil, capacityFailure("CapacityPathInvalid", err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return nil, capacityFailure("CapacityPathInvalid", fmt.Errorf("fixed capacity Pool path must exist without symlink components"))
	}
	entry, err := p.capacityMount(path)
	if err != nil {
		return nil, capacityFailure("CapacityMountUnavailable", err)
	}
	if entry.FsType != "ext4" && entry.FsType != "xfs" {
		return nil, capacityFailure("CapacityBackendUnsupported", fmt.Errorf("filesystem %q does not prove a fixed exclusive block allocation", entry.FsType))
	}
	if p.allocations == nil {
		return nil, capacityFailure("CapacityInspectorUnavailable", fmt.Errorf("block allocation inspector is unavailable"))
	}
	device := fmt.Sprintf("%d:%d", entry.Major, entry.Minor)
	extents, err := p.allocations.Resolve(device)
	if err != nil {
		return nil, capacityFailure("CapacityAllocationUnproven", err)
	}
	unit := &volumeapi.PoolCapacityUnit{Device: device, Source: entry.Source, Filesystem: entry.FsType, Extents: extents}
	if err := unit.Validate(); err != nil {
		return nil, capacityFailure("CapacityAllocationUnproven", err)
	}
	if pool.Status.CapacityUnit != nil && !volumeapi.SameCapacityUnit(unit, pool.Status.CapacityUnit) {
		return unit, capacityFailure("CapacityIdentityChanged", fmt.Errorf("filesystem backing allocation no longer matches the Pool's recorded identity"))
	}
	return unit, Check{OK: true, Known: true, Reason: "CapacityAllocationVerified", Message: "filesystem has a supported fixed block allocation"}
}

func (p *Probe) capacityMount(path string) (*mountutils.MountInfo, error) {
	if p.listMounts == nil {
		return nil, fmt.Errorf("mount table reader is unavailable")
	}
	mounts, err := p.listMounts()
	if err != nil {
		return nil, err
	}
	var selected *mountutils.MountInfo
	ambiguous := false
	for i := range mounts {
		point := filepath.Clean(unescapeMountPath(mounts[i].MountPoint))
		if point != path && !strings.HasPrefix(path, strings.TrimSuffix(point, "/")+"/") {
			continue
		}
		if selected == nil || len(point) > len(unescapeMountPath(selected.MountPoint)) {
			selected = &mounts[i]
			ambiguous = false
		} else if point == unescapeMountPath(selected.MountPoint) {
			ambiguous = true
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("capacity mount is missing")
	}
	if ambiguous {
		return nil, fmt.Errorf("capacity mount is ambiguous")
	}
	return selected, nil
}

func capacityFailure(reason string, err error) Check {
	return Check{Known: true, Reason: reason, Message: err.Error()}
}

// VerifyHostPool adds a live allocation check where the node's existing host
// sysfs and device-mapper access are available. Helpers retain the bound-mount
// check; they do not receive host device access for inspection.
func VerifyHostPool(hostRoot string, pool volumeapi.Pool) error {
	path, err := hostPath(hostRoot, pool.MountPath)
	if err != nil {
		return err
	}
	if err := VerifyMountedPath(path, pool); err != nil {
		return err
	}
	return NewProbe(hostRoot).VerifyCapacityUnit(pool)
}

func (p *Probe) VerifyCapacityUnit(pool volumeapi.Pool) error {
	if pool.CapacityPolicy == "" {
		return nil
	}
	if pool.CapacityPolicy != volumeapi.PoolCapacityPolicyFixedBlock || pool.Status.CapacityUnit.Validate() != nil {
		return fmt.Errorf("Pool has no supported anchored capacity allocation")
	}
	_, check := p.InspectCapacityUnit(pool)
	if !check.OK {
		return fmt.Errorf("verify live Pool allocation: %s: %s", check.Reason, check.Message)
	}
	return nil
}

// capacityIsolation inspects all local backing allocations before any writable
// probe or inventory scan. Unsupported peers never become additional capacity.
func (r *Reconciler) capacityIsolation(pools []volumeapi.Pool) map[string]allocationObservation {
	local := []volumeapi.Pool{}
	for _, p := range pools {
		if p.NodeName == r.NodeName {
			local = append(local, p)
		}
	}
	results := make(map[string]allocationObservation, len(local))
	inspector, configured := r.Inspector.(AllocationInspector)
	for _, p := range local {
		if p.CapacityPolicy == "" && (len(local) == 1 || p.DeletionTimestamp != nil) {
			continue
		}
		if p.CapacityPolicy != volumeapi.PoolCapacityPolicyFixedBlock {
			results[p.UID] = allocationObservation{check: capacityFailure("CapacityIsolationRequired", fmt.Errorf("multiple Pools on one node require capacityPolicy FixedBlock"))}
		} else if !configured {
			results[p.UID] = allocationObservation{check: capacityFailure("CapacityInspectorUnavailable", fmt.Errorf("block allocation inspector is unavailable"))}
		} else {
			unit, check := inspector.InspectCapacityUnit(p)
			results[p.UID] = allocationObservation{unit: unit, check: check}
		}
	}
	for i, p := range local {
		for _, peer := range local[i+1:] {
			if p.DeletionTimestamp == nil && peer.DeletionTimestamp == nil {
				compareAllocations(p, peer, results)
			}
		}
	}
	return results
}

type allocationObservation struct {
	unit  *volumeapi.PoolCapacityUnit
	check Check
}

func compareAllocations(left, right volumeapi.Pool, results map[string]allocationObservation) {
	a, b := results[left.UID], results[right.UID]
	leftUnit, rightUnit := a.unit, b.unit
	reason, message := "", ""
	if volumeapi.PoolPathsOverlap(left.MountPath, right.MountPath) {
		reason, message = "CapacityPathsOverlap", "Pool paths overlap or are nested"
	} else if leftUnit == nil || rightUnit == nil {
		reason, message = "CapacityPeerUnproven", "a registered peer has no fixed capacity evidence"
	} else if overlap, err := capacityunit.Overlap(leftUnit.Extents, rightUnit.Extents); err != nil || overlap {
		reason, message = "CapacityBackingOverlap", "registered Pools have overlapping or unproven backing allocations"
	}
	if reason == "" {
		return
	}
	for _, p := range []volumeapi.Pool{left, right} {
		observed := results[p.UID]
		if !observed.check.OK && observed.check.Reason != "" && reason == "CapacityPeerUnproven" {
			continue
		}
		observed.check = Check{Known: true, Reason: reason, Message: message}
		results[p.UID] = observed
	}
}

func verifyCapacityMountedPath(path string, pool volumeapi.Pool) error {
	if pool.CapacityPolicy == "" {
		return nil
	}
	if pool.CapacityPolicy != volumeapi.PoolCapacityPolicyFixedBlock {
		return fmt.Errorf("unsupported capacity policy")
	}
	if err := pool.Status.CapacityUnit.Validate(); err != nil {
		return err
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, volumeapi.PoolConditionCapacityIndependent)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != pool.Generation {
		return fmt.Errorf("Pool capacity allocation lacks a current successful observation")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
		return fmt.Errorf("Pool root must be an absolute non-root path")
	}
	if err := inspectDirectory(path); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != filepath.Clean(path) {
		return fmt.Errorf("fixed capacity Pool path must have no symlink components")
	}
	entry, err := NewProbe("/").capacityMount(filepath.Clean(path))
	if err != nil {
		return err
	}
	unit := pool.Status.CapacityUnit
	if fmt.Sprintf("%d:%d", entry.Major, entry.Minor) != unit.Device || entry.Source != unit.Source || entry.FsType != unit.Filesystem {
		return fmt.Errorf("Pool filesystem no longer matches its fixed capacity identity")
	}
	return nil
}
