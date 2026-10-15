package capacity

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

// MeasurementEvidence hashes registration and anchored filesystem evidence.
// Probe timestamps are excluded so routine status refreshes preserve the digest.
func MeasurementEvidence(pool volumeapi.Pool) (string, error) {
	if pool.Name == "" || pool.UID == "" || pool.NodeName == "" || !filepath.IsAbs(pool.MountPath) || filepath.Clean(pool.MountPath) == "/" {
		return "", fmt.Errorf("measurement requires an exact Pool registration")
	}
	if pool.CapacityPolicy != "" {
		if pool.CapacityPolicy != volumeapi.PoolCapacityPolicyFixedBlock || pool.Status.CapacityUnit.Validate() != nil {
			return "", fmt.Errorf("measurement requires supported capacity evidence")
		}
	}
	if pool.MountPolicy != "" && (pool.MountPolicy != volumeapi.PoolMountPolicyRequireMountPoint || pool.Status.MountIdentity == nil) {
		return "", fmt.Errorf("measurement requires supported mount evidence")
	}
	evidence := struct {
		Name, UID, NodeName, MountPath, CapacityPolicy, MountPolicy string
		Generation                                                  int64
		CapacityUnit                                                *volumeapi.PoolCapacityUnit
		MountIdentity                                               *volumeapi.PoolMountIdentity
	}{pool.Name, pool.UID, pool.NodeName, pool.MountPath, pool.CapacityPolicy, pool.MountPolicy, pool.Generation, pool.Status.CapacityUnit, pool.Status.MountIdentity}
	data, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// UsageConfigurationEvidence binds a quiesced-copy measurement to its Pool
// configuration and backing. A scan generation or observation refresh does not
// change that configuration; placement group and reservation limit edits do.
// MeasurementEvidence remains the generation-bound wire contract for helpers.
func UsageConfigurationEvidence(pool volumeapi.Pool) (string, error) {
	if _, err := MeasurementEvidence(pool); err != nil {
		return "", err
	}
	evidence := struct {
		Name, UID, NodeName, MountPath, PoolGroup, CapacityLimit, CapacityPolicy, MountPolicy string
		CapacityUnit                                                                          *volumeapi.PoolCapacityUnit
		MountIdentity                                                                         *volumeapi.PoolMountIdentity
	}{pool.Name, pool.UID, pool.NodeName, pool.MountPath, pool.PoolGroup, pool.CapacityLimit,
		pool.CapacityPolicy, pool.MountPolicy, pool.Status.CapacityUnit, pool.Status.MountIdentity}
	data, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
