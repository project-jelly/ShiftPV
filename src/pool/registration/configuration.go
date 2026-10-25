package registration

import (
	"path/filepath"

	"k8s.io/apimachinery/pkg/util/validation"
)

type Configuration struct {
	NodeName, PoolGroup, MountPath, MountPolicy, CapacityPolicy, CapacityLimit string
}

func ValidateConfiguration(c Configuration) Check {
	if check := ValidateTopologyConfiguration(c); !check.OK {
		return check
	}
	if _, err := ParseLimitBytes(c.CapacityLimit); err != nil {
		return failed("PoolCapacityLimitInvalid", "capacity.limit must be an exact positive int64 byte count: "+err.Error())
	}
	return Check{Known: true, OK: true}
}

// ValidateTopologyConfiguration validates registered node scope without
// applying mutable reservation limits or readiness observations.
func ValidateTopologyConfiguration(c Configuration) Check {
	if check := ValidateBackingConfiguration(c); !check.OK {
		return check
	}
	if len(validation.IsDNS1123Label(c.PoolGroup)) != 0 {
		return failed("PoolConfigurationInvalid", "poolGroup must be a DNS-1123 label")
	}
	return Check{Known: true, OK: true}
}

// ValidateBackingConfiguration checks the configuration needed to safely
// observe storage, even when placement settings no longer admit new work.
func ValidateBackingConfiguration(c Configuration) Check {
	if c.NodeName == "" || !filepath.IsAbs(c.MountPath) || filepath.Clean(c.MountPath) == "/" ||
		(c.MountPolicy != "" && c.MountPolicy != "RequireMountPoint") ||
		(c.CapacityPolicy != "" && c.CapacityPolicy != FixedBlock) {
		return failed("PoolConfigurationInvalid", "invalid nodeName, mountPath, mountPolicy, or capacityPolicy")
	}
	return Check{Known: true, OK: true}
}
