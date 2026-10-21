package uninstall

import (
	"fmt"
	"path/filepath"
	"slices"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// preservedPoolUIDs selects exact retired registrations that remain as durable
// identity and path reservations after driver removal. It grants neither Pool
// deletion nor filesystem absence; their protection finalizers must remain.
func (c *Checker) preservedPoolUIDs(pools []volumeapi.Pool) (map[string]bool, error) {
	preserved := make(map[string]bool, len(c.PreserveRetiredPoolUIDs))
	for _, uid := range c.PreserveRetiredPoolUIDs {
		if !volume.ValidIdentityToken(uid) {
			return nil, fmt.Errorf("preserved retired Pool UID %q is invalid", uid)
		}
		var matched *volumeapi.Pool
		for index := range pools {
			if pools[index].UID != uid {
				continue
			}
			if matched != nil {
				return nil, fmt.Errorf("preserved retired Pool UID %q is ambiguous", uid)
			}
			matched = &pools[index]
		}
		if matched == nil {
			return nil, fmt.Errorf("preserved retired Pool UID %q was not found", uid)
		}
		pool := *matched
		if !pool.RegistrationRetired() || !pool.RegistrationApproved() ||
			!slices.Contains(pool.Finalizers, PoolProtectionFinalizer) ||
			!volume.ValidObjectName(pool.Name) || !volume.ValidObjectName(pool.NodeName) ||
			!filepath.IsAbs(pool.MountPath) || filepath.Clean(pool.MountPath) != pool.MountPath || pool.MountPath == "/" {
			return nil, fmt.Errorf("Pool %q UID %q is not an approved protected retirement", pool.Name, uid)
		}
		preserved[uid] = true
	}
	return preserved, nil
}
