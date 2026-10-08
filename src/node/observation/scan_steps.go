//go:build linux || darwin

package observation

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// configurationInvalid repeats the fail-closed precondition of an inventory
// scan: a scanner that cannot name an installation, a publication, an absolute
// host or target root, a pool, or a page budget within 1..256 observes nothing.
func (s *Scanner) configurationInvalid(pool volumeapi.Pool) bool {
	return s == nil || s.Installation == nil || s.Publications == nil || !filepath.IsAbs(s.HostRoot) ||
		!filepath.IsAbs(s.TargetRoot) || pool.UID == "" || s.Limit < 1 || s.Limit > 256
}

// observeRecorded pages the recorded placements into result until the limit is
// reached, then probes one more page to learn whether anything was left behind.
// It reports false when a read failed and result already carries the message.
func (s *Scanner) observeRecorded(ctx context.Context, inventory *ownership.Inventory, pool volumeapi.Pool,
	root, installationID string, known map[string]struct{}, result *volumeapi.PoolInventory) bool {
	done := false
	for !done && len(result.Copies) < s.Limit {
		page, pageDone, pageErr := inventory.Page(ctx, min(64, s.Limit-len(result.Copies)))
		if pageErr != nil {
			result.Message = "InventoryReadFailed: " + pageErr.Error()
			return false
		}
		for _, item := range page {
			result.Copies = append(result.Copies, s.observeItem(item, pool, root, installationID))
			if item.Identity != nil && item.Present {
				known[physicalKey(*item.Identity)] = struct{}{}
			}
		}
		done = pageDone
	}
	for !done && len(result.Copies) == s.Limit {
		page, pageDone, pageErr := inventory.Page(ctx, 1)
		if pageErr != nil {
			result.Message = "InventoryReadFailed: " + pageErr.Error()
			return false
		}
		if len(page) > 0 {
			break
		}
		done = pageDone
	}
	result.Truncated = !done
	return true
}

// observeItem turns one recorded placement into a copy observation: a placement
// registered to another installation, pool or node loses its identity, and only
// an intact present copy is asked whether it is still published.
func (s *Scanner) observeItem(item ownership.Observation, pool volumeapi.Pool, root, installationID string) volumeapi.CopyObservation {
	observation := volumeapi.CopyObservation{Marker: item.Marker, Identity: item.Identity, Present: item.Present, Problem: item.Problem}
	if item.Identity != nil && (item.Identity.InstallationID != installationID || item.Identity.PoolName != pool.Name ||
		item.Identity.PoolUID != pool.UID || item.Identity.NodeName != pool.NodeName) {
		observation.Identity = nil
		observation.Problem = "PoolIdentityMismatch"
	}
	if observation.Identity != nil && observation.Present && observation.Problem == "" {
		source := filepath.Join(root, filepath.FromSlash(physicalKey(*observation.Identity)))
		published, err := s.Publications.HasPublishedTarget(source, s.TargetRoot)
		observation.Published = published
		if err != nil {
			observation.Problem = "PublicationObservationFailed: " + err.Error()
		}
	}
	return observation
}

// Physical paths are collected before placements: native creation writes its
// placement before exposing the serving directory. Paths moved or removed since
// collection are rechecked; every remaining unrecorded path stays a problem.
func (s *Scanner) observeUnrecorded(ctx context.Context, root string, paths []volumeapi.CopyObservation,
	known map[string]struct{}, result *volumeapi.PoolInventory, pathType func(string, string) (bool, bool, error)) bool {
	for _, item := range paths {
		if err := ctx.Err(); err != nil {
			result.Message = "PhysicalInventoryFailed: " + err.Error()
			return false
		}
		key := strings.TrimPrefix(item.Marker, "path:")
		if _, found := known[key]; found {
			continue
		}
		present, directory, err := pathType(root, key)
		if err != nil {
			result.Message = "PhysicalInventoryFailed: " + err.Error()
			return false
		}
		if !present {
			if !completedCreationStage(key, result.Copies) {
				result.Message = "PhysicalInventoryChanged"
				return false
			}
			continue
		}
		if len(result.Copies) == s.Limit {
			result.Truncated = true
			break
		}
		item.Problem = "UnrecordedPath"
		if !directory {
			item.Problem = "UnexpectedPathType"
		}
		result.Copies = append(result.Copies, item)
	}
	return true
}

// Only a verified present serving copy explains its vanished creation stage.
// Other disappearing paths require a fresh scan rather than an absence proof.
func completedCreationStage(key string, copies []volumeapi.CopyObservation) bool {
	for _, item := range copies {
		if item.Identity == nil || !item.Present || item.Problem != "" {
			continue
		}
		if item.Identity.Role == volume.RoleServing && key == ".shiftpv/incoming/create-"+item.Identity.CopyID {
			return true
		}
	}
	return false
}

// Walk each parent without following symlinks, including .shiftpv. A missing
// collected path is not a copy-absence receipt or permission to delete data.
func physicalPathType(root, key string) (bool, bool, error) {
	parts := strings.Split(key, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false, false, ownership.ErrIdentity
		}
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, false, err
	}
	defer func() { _ = unix.Close(fd) }()
	for _, part := range parts[:len(parts)-1] {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				return false, false, nil
			}
			return false, false, err
		}
		_ = unix.Close(fd)
		fd = next
	}
	var stat unix.Stat_t
	if err := unix.Fstatat(fd, parts[len(parts)-1], &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, false, nil
		}
		return false, false, err
	}
	return true, stat.Mode&unix.S_IFMT == unix.S_IFDIR, nil
}

// copyProblemMessage summarizes an otherwise clean inventory: a single troubled
// copy is enough to withhold validity from the whole pool observation.
func copyProblemMessage(copies []volumeapi.CopyObservation) string {
	for _, observed := range copies {
		if observed.Problem != "" {
			return "CopyObservationProblem"
		}
	}
	return ""
}
