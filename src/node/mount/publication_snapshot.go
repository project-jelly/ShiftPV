//go:build linux || darwin

package mount

import (
	"fmt"
	"path/filepath"
	"strings"

	mountutils "k8s.io/mount-utils"
)

// PublicationSnapshot is one inventory scan's mount table. Publish/unpublish
// continue to use live mount reads under their volume lock.
type PublicationSnapshot struct {
	mounts []mountutils.MountInfo
}

func (b *Binder) PublicationSnapshot() (*PublicationSnapshot, error) {
	mounts, err := mountutils.ParseMountInfo(b.MountInfoPath)
	if err != nil {
		return nil, fmt.Errorf("read publication mount table: %w", err)
	}
	return &PublicationSnapshot{mounts: mounts}, nil
}

func (s *PublicationSnapshot) HasPublishedTarget(source, targetRoot string) (bool, error) {
	exists, err := mountutils.PathExists(source)
	if !exists || mountutils.IsCorruptedMnt(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect publication source: %w", err)
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return false, err
	}
	// Match mount-utils SearchMountPoints: the last covering mount wins, even
	// when a later mount hides an earlier one. Device plus filesystem root
	// identifies bind references; comparing source path strings is insufficient.
	for i := len(s.mounts) - 1; i >= 0; i-- {
		parent := s.mounts[i]
		if source != parent.MountPoint && !mountutils.PathWithinBase(source, parent.MountPoint) {
			continue
		}
		root := filepath.Join(parent.Root, strings.TrimPrefix(source, parent.MountPoint))
		for _, ref := range s.mounts {
			if ref.ID != parent.ID && ref.Major == parent.Major && ref.Minor == parent.Minor && ref.Root == root && ValidateTarget(targetRoot, ref.MountPoint) == nil {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("publication source %q has no covering mount", source)
}
