//go:build linux || darwin

package observation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// verifyUnclaimedPool is read-only. Cancellation requires a never-approved
// FixedBlock registration, no storage metadata/data, and no publication. It
// never clears identity on an unsupported or changed filesystem.
func (s *Scanner) verifyUnclaimedPool(ctx context.Context, pool volumeapi.Pool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !pool.DeletingUnapprovedFixedBlock() || s.configurationInvalid(pool) || !pool.BackingConfigurationCheck().OK {
		return ownership.ErrIdentity
	}
	installationID, err := s.Installation.InstallationID(ctx)
	if err != nil {
		return err
	}
	if !volume.ValidIdentityToken(installationID) {
		return ownership.ErrIdentity
	}
	root := filepath.Join(s.HostRoot, strings.TrimPrefix(filepath.Clean(pool.MountPath), string(filepath.Separator)))
	if err := unclaimedPoolDataAbsent(ctx, root); err != nil {
		return err
	}
	publications := s.Publications
	if s.SnapshotPublications != nil {
		publications, err = s.SnapshotPublications()
		if err != nil {
			return err
		}
		if publications == nil {
			return fmt.Errorf("publication snapshot is unavailable")
		}
	}
	published, err := publications.HasPublishedTarget(root, s.TargetRoot)
	if err != nil {
		return err
	}
	if published {
		return fmt.Errorf("%w: Pool has a published target", ownership.ErrNeedsReview)
	}
	return ctx.Err()
}

func unclaimedPoolDataAbsent(ctx context.Context, root string) error {
	exists, err := unclaimedPathExists(root)
	if err != nil || !exists {
		return err
	}
	// Even an empty or foreign metadata directory may hide earlier ownership.
	// Such storage must use the anchored release path or operator review.
	present, _, err := physicalPathType(root, ".shiftpv")
	if err != nil {
		return err
	}
	if present {
		return fmt.Errorf("%w: Pool metadata exists", ownership.ErrNeedsReview)
	}
	paths, truncated, err := scanPhysical(ctx, root, nil, 1)
	if err != nil {
		return err
	}
	if truncated || len(paths) != 0 {
		return fmt.Errorf("%w: managed Pool data exists", ownership.ErrNeedsReview)
	}
	return nil
}

// A never-approved registration can outlive its test mount/path. Check the
// existing ancestors too: ENOENT through a symlink is not trustworthy absence.
func unclaimedPathExists(root string) (bool, error) {
	for path := root; ; path = filepath.Dir(path) {
		resolved, err := filepath.EvalSymlinks(path)
		if err == nil {
			if resolved != path {
				return false, fmt.Errorf("unclaimed Pool path has symlink components")
			}
			return path == root, nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(path) == path {
			return false, err
		}
	}
}
