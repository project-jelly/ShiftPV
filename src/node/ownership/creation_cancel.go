//go:build linux || darwin

package ownership

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/project-jelly/ShiftPV/src/volume"
	"golang.org/x/sys/unix"
)

// CancelCreation retires an approved initial copy without completing creation.
// Its caller must require CreationCancelled journal authority; older normal
// reclaim readers do not understand a create-stage inode. All paths are checked
// under the same lock as PrepareServing, including the no-effects case.
func CancelCreation(ctx context.Context, root string, target volume.CopyIdentity, operationID string, authority func(context.Context, bool) error) (Receipt, string, error) {
	return cancelCreation(ctx, root, target, operationID, authority, preflightPurge, purgeRetired)
}

func cancelCreation(ctx context.Context, root string, target volume.CopyIdentity, operationID string, authority func(context.Context, bool) error,
	preflight func(*Store) error, purge func(context.Context, *Store, localIntent) error) (Receipt, string, error) {
	if authority == nil || !volume.IsCreationCancellation(target, operationID) {
		return Receipt{}, "", ErrIdentity
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, "", err
	}
	if err := authority(ctx, false); err != nil {
		return Receipt{}, "", err
	}
	s, err := Open(root, PoolIdentity{InstallationID: target.InstallationID, PoolUID: target.PoolUID})
	if err != nil {
		return Receipt{}, "", err
	}
	defer s.Close()
	lock, err := s.Acquire(ctx, target.VolumeID)
	if err != nil {
		return Receipt{}, "", err
	}
	defer lock.Close()
	if err := authority(ctx, false); err != nil {
		return Receipt{}, "", err
	}
	if done, receipt, digest, err := s.replayCreationCancellation(ctx, target, operationID, authority); done {
		return receipt, digest, err
	}
	intent, source, err := s.prepareCreationCancellation(target, operationID)
	if err != nil {
		return Receipt{}, "", err
	}
	if err := authority(ctx, source == 2 || source == -1); err != nil {
		return Receipt{}, "", err
	}
	if intent.Inode != 0 {
		if err := preflight(s); err != nil {
			return Receipt{}, "", err
		}
		if err := s.retireCreation(target, intent, source); err != nil {
			return Receipt{}, "", err
		}
		if err := authority(ctx, true); err != nil {
			return Receipt{}, "", err
		}
		if err := purge(ctx, s, intent); err != nil {
			return Receipt{}, "", err
		}
	}
	if err := authority(ctx, true); err != nil {
		return Receipt{}, "", err
	}
	if err := s.verifyCreationAbsent(target); err != nil {
		return Receipt{}, "", err
	}
	return s.settleReclaim(target, operationID, intent)
}

type creationPath struct {
	parent          *os.File
	directory, name string
}

// Indices are local to this ordered list: serving, create-stage, retired.
func (s *Store) creationPaths(target volume.CopyIdentity) []creationPath {
	return []creationPath{{s.root, "volumes", target.VolumeID}, {s.control, "incoming", "create-" + target.CopyID}, {s.control, "retired", target.CopyID}}
}

func (p creationPath) observe(target volume.CopyIdentity, requireEmpty bool) (placement, bool, error) {
	parent, err := unix.Openat(int(p.parent.Fd()), p.directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return placement{}, false, nil
	}
	if err != nil {
		return placement{}, false, creationPathError(err)
	}
	defer unix.Close(parent)
	fd, err := unix.Openat(parent, p.name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return placement{}, false, nil
	}
	if err != nil {
		return placement{}, false, creationPathError(err)
	}
	file := os.NewFile(uintptr(fd), "creation-copy")
	defer file.Close()
	if requireEmpty {
		names, err := file.Readdirnames(1)
		if len(names) != 0 || err != io.EOF {
			return placement{}, false, fmt.Errorf("%w: unplaced creation stage is not provably empty", ErrNeedsReview)
		}
	}
	record, err := placementFor(fd, target)
	return record, err == nil, err
}

func creationPathError(err error) error {
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		return errors.Join(ErrNeedsReview, err)
	}
	return err
}

func (s *Store) prepareCreationCancellation(target volume.CopyIdentity, operationID string) (localIntent, int, error) {
	intent, exists, err := s.localIntent(target, operationID)
	if err != nil {
		return localIntent{}, -1, err
	}
	marker, err := s.copyMarkerExists(target)
	if err != nil {
		return localIntent{}, -1, err
	}
	placed, placeErr := s.readPlacement(target.CopyID)
	if placeErr != nil && !errors.Is(placeErr, os.ErrNotExist) {
		return localIntent{}, -1, placeErr
	}
	hasPlacement := placeErr == nil
	if hasPlacement && (!marker || placed.Identity != target) {
		return localIntent{}, -1, ErrIdentity
	}
	source, observed, err := s.observeCreationSource(target, !hasPlacement && !exists)
	if err != nil {
		return localIntent{}, -1, err
	}
	if exists {
		return intent, source, validateCreationResume(intent, marker, source, observed, hasPlacement, placed)
	}
	if err := validateCreationSource(marker, source, observed, hasPlacement, placed); err != nil {
		return localIntent{}, -1, err
	}
	intent = localIntent{OperationID: operationID, Target: target, Device: observed.Device, Inode: observed.Inode}
	if err := s.ensureMarker(operationMarker("cleanup", operationID), intent); err != nil {
		return localIntent{}, -1, err
	}
	return intent, source, nil
}

func validateCreationSource(marker bool, source int, observed placement, hasPlacement bool, placed placement) error {
	if source == 2 || source == 0 && !hasPlacement || source == -1 && hasPlacement || source != -1 && !marker {
		return fmt.Errorf("%w: creation location has no matching intent/placement", ErrNeedsReview)
	}
	if hasPlacement && observed != placed {
		return ErrIdentity
	}
	return nil
}

func validateCreationResume(intent localIntent, marker bool, source int, observed placement, hasPlacement bool, placed placement) error {
	if (intent.Device == 0) != (intent.Inode == 0) || (intent.Inode != 0 && !marker) ||
		(source != -1 && (intent.Device != observed.Device || intent.Inode != observed.Inode)) ||
		(hasPlacement && (intent.Device != placed.Device || intent.Inode != placed.Inode)) {
		return ErrIdentity
	}
	return nil
}

func (s *Store) observeCreationSource(target volume.CopyIdentity, emptyStage bool) (int, placement, error) {
	if absent, err := metadataPathAbsent(int(s.control.Fd()), "incoming", target.CopyID); err != nil || !absent {
		return -1, placement{}, errors.Join(err, ErrNeedsReview)
	}
	source := -1
	var observed placement
	for i, path := range s.creationPaths(target) {
		record, present, err := path.observe(target, i == 1 && emptyStage)
		if err != nil {
			return -1, placement{}, err
		}
		if present {
			if source != -1 {
				return -1, placement{}, fmt.Errorf("%w: multiple creation locations exist", ErrNeedsReview)
			}
			source, observed = i, record
		}
	}
	return source, observed, nil
}

func (s *Store) retireCreation(target volume.CopyIdentity, intent localIntent, source int) error {
	if source == -1 || source == 2 {
		// No source remains after an already journaled retire/purge. Ensure the
		// retired parent exists for the existing purge implementation.
		fd, err := s.openOrCreateDirectory(int(s.control.Fd()), "retired", 0700)
		if err == nil {
			return unix.Close(fd)
		}
		return err
	}
	path := s.creationPaths(target)[source]
	parent, err := unix.Openat(int(path.parent.Fd()), path.directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	fd, err := openIdentityDirectory(parent, path.name, intent)
	if err != nil {
		return err
	}
	unix.Close(fd)
	retired, err := s.openOrCreateDirectory(int(s.control.Fd()), "retired", 0700)
	if err != nil {
		return err
	}
	defer unix.Close(retired)
	if err := renameDirectory(parent, path.name, retired, target.CopyID); err != nil {
		return err
	}
	return errors.Join(unix.Fsync(parent), unix.Fsync(retired), s.control.Sync(), s.root.Sync())
}

func (s *Store) verifyCreationAbsent(target volume.CopyIdentity) error {
	source, _, err := s.observeCreationSource(target, false)
	if err != nil {
		return err
	}
	if source != -1 {
		return fmt.Errorf("%w: a creation location still exists", ErrNeedsReview)
	}
	return nil
}

func (s *Store) replayCreationCancellation(ctx context.Context, target volume.CopyIdentity, operationID string, authority func(context.Context, bool) error) (bool, Receipt, string, error) {
	var receipt Receipt
	err := s.readMarker(operationMarker("receipt", operationID), &receipt)
	if errors.Is(err, os.ErrNotExist) {
		return false, Receipt{}, "", nil
	}
	if err != nil {
		return true, Receipt{}, "", err
	}
	intent, exists, err := s.localIntent(target, operationID)
	if err != nil || !exists || receipt.OperationID != operationID || receipt.Target != target || !receipt.Retired || !receipt.Purged ||
		receipt.Device != intent.Device || receipt.Inode != intent.Inode || (receipt.Device == 0) != (receipt.Inode == 0) {
		return true, Receipt{}, "", errors.Join(err, ErrIdentity)
	}
	if err := s.verifyCreationAbsent(target); err != nil {
		return true, Receipt{}, "", err
	}
	return s.replayReclaimReceipt(ctx, target, operationID, authority)
}
