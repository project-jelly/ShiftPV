//go:build linux || darwin

package ownership

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
	"golang.org/x/sys/unix"
)

// CollectMetadata drops only expired, completed reclaim records. Lock files
// remain stable for the lifetime of the Pool; live retry evidence is retained.
func CollectMetadata(ctx context.Context, root string, pool PoolIdentity, now time.Time, retention time.Duration, unused func(context.Context, volume.CopyIdentity) (bool, error)) (int, error) {
	if retention < time.Hour || unused == nil {
		return 0, ErrIdentity
	}
	s, err := OpenExisting(root, pool)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer s.Close()
	removed := 0
	for {
		names, readErr := s.control.Readdirnames(128)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return removed, err
			}
			if !strings.HasPrefix(name, "receipt-") || !strings.HasSuffix(name, ".json") {
				continue
			}
			receipt, valid := s.completedReclaim(name)
			if !valid {
				continue
			}
			if !s.expiredMarker(name, now, retention) {
				continue
			}
			done, err := s.collectReclaimRecord(ctx, pool, receipt, now, retention, unused)
			if err != nil {
				return removed, err
			}
			if done {
				removed++
			}
		}
		if errors.Is(readErr, io.EOF) {
			return removed, nil
		}
		if readErr != nil {
			return removed, readErr
		}
	}
}

func (s *Store) expiredMarker(name string, now time.Time, retention time.Duration) bool {
	f, err := s.openControl(name, unix.O_RDONLY, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	return err == nil && info.Mode().IsRegular() && !info.ModTime().After(now) && now.Sub(info.ModTime()) >= retention
}

func (s *Store) collectReclaimRecord(ctx context.Context, pool PoolIdentity, receipt Receipt, now time.Time, retention time.Duration, unused func(context.Context, volume.CopyIdentity) (bool, error)) (bool, error) {
	// Current-Pool operations use this same volume lock. Historical Pool records
	// cannot be replayed through the current identity and need no recreated lock.
	if receipt.Target.PoolUID == pool.PoolUID {
		lock, err := s.AcquireExisting(ctx, receipt.Target.VolumeID)
		if errors.Is(err, ErrBusy) || errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		defer lock.Close()
	}
	if !s.reclaimRecordExpired(receipt, now, retention) {
		return false, nil
	}
	if absent, err := s.reclaimMetadataAbsent(receipt); err != nil || !absent {
		return false, err
	}
	// API reads may wait. Keep the candidate volume serialized without holding
	// the Pool-wide marker lock across network I/O; recheck local proof below.
	allowed, err := unused(ctx, receipt.Target)
	if err != nil || !allowed {
		return false, err
	}
	return s.forgetExpiredReclaim(ctx, pool, receipt, now, retention)
}

func (s *Store) forgetExpiredReclaim(ctx context.Context, pool PoolIdentity, receipt Receipt, now time.Time, retention time.Duration) (bool, error) {
	lock, err := s.openControl("identity.lock", unix.O_RDWR|unix.O_CREAT, 0600)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return false, nil
		}
		return false, err
	}
	if err := s.checkMarker("pool.json", pool); err != nil {
		return false, err
	}
	if !s.reclaimRecordExpired(receipt, now, retention) {
		return false, nil
	}
	receiptName := operationMarker("receipt", receipt.OperationID)
	intentName := operationMarker("cleanup", receipt.OperationID)
	if absent, err := s.reclaimMetadataAbsent(receipt); err != nil || !absent {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, s.forgetReclaimRecord(receiptName, intentName)
}

func (s *Store) forgetReclaimRecord(receiptName, intentName string) error {
	// Remove the redundant intent first. The complete receipt remains replayable
	// if this sequence crashes, and a later pass resumes its removal.
	if err := unix.Unlinkat(int(s.control.Fd()), intentName, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err := s.control.Sync(); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(s.control.Fd()), receiptName, 0); err != nil {
		return err
	}
	return s.control.Sync()
}

func metadataPathAbsent(parent int, directory, name string) (bool, error) {
	fd, err := unix.Openat(parent, directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer unix.Close(fd)
	exists, err := pathExists(fd, name)
	return !exists, err
}

func (s *Store) reclaimMetadataAbsent(receipt Receipt) (bool, error) {
	// Absence and the canonical marker checks use directory FDs, never symlinks.
	if err := s.verifyAbsent(receipt.Target); err != nil {
		return false, nil
	}
	for _, path := range []struct {
		parent          *os.File
		directory, name string
	}{
		{s.root, "volumes", receipt.Target.VolumeID},
		{s.control, "incoming", receipt.Target.CopyID},
		{s.control, "incoming", "create-" + receipt.Target.CopyID},
		{s.control, "retired", receipt.Target.CopyID},
	} {
		if absent, err := metadataPathAbsent(int(path.parent.Fd()), path.directory, path.name); err != nil || !absent {
			return false, err
		}
	}
	for _, path := range []struct {
		parent *os.File
		name   string
	}{
		{s.control, copyMarker(receipt.Target.CopyID)}, {s.placements, placementMarker(receipt.Target.CopyID)},
	} {
		if exists, err := pathExists(int(path.parent.Fd()), path.name); err != nil || exists {
			return false, err
		}
	}
	return true, nil
}

func (s *Store) completedReclaim(name string) (Receipt, bool) {
	var r Receipt
	if s.readMarker(name, &r) != nil {
		return r, false
	}
	valid := r.Retired && r.Purged && r.Target.Validate() == nil && r.Target.InstallationID == s.pool.InstallationID &&
		volume.ValidIdentityToken(r.OperationID) && name == operationMarker("receipt", r.OperationID) && r.Device != 0 && r.Inode != 0
	return r, valid
}
func (s *Store) reclaimRecordExpired(receipt Receipt, now time.Time, retention time.Duration) bool {
	receiptName := operationMarker("receipt", receipt.OperationID)
	if s.checkMarker(receiptName, receipt) != nil || !s.expiredMarker(receiptName, now, retention) {
		return false
	}
	intentName := operationMarker("cleanup", receipt.OperationID)
	intent := localIntent{OperationID: receipt.OperationID, Target: receipt.Target, Device: receipt.Device, Inode: receipt.Inode}
	if err := s.checkMarker(intentName, intent); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false
	} else if err == nil && !s.expiredMarker(intentName, now, retention) {
		return false
	}
	return true
}
