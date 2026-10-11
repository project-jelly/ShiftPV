//go:build linux || darwin

package ownership

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/project-jelly/ShiftPV/src/volume"
	"golang.org/x/sys/unix"
)

// ReleaseEmptyPool ends the exact empty Pool's identity and lock namespace.
// Copy data and copy metadata keep the Pool intact; receipts are retained.
func ReleaseEmptyPool(ctx context.Context, root string, identity PoolIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := OpenExisting(root, identity)
	if errors.Is(err, os.ErrNotExist) {
		empty, inspectErr := managedPoolDataEmpty(root)
		if inspectErr != nil {
			return inspectErr
		}
		if !empty {
			return ErrNeedsReview
		}
		return syncReleasedIdentity(root)
	}
	if err != nil {
		return err
	}
	defer store.Close()

	lock, err := store.openControl("identity.lock", unix.O_RDWR|unix.O_CREAT, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ErrBusy
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.checkMarker("pool.json", identity); err != nil {
		return err
	}
	empty, err := store.managedDataEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return ErrNeedsReview
	}
	return store.releaseLockNamespace(ctx)
}

func (store *Store) releaseLockNamespace(ctx context.Context) error {
	locks, err := store.lockVolumeFiles(ctx)
	if err != nil {
		return err
	}
	defer func() {
		for _, file := range locks {
			file.Close()
		}
	}()
	if err := unix.Unlinkat(int(store.control.Fd()), "pool.json", 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	// A crash must not persist removed locks while resurrecting their identity.
	if err := store.control.Sync(); err != nil {
		return err
	}
	for _, file := range locks {
		if err := unix.Unlinkat(int(store.control.Fd()), file.Name(), 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return store.control.Sync()
}

func syncReleasedIdentity(root string) error {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	controlFD, err := unix.Openat(rootFD, ".shiftpv", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return unix.Fsync(rootFD)
	}
	if err != nil {
		return err
	}
	defer unix.Close(controlFD)
	return unix.Fsync(controlFD)
}

// Only a controller-approved empty Pool may end its volume lock namespace.
// identity.lock stays in place across Pool re-registration.
func (s *Store) lockVolumeFiles(ctx context.Context) (files []*os.File, result error) {
	defer func() {
		if result != nil {
			for _, file := range files {
				file.Close()
			}
			files = nil
		}
	}()
	for {
		names, readErr := s.control.Readdirnames(128)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return files, err
			}
			if !strings.HasPrefix(name, "lock-") || volume.ValidateID(strings.TrimPrefix(name, "lock-")) != nil {
				continue
			}
			file, err := s.openControl(name, unix.O_RDONLY, 0)
			if err != nil {
				return files, err
			}
			files = append(files, file)
			info, err := file.Stat()
			if err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
				return files, ErrNeedsReview
			}
			if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
				return files, ErrBusy
			}
		}
		if errors.Is(readErr, io.EOF) {
			return files, nil
		}
		if readErr != nil {
			return files, readErr
		}
	}
}

func (s *Store) managedDataEmpty() (bool, error) {
	for _, candidate := range []struct {
		parent *os.File
		name   string
	}{
		{parent: s.root, name: "volumes"},
		{parent: s.control, name: "incoming"},
		{parent: s.control, name: "retired"},
	} {
		empty, err := directoryEmptyAt(int(candidate.parent.Fd()), candidate.name)
		if err != nil || !empty {
			return empty, err
		}
	}
	return readDirectoryEmpty(s.placements)
}

func managedPoolDataEmpty(root string) (bool, error) {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false, err
	}
	defer unix.Close(rootFD)
	empty, err := directoryEmptyAt(rootFD, "volumes")
	if err != nil || !empty {
		return empty, err
	}
	controlFD, err := unix.Openat(rootFD, ".shiftpv", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer unix.Close(controlFD)
	for _, name := range []string{"placements", "incoming", "retired"} {
		empty, err := directoryEmptyAt(controlFD, name)
		if err != nil || !empty {
			return empty, err
		}
	}
	return true, nil
}

func directoryEmptyAt(parent int, name string) (bool, error) {
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	directory := os.NewFile(uintptr(fd), name)
	defer directory.Close()
	return readDirectoryEmpty(directory)
}

func readDirectoryEmpty(directory *os.File) (bool, error) {
	names, err := directory.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return len(names) == 0, nil
	}
	return false, err
}
