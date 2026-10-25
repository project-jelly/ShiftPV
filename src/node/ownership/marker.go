//go:build linux || darwin

package ownership

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/uuid"

	"github.com/project-jelly/ShiftPV/src/volume"
)

var temporaryMarkerName = regexp.MustCompile(`^pending-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// removeAbandonedPlacementMarkers runs only under identity.lock after checking
// pool.json. All marker writers hold that lock through rename, so a generated
// temporary that remains here cannot belong to an active writer. Its content
// may be partial; it never grants authority and is not a placement receipt.
func (s *Store) removeAbandonedPlacementMarkers(ctx context.Context) (result error) {
	fd, err := unix.Openat(int(s.placements.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(fd), "shiftpv-placement-temporaries")
	defer directory.Close()
	changed := false
	defer func() {
		if changed {
			result = errors.Join(result, s.placements.Sync())
		}
	}()
	for {
		names, readErr := directory.Readdirnames(128)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !temporaryMarkerName.MatchString(name) {
				continue
			}
			if err := s.removeAbandonedPlacementMarker(name); err != nil {
				return err
			}
			changed = true
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *Store) removeAbandonedPlacementMarker(name string) error {
	file, err := openMarker(s.placements, name, unix.O_RDONLY, 0)
	if err != nil {
		return errors.Join(ErrNeedsReview, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Mode() != 0600 || info.Size() > 8192 {
		return ErrNeedsReview
	}
	return unix.Unlinkat(int(s.placements.Fd()), name, 0)
}

func (s *Store) ensureMarker(name string, value any) error {
	return s.ensureMarkerAt(s.control, name, value)
}

func (s *Store) ensurePlacementMarker(copyID string, value placement) error {
	return s.ensureMarkerAt(s.placements, placementMarker(copyID), value)
}

func (s *Store) ensureMarkerAt(directory *os.File, name string, value any) error {
	lock, err := s.openControl("identity.lock", unix.O_RDWR|unix.O_CREAT, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	if err := s.checkMarkerAt(directory, name, value); err == nil {
		return directory.Sync()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := canonical(value)
	if err != nil {
		return err
	}
	temporary := "pending-" + string(uuid.NewUUID())
	file, err := openMarker(directory, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(int(directory.Fd()), temporary, 0)
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return err
	}
	if err := unix.Renameat(int(directory.Fd()), temporary, int(directory.Fd()), name); err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		return err
	}
	return s.checkMarkerAt(directory, name, value)
}

func (s *Store) checkMarker(name string, expected any) error {
	return s.checkMarkerAt(s.control, name, expected)
}

func (s *Store) checkPlacementMarker(copyID string, expected placement) error {
	return s.checkMarkerAt(s.placements, placementMarker(copyID), expected)
}

func (s *Store) checkMarkerAt(directory *os.File, name string, expected any) error {
	file, err := openMarker(directory, name, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil {
		return err
	}
	want, err := canonical(expected)
	if err != nil {
		return err
	}
	if len(data) > 8192 || string(data) != string(want) {
		return ErrIdentity
	}
	return nil
}

func (s *Store) removeMarkerAt(directory *os.File, name string, expected any) error {
	lock, err := s.openControl("identity.lock", unix.O_RDWR|unix.O_CREAT, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	if err := s.checkMarkerAt(directory, name, expected); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := unix.Unlinkat(int(directory.Fd()), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return directory.Sync()
}

func (s *Store) removeCopyMetadata(identity volume.CopyIdentity, device, inode uint64) error {
	placed := placement{Identity: identity, Device: device, Inode: inode}
	if err := s.removeMarkerAt(s.placements, placementMarker(identity.CopyID), placed); err != nil {
		return err
	}
	return s.removeMarkerAt(s.control, copyMarker(identity.CopyID), identity)
}

func (s *Store) openControl(name string, flags int, mode uint32) (*os.File, error) {
	return openMarker(s.control, name, flags, mode)
}

func (s *Store) openPlacement(name string, flags int, mode uint32) (*os.File, error) {
	return openMarker(s.placements, name, flags, mode)
}

func openMarker(directory *os.File, name string, flags int, mode uint32) (*os.File, error) {
	if name == "" || strings.ContainsAny(name, "/\\\x00") {
		return nil, ErrIdentity
	}
	fd, err := unix.Openat(int(directory.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, mode)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		file.Close()
		return nil, ErrIdentity
	}
	return file, nil
}

func (s *Store) readPlacement(copyID string) (placement, error) {
	var envelope struct {
		Version int       `json:"version"`
		Value   placement `json:"value"`
	}
	file, err := s.openPlacement(placementMarker(copyID), unix.O_RDONLY, 0)
	if err != nil {
		return placement{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(data) > 8192 || json.Unmarshal(data, &envelope) != nil || envelope.Version != 1 {
		return placement{}, ErrIdentity
	}
	if err := s.checkPlacementMarker(copyID, envelope.Value); err != nil {
		return placement{}, err
	}
	return envelope.Value, nil
}

func canonical(value any) ([]byte, error) {
	return json.Marshal(struct {
		Version int `json:"version"`
		Value   any `json:"value"`
	}{Version: 1, Value: value})
}

func copyMarker(copyID string) string      { return "copy-" + copyID + ".json" }
func placementMarker(copyID string) string { return "placement-" + copyID + ".json" }

func operationMarker(prefix, operationID string) string {
	sum := sha256.Sum256([]byte(operationID))
	return prefix + "-" + hex.EncodeToString(sum[:16]) + ".json"
}
