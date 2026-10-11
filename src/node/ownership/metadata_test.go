//go:build linux || darwin

package ownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
	"golang.org/x/sys/unix"
)

func metadataFixture(t *testing.T) (string, PoolIdentity, Receipt, time.Time) {
	t.Helper()
	root := t.TempDir()
	target := testIdentity()
	pool := PoolIdentity{InstallationID: target.InstallationID, PoolUID: target.PoolUID}
	s, err := Open(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lock, err := s.Acquire(context.Background(), target.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	r := Receipt{OperationID: "delete-operation", Target: target, Device: 1, Inode: 2, Retired: true, Purged: true}
	for name, value := range map[string]any{operationMarker("receipt", r.OperationID): r, operationMarker("cleanup", r.OperationID): localIntent{OperationID: r.OperationID, Target: r.Target, Device: r.Device, Inode: r.Inode}} {
		if err := s.ensureMarker(name, value); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	old := now.Add(-8 * 24 * time.Hour)
	for _, kind := range []string{"receipt", "cleanup"} {
		if err := os.Chtimes(filepath.Join(root, ".shiftpv", operationMarker(kind, r.OperationID)), old, old); err != nil {
			t.Fatal(err)
		}
	}
	return root, pool, r, now
}

func TestMetadataGCRequiresClosedExpiredExactRecord(t *testing.T) {
	for _, name := range []string{"expired", "live reference", "API failure", "young receipt", "future receipt", "young intent", "receipt refreshed during authority", "malformed receipt", "malformed intent", "symlink receipt", "data reappeared", "marker remains", "busy lock", "partial GC", "cancelled authority", "incomplete receipt", "foreign installation"} {
		t.Run(name, func(t *testing.T) {
			root, pool, r, now := metadataFixture(t)
			receiptPath := filepath.Join(root, ".shiftpv", operationMarker("receipt", r.OperationID))
			intentPath := filepath.Join(root, ".shiftpv", operationMarker("cleanup", r.OperationID))
			unused := func(context.Context, volume.CopyIdentity) (bool, error) { return true, nil }
			var held *os.File
			switch name {
			case "live reference":
				unused = func(context.Context, volume.CopyIdentity) (bool, error) { return false, nil }
			case "API failure":
				unused = func(context.Context, volume.CopyIdentity) (bool, error) { return false, errors.New("API unavailable") }
			case "young receipt":
				os.Chtimes(receiptPath, now, now)
			case "future receipt":
				future := now.Add(time.Hour)
				os.Chtimes(receiptPath, future, future)
			case "young intent":
				os.Chtimes(intentPath, now, now)
			case "receipt refreshed during authority":
				unused = func(context.Context, volume.CopyIdentity) (bool, error) {
					return true, os.Chtimes(receiptPath, now, now)
				}
			case "malformed receipt":
				os.WriteFile(receiptPath, []byte("{}"), 0600)
			case "malformed intent":
				os.WriteFile(intentPath, []byte("{}"), 0600)
			case "symlink receipt":
				outside := filepath.Join(t.TempDir(), "receipt")
				bytes, _ := os.ReadFile(receiptPath)
				os.WriteFile(outside, bytes, 0600)
				os.Remove(receiptPath)
				os.Symlink(outside, receiptPath)
			case "data reappeared":
				os.MkdirAll(filepath.Join(root, "volumes", r.Target.VolumeID), 0755)
			case "marker remains":
				os.WriteFile(filepath.Join(root, ".shiftpv", copyMarker(r.Target.CopyID)), []byte("unknown"), 0600)
			case "busy lock":
				s, err := OpenExisting(root, pool)
				if err != nil {
					t.Fatal(err)
				}
				held, err = s.AcquireExisting(context.Background(), r.Target.VolumeID)
				s.Close()
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
			case "partial GC":
				os.Remove(intentPath)
			case "cancelled authority":
				unused = func(context.Context, volume.CopyIdentity) (bool, error) { return false, context.Canceled }
			case "incomplete receipt", "foreign installation":
				if name == "incomplete receipt" {
					r.Purged = false
				} else {
					r.Target.InstallationID = "foreign"
				}
				data, err := canonical(r)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(receiptPath, data, 0600); err != nil {
					t.Fatal(err)
				}
				old := now.Add(-8 * 24 * time.Hour)
				os.Chtimes(receiptPath, old, old)

			}
			removed, err := CollectMetadata(context.Background(), root, pool, now, 7*24*time.Hour, unused)
			if name == "API failure" || name == "cancelled authority" {
				if err == nil {
					t.Fatal("API failure ignored")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := 0
			if name == "expired" || name == "partial GC" {
				want = 1
			}
			if removed != want {
				t.Fatalf("removed=%d want=%d", removed, want)
			}
			if _, err := os.Lstat(receiptPath); want == 1 && !errors.Is(err, os.ErrNotExist) || want == 0 && err != nil {
				t.Fatalf("receipt retention err=%v", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".shiftpv", "lock-"+r.Target.VolumeID)); err != nil {
				t.Fatal("GC removed a live Pool lock", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".shiftpv", "pool.json")); err != nil {
				t.Fatal("GC removed Pool identity", err)
			}
		})
	}
}

func TestMetadataGCRechecksPhysicalAbsenceAfterAuthority(t *testing.T) {
	root, pool, r, now := metadataFixture(t)
	unused := func(context.Context, volume.CopyIdentity) (bool, error) {
		return true, os.MkdirAll(filepath.Join(root, "volumes", r.Target.VolumeID), 0755)
	}
	removed, err := CollectMetadata(context.Background(), root, pool, now, 7*24*time.Hour, unused)
	if err != nil || removed != 0 {
		t.Fatalf("reappeared copy GC=%d err=%v", removed, err)
	}
}

func TestMetadataGCDoesNotHoldPoolMarkerLockDuringAPIRead(t *testing.T) {
	root, pool, _, now := metadataFixture(t)
	reading := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	defer func() {
		close(resume)
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("GC did not resume after API read")
		}
	}()
	go func() {
		_, err := CollectMetadata(context.Background(), root, pool, now, 7*24*time.Hour, func(context.Context, volume.CopyIdentity) (bool, error) {
			close(reading)
			<-resume
			return true, nil
		})
		done <- err
	}()
	select {
	case <-reading:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not start")
	}
	s, err := OpenExisting(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Use the exact lock shared by marker writes. A stalled API read must not
	// serialize other volumes' marker writes or Pool registration observations.
	markerLock, err := s.openControl("identity.lock", unix.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer markerLock.Close()
	if err := unix.Flock(int(markerLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal("GC held Pool marker lock during API read", err)
	}
	if err := unix.Flock(int(markerLock.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataGCAfterPoolReregistration(t *testing.T) {
	root, pool, r, now := metadataFixture(t)
	if err := ReleaseEmptyPool(context.Background(), root, pool); err != nil {
		t.Fatal(err)
	}
	replacement := PoolIdentity{InstallationID: pool.InstallationID, PoolUID: "replacement-pool"}
	s, err := Open(root, replacement)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	removed, err := CollectMetadata(context.Background(), root, replacement, now, 7*24*time.Hour, func(context.Context, volume.CopyIdentity) (bool, error) { return true, nil })
	if err != nil || removed != 1 {
		t.Fatalf("historical GC=%d err=%v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".shiftpv", "lock-"+r.Target.VolumeID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("historical GC recreated lock", err)
	}
}

func TestPoolReleasePreservesLockedNamespaceAndThenRemovesLocks(t *testing.T) {
	root, pool, r, _ := metadataFixture(t)
	s, err := OpenExisting(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	held, err := s.AcquireExisting(context.Background(), r.Target.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, pool); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy release=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".shiftpv", "pool.json")); err != nil {
		t.Fatal("busy release changed identity")
	}
	held.Close()
	// Keep an old opened descriptor. After release it must fail the Pool marker
	// fence even if its flock succeeds; registration creates a new namespace.
	stale, err := s.openControl("lock-"+r.Target.VolumeID, unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	if err := ReleaseEmptyPool(context.Background(), root, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".shiftpv", "lock-"+r.Target.VolumeID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released lock remains: %v", err)
	}
	if err := unix.Flock(int(stale.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if err := s.checkMarker("pool.json", pool); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale Pool fence accepted", err)
	}
}
