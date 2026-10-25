//go:build linux || darwin

package ownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const abandonedPlacementName = "pending-00112233-4455-4677-8899-aabbccddeeff"

func TestInterruptedPlacementTemporaryDoesNotStrandPoolRelease(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	identity := testIdentity()
	pool := PoolIdentity{InstallationID: identity.InstallationID, PoolUID: identity.PoolUID}
	store, err := Open(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ensureMarker(copyMarker(identity.CopyID), identity); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+identity.CopyID)
	if err := os.MkdirAll(stage, 0755); err != nil {
		t.Fatal(err)
	}
	// A process stop before the atomic rename leaves a partial temporary;
	// the durable copy intent still permits the next executor to resume.
	temporary := filepath.Join(root, ".shiftpv", "placements", abandonedPlacementName)
	if err := os.WriteFile(temporary, []byte(`{"version":1,"value":`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := PrepareServing(ctx, root, identity, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("create did not resume: %v", err)
	}
	if err := VerifyServingPath(root, identity); err != nil {
		t.Fatal(err)
	}
	// Purge is injected for portability; the same retire, receipt, metadata
	// removal and Pool release run on Linux and Darwin.
	receipt, _, err := reclaimWithState(ctx, root, identity, "delete-after-interruption",
		func(context.Context, bool) error { return nil }, func(*Store) error { return nil },
		func(context.Context, *Store, localIntent) error {
			return os.RemoveAll(filepath.Join(root, ".shiftpv", "retired", identity.CopyID))
		})
	if err != nil || !receipt.Purged {
		t.Fatalf("reclaim did not finish: receipt=%+v err=%v", receipt, err)
	}
	for range 2 {
		if err := ReleaseEmptyPool(ctx, root, pool); err != nil {
			t.Fatalf("empty Pool remained blocked after resumed create/delete: %v", err)
		}
	}
	if _, err := os.Lstat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned temporary remains: %v", err)
	}
	replacement, err := Open(root, PoolIdentity{InstallationID: identity.InstallationID, PoolUID: "replacement-pool"})
	if err != nil {
		t.Fatalf("released Pool path cannot be registered again: %v", err)
	}
	replacement.Close()
}

func TestReleasePreservesUnprovenPlacementTemporaries(t *testing.T) {
	for _, kind := range []string{"unknown name", "unknown UUID", "directory", "symlink", "hardlink", "mode", "oversized", "foreign owner"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "foreign owner" && os.Geteuid() != 0 {
				t.Skip("foreign ownership requires root")
			}
			root := t.TempDir()
			pool := PoolIdentity{InstallationID: "installation", PoolUID: "pool-uid"}
			store, err := Open(root, pool)
			if err != nil {
				t.Fatal(err)
			}
			store.Close()
			name := abandonedPlacementName
			if kind == "unknown name" {
				name = "operator-note"
			} else if kind == "unknown UUID" {
				name = "pending-operator-note"
			}
			path := filepath.Join(root, ".shiftpv", "placements", name)
			external := filepath.Join(t.TempDir(), "preserve")
			if err := os.WriteFile(external, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(external, path)
			case "hardlink":
				err = os.Link(external, path)
			default:
				data := "preserve"
				if kind == "oversized" {
					data = strings.Repeat("x", 8193)
				}
				err = os.WriteFile(path, []byte(data), 0600)
				if err == nil && kind == "mode" {
					err = os.Chmod(path, 0644)
				}
				if err == nil && kind == "foreign owner" {
					err = os.Chown(path, 1, 1)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := ReleaseEmptyPool(context.Background(), root, pool); !errors.Is(err, ErrNeedsReview) {
				t.Fatalf("unproven metadata did not block release: %v", err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("unproven metadata removed: %v", err)
			}
			if data, err := os.ReadFile(external); err != nil || string(data) != "preserve" {
				t.Fatalf("external data changed: data=%q err=%v", data, err)
			}
			if existing, err := OpenExisting(root, pool); err != nil {
				t.Fatalf("blocked release removed Pool identity: %v", err)
			} else {
				existing.Close()
			}
		})
	}
}

func TestReleaseDoesNotCollectAnActiveMarkerWriter(t *testing.T) {
	root := t.TempDir()
	pool := PoolIdentity{InstallationID: "installation", PoolUID: "pool-uid"}
	store, err := Open(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lock, err := store.openControl("identity.lock", unix.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(root, ".shiftpv", "placements", abandonedPlacementName)
	if err := os.WriteFile(temporary, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, pool); !errors.Is(err, ErrBusy) {
		t.Fatalf("active marker writer did not block release: %v", err)
	}
	if _, err := os.Lstat(temporary); err != nil {
		t.Fatalf("active temporary was collected: %v", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, pool); err != nil {
		t.Fatalf("abandoned temporary blocked release after writer exit: %v", err)
	}
}

func TestReleaseEmptyPoolAllowsNewPoolIdentity(t *testing.T) {
	root := t.TempDir()
	oldIdentity := PoolIdentity{InstallationID: "installation", PoolUID: "old-pool-uid"}
	store, err := Open(root, oldIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, oldIdentity); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, oldIdentity); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".shiftpv", "pool.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pool identity remains: %v", err)
	}
	replacement, err := Open(root, PoolIdentity{InstallationID: oldIdentity.InstallationID, PoolUID: "new-pool-uid"})
	if err != nil {
		t.Fatalf("new Pool identity was not admitted: %v", err)
	}
	replacement.Close()
}

func TestReleaseEmptyPoolPreservesManagedData(t *testing.T) {
	root := t.TempDir()
	identity := testIdentity()
	if err := PrepareServing(context.Background(), root, identity, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	pool := PoolIdentity{InstallationID: identity.InstallationID, PoolUID: identity.PoolUID}
	if err := ReleaseEmptyPool(context.Background(), root, pool); !errors.Is(err, ErrNeedsReview) {
		t.Fatalf("non-empty Pool release error = %v", err)
	}
	if err := VerifyServingPath(root, identity); err != nil {
		t.Fatalf("serving data changed after denied release: %v", err)
	}
}

func TestReleaseEmptyPoolPreservesUnrecordedManagedPath(t *testing.T) {
	root := t.TempDir()
	pool := PoolIdentity{InstallationID: "installation", PoolUID: "pool-uid"}
	store, err := Open(root, pool)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	path := filepath.Join(root, ".shiftpv", "incoming", "unrecorded")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseEmptyPool(context.Background(), root, pool); !errors.Is(err, ErrNeedsReview) {
		t.Fatalf("unrecorded path release error = %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unrecorded path changed: %v", err)
	}
}
