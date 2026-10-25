//go:build linux

package ownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/volume"
)

func cancellationBoundary(t *testing.T, boundary string) (string, volume.CopyIdentity) {
	t.Helper()
	root, copy := t.TempDir(), testIdentity()
	copy.CopyID = "initial-" + copy.VolumeUID
	if boundary == "before_store" {
		return root, copy
	}
	if err := PrepareServing(context.Background(), root, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "volumes", copy.VolumeID)
	stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)
	switch boundary {
	case "before_copy_intent":
		for _, path := range []string{final, filepath.Join(root, ".shiftpv", "placements", placementMarker(copy.CopyID)), filepath.Join(root, ".shiftpv", copyMarker(copy.CopyID))} {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	case "before_stage_mkdir", "before_placement_marker", "before_final_rename":
		if err := os.Rename(final, stage); err != nil {
			t.Fatal(err)
		}
		if boundary != "before_final_rename" {
			if err := os.Remove(filepath.Join(root, ".shiftpv", "placements", placementMarker(copy.CopyID))); err != nil {
				t.Fatal(err)
			}
		}
		if boundary == "before_stage_mkdir" {
			if err := os.Remove(stage); err != nil {
				t.Fatal(err)
			}
		}
		if boundary == "before_final_rename" {
			if err := os.WriteFile(filepath.Join(stage, "payload"), []byte("owned"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	case "serving":
		if err := os.WriteFile(filepath.Join(final, "payload"), []byte("owned"), 0600); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal(boundary)
	}
	return root, copy
}

func allowCancellation(context.Context, bool) error { return nil }

func TestCreationCancellationClosesEveryLocalApprovalBoundary(t *testing.T) {
	for _, boundary := range []string{"before_store", "before_copy_intent", "before_stage_mkdir", "before_placement_marker", "before_final_rename", "serving"} {
		t.Run(boundary, func(t *testing.T) {
			root, copy := cancellationBoundary(t, boundary)
			op := volume.CreationCancellationPrefix + copy.VolumeUID
			receipt, digest, err := CancelCreation(context.Background(), root, copy, op, allowCancellation)
			if err != nil || !receipt.Purged || !receipt.Retired || receipt.Target != copy || len(digest) != 64 {
				t.Fatalf("boundary=%s receipt=%+v err=%v", boundary, receipt, err)
			}
			noData := boundary == "before_store" || boundary == "before_copy_intent" || boundary == "before_stage_mkdir"
			if (receipt.Inode == 0) != noData {
				t.Fatalf("no-data receipt=%+v boundary=%s", receipt, boundary)
			}
			s, err := OpenExisting(root, PoolIdentity{InstallationID: copy.InstallationID, PoolUID: copy.PoolUID})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.verifyCreationAbsent(copy); err != nil {
				t.Fatal(err)
			}
			if exists, err := s.copyMarkerExists(copy); err != nil || exists {
				t.Fatalf("copy marker survived: %v %v", exists, err)
			}
			s.Close()
			replayed, replayedDigest, err := CancelCreation(context.Background(), root, copy, op, allowCancellation)
			if err != nil || replayed != receipt || replayedDigest != digest {
				t.Fatalf("replay changed receipt: %+v err=%v", replayed, err)
			}
			if boundary == "before_store" {
				if _, err := os.Stat(filepath.Join(root, "volumes")); !os.IsNotExist(err) {
					t.Fatal("cancellation created a serving directory")
				}
			}
		})
	}
}

func TestCreationCancellationPreservesUnprovenPaths(t *testing.T) {
	for _, fault := range []string{"unplaced payload", "missing copy intent", "unrecorded final", "unrecorded retired", "replaced inode", "duplicate location", "symlink"} {
		t.Run(fault, func(t *testing.T) {
			boundary := "before_final_rename"
			if fault == "unplaced payload" || fault == "missing copy intent" {
				boundary = "before_placement_marker"
			}
			if fault == "unrecorded final" || fault == "unrecorded retired" {
				boundary = "serving"
			}
			root, copy := cancellationBoundary(t, boundary)
			stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)
			final := filepath.Join(root, "volumes", copy.VolumeID)
			preserved := stage
			switch fault {
			case "unplaced payload":
				if err := os.WriteFile(filepath.Join(stage, "payload"), []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing copy intent":
				if err := os.Remove(filepath.Join(root, ".shiftpv", copyMarker(copy.CopyID))); err != nil {
					t.Fatal(err)
				}
			case "unrecorded final":
				preserved = final
				if err := os.Remove(filepath.Join(root, ".shiftpv", "placements", placementMarker(copy.CopyID))); err != nil {
					t.Fatal(err)
				}
			case "unrecorded retired":
				parent := filepath.Join(root, ".shiftpv", "retired")
				if err := os.Mkdir(parent, 0700); err != nil {
					t.Fatal(err)
				}
				preserved = filepath.Join(parent, copy.CopyID)
				if err := os.Rename(final, preserved); err != nil {
					t.Fatal(err)
				}
			case "replaced inode":
				preserved = stage + "-original"
				if err := os.Rename(stage, preserved); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(stage, 0700); err != nil {
					t.Fatal(err)
				}
			case "duplicate location":
				if err := os.Mkdir(final, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				preserved = stage + "-original"
				if err := os.Rename(stage, preserved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(preserved, stage); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(preserved)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := CancelCreation(context.Background(), root, copy, volume.CreationCancellationPrefix+copy.VolumeUID, allowCancellation); err == nil {
				t.Fatal("unknown path was cancelled")
			}
			after, err := os.Stat(preserved)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("unknown inode changed: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, ".shiftpv", operationMarker("receipt", volume.CreationCancellationPrefix+copy.VolumeUID))); !os.IsNotExist(err) {
				t.Fatal("unknown path produced receipt")
			}
		})
	}
}

func TestCreationCancellationRecoversRetireAndPurgeResponseLoss(t *testing.T) {
	for _, afterPurge := range []bool{false, true} {
		t.Run(map[bool]string{false: "after retire", true: "after purge"}[afterPurge], func(t *testing.T) {
			root, copy := cancellationBoundary(t, "before_final_rename")
			op := volume.CreationCancellationPrefix + copy.VolumeUID
			lost := errors.New("effect response lost")
			_, _, err := cancelCreation(context.Background(), root, copy, op, allowCancellation, preflightPurge, func(ctx context.Context, s *Store, i localIntent) error {
				if afterPurge {
					if err := purgeRetired(ctx, s, i); err != nil {
						return err
					}
				}
				return lost
			})
			if !errors.Is(err, lost) {
				t.Fatal(err)
			}
			receipt, digest, err := CancelCreation(context.Background(), root, copy, op, allowCancellation)
			if err != nil || !receipt.Purged || receipt.Inode == 0 || len(digest) != 64 {
				t.Fatalf("resumed cancellation=%+v err=%v", receipt, err)
			}
		})
	}
}

func TestCreationCancellationRechecksAuthorityBeforeRetire(t *testing.T) {
	root, copy := cancellationBoundary(t, "before_final_rename")
	calls := 0
	denied := errors.New("cancellation authority revoked")
	_, _, err := CancelCreation(context.Background(), root, copy, volume.CreationCancellationPrefix+copy.VolumeUID, func(context.Context, bool) error {
		calls++
		if calls == 3 {
			return denied
		}
		return nil
	})
	if !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID, "payload")); err != nil || string(b) != "owned" {
		t.Fatalf("revocation lost payload: %q %v", b, err)
	}
}

func TestCreationCancellationSerializesAnAlreadyRunningCreator(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	copy := testIdentity()
	copy.CopyID = "initial-" + copy.VolumeUID
	var cancelled atomic.Bool
	entered, release, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	denied := errors.New("creation fenced")
	go func() {
		calls := 0
		result <- PrepareServing(ctx, root, copy, func(context.Context) error {
			calls++
			if cancelled.Load() {
				return denied
			}
			if calls == 2 {
				close(entered)
				<-release
			}
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("creator did not hold the lock")
	}
	cancelled.Store(true)
	op := volume.CreationCancellationPrefix + copy.VolumeUID
	if _, _, err := CancelCreation(ctx, root, copy, op, allowCancellation); !errors.Is(err, ErrBusy) {
		close(release)
		t.Fatalf("cancellation did not serialize creator: %v", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, denied) {
		t.Fatalf("creator did not observe fence: %v", err)
	}
	receipt, _, err := CancelCreation(ctx, root, copy, op, allowCancellation)
	if err != nil || receipt.Inode == 0 || !receipt.Purged {
		t.Fatalf("running creator was not settled: %+v %v", receipt, err)
	}
}

func TestCreationCancellationNoDataReceiptCollectionRequiresUnusedProof(t *testing.T) {
	root, copy := cancellationBoundary(t, "before_store")
	op := volume.CreationCancellationPrefix + copy.VolumeUID
	if _, _, err := CancelCreation(context.Background(), root, copy, op, allowCancellation); err != nil {
		t.Fatal(err)
	}
	pool := PoolIdentity{InstallationID: copy.InstallationID, PoolUID: copy.PoolUID}
	now := time.Now().Add(2 * time.Hour)
	if n, err := CollectMetadata(context.Background(), root, pool, now, time.Hour, func(context.Context, volume.CopyIdentity) (bool, error) { return false, nil }); err != nil || n != 0 {
		t.Fatalf("live no-data proof collected: %d %v", n, err)
	}
	if n, err := CollectMetadata(context.Background(), root, pool, now, time.Hour, func(context.Context, volume.CopyIdentity) (bool, error) { return true, nil }); err != nil || n != 1 {
		t.Fatalf("settled no-data proof retained: %d %v", n, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".shiftpv", operationMarker("receipt", op))); !os.IsNotExist(err) {
		t.Fatal("receipt survived collection")
	}
}
