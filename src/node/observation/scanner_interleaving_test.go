//go:build linux || darwin

package observation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Inject only the boundary around the real physical scan. Native filesystem
// creation, placement reads and copy validation stay real. Publication evidence
// is provided by the fixture dependency.
func TestScannerObservesCreationAcrossPhysicalCollection(t *testing.T) {
	for _, boundary := range []string{"before collection", "after collection", "staged then completed"} {
		t.Run(boundary, func(t *testing.T) {
			scanner, pool, root, seed := interleavingFixture(t)
			copy := seed
			copy.VolumeID = "shiftpv-22222222222222222222222222222222"
			copy.VolumeUID, copy.CopyID = "new-volume", "new-copy"
			ctx := context.Background()
			create := func() {
				if err := ownership.PrepareServing(ctx, root, copy, func(context.Context) error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "staged then completed" {
				create()
				if err := os.Rename(filepath.Join(root, "volumes", copy.VolumeID), filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			result := scanner.scan(ctx, pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
				calls++
				if boundary == "before collection" {
					create()
				}
				paths, truncated, err := scanPhysical(ctx, root, known, limit)
				if boundary != "before collection" {
					create()
				}
				return paths, truncated, err
			}, physicalPathType)
			if calls != 1 || !result.Valid || result.Truncated || result.Message != "" || len(result.Copies) != 2 {
				t.Fatalf("creation interleaving was misclassified: calls=%d result=%+v", calls, result)
			}
			seen := map[volume.CopyIdentity]bool{}
			for _, observed := range result.Copies {
				if observed.Identity == nil || !observed.Present || observed.Problem != "" || !observed.Published {
					t.Fatalf("copy was not verified: %+v", observed)
				}
				seen[*observed.Identity] = true
			}
			if !seen[seed] || !seen[copy] {
				t.Fatalf("scan lost or duplicated a copy: %+v", result.Copies)
			}
		})
	}
}

func TestScannerRechecksCollectedUnrecordedPaths(t *testing.T) {
	for _, change := range []string{"unchanged", "removed", "changed type", "symlink parent"} {
		t.Run(change, func(t *testing.T) {
			scanner, pool, root, _ := interleavingFixture(t)
			parent := filepath.Join(root, ".shiftpv", "incoming")
			orphan := filepath.Join(parent, "orphan")
			if err := os.Mkdir(orphan, 0700); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			if err := os.WriteFile(filepath.Join(outside, "data"), []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			result := scanner.scan(context.Background(), pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
				paths, truncated, err := scanPhysical(ctx, root, known, limit)
				switch change {
				case "removed":
					if err := os.Remove(orphan); err != nil {
						t.Fatal(err)
					}
				case "changed type":
					if err := os.Remove(orphan); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(outside, orphan); err != nil {
						t.Fatal(err)
					}
				case "symlink parent":
					if err := os.Rename(parent, parent+"-preserved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(outside, parent); err != nil {
						t.Fatal(err)
					}
				}
				return paths, truncated, err
			}, physicalPathType)
			if change == "removed" {
				if result.Valid || result.Message != "PhysicalInventoryChanged" || result.Truncated || len(result.Copies) != 1 {
					t.Fatalf("vanished path became complete absence: %+v", result)
				}
				if fresh := scanner.Scan(context.Background(), pool, time.Now()); !fresh.Valid || fresh.Truncated || len(fresh.Copies) != 1 {
					t.Fatalf("stable removal did not converge: %+v", fresh)
				}
			} else {
				if result.Valid || result.Message == "" {
					t.Fatalf("remaining unrecorded path was accepted: %+v", result)
				}
				if change == "symlink parent" {
					if !strings.HasPrefix(result.Message, "PhysicalInventoryFailed:") {
						t.Fatalf("parent symlink was followed: %+v", result)
					}
				} else {
					want := "UnrecordedPath"
					if change == "changed type" {
						want = "UnexpectedPathType"
					}
					if len(result.Copies) != 2 || result.Copies[1].Problem != want {
						t.Fatalf("path's current type was not observed: %+v", result)
					}
				}
			}
			data, err := os.ReadFile(filepath.Join(outside, "data"))
			if err != nil || string(data) != "preserve" {
				t.Fatal("scan changed external data")
			}
		})
	}
}

func TestScannerCreationInterleavingRetainsInventoryLimit(t *testing.T) {
	scanner, pool, _, copy := interleavingFixture(t)
	scanner.Limit = 1
	copy.VolumeID = "shiftpv-22222222222222222222222222222222"
	copy.VolumeUID, copy.CopyID = "new-volume", "new-copy"
	result := scanner.scan(context.Background(), pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
		paths, truncated, err := scanPhysical(ctx, root, known, limit)
		if err := ownership.PrepareServing(ctx, root, copy, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		return paths, truncated, err
	}, physicalPathType)
	if !result.Truncated || len(result.Copies) != 1 {
		t.Fatalf("new placement bypassed the inventory limit: %+v", result)
	}
}

func TestScannerDefersAbsenceWhenCollectedStageFinishesAfterPlacementRead(t *testing.T) {
	for _, recorded := range []bool{true, false} {
		name := "before placement"
		if recorded {
			name = "after placement"
		}
		t.Run(name, func(t *testing.T) {
			scanner, pool, root, copy := interleavingFixture(t)
			stage := filepath.Join(root, ".shiftpv", "incoming", "create-"+copy.CopyID)
			if err := os.Rename(filepath.Join(root, "volumes", copy.VolumeID), stage); err != nil {
				t.Fatal(err)
			}
			wantCopies := 1
			if !recorded {
				// Reconstruct a create interrupted after stage mkdir, with its
				// copy intent intact but before the placement write.
				if err := os.Remove(filepath.Join(root, ".shiftpv", "placements", "placement-"+copy.CopyID+".json")); err != nil {
					t.Fatal(err)
				}
				wantCopies = 0
			}
			checks := 0
			result := scanner.scan(context.Background(), pool, time.Now(), scanPhysical, func(root, key string) (bool, bool, error) {
				checks++
				if err := ownership.PrepareServing(context.Background(), root, copy, func(context.Context) error { return nil }); err != nil {
					t.Fatal(err)
				}
				return physicalPathType(root, key)
			})
			if checks != 1 || result.Valid || result.Message != "PhysicalInventoryChanged" || len(result.Copies) != wantCopies {
				t.Fatalf("changing stage became valid absence: checks=%d result=%+v", checks, result)
			}
			if recorded && result.Copies[0].Present {
				t.Fatalf("stage was reported as serving: %+v", result)
			}
			fresh := scanner.Scan(context.Background(), pool, time.Now())
			if !fresh.Valid || fresh.Truncated || len(fresh.Copies) != 1 || !fresh.Copies[0].Present || fresh.Copies[0].Identity == nil || *fresh.Copies[0].Identity != copy {
				t.Fatalf("stable completed stage did not converge: %+v", fresh)
			}
		})
	}
}

func interleavingFixture(t *testing.T) (*Scanner, volumeapi.Pool, string, volume.CopyIdentity) {
	t.Helper()
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	copy := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", NodeName: "node-a", VolumeID: "shiftpv-11111111111111111111111111111111", VolumeUID: "seed-volume", CopyID: "seed-copy", Role: volume.RoleServing}
	if err := ownership.PrepareServing(context.Background(), root, copy, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	pool := volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}
	scanner := &Scanner{HostRoot: host, TargetRoot: "/pods", Installation: installation{id: copy.InstallationID}, Publications: publications{published: true}, Limit: 256}
	return scanner, pool, root, copy
}

func TestScannerPromotionAfterPhysicalCollection(t *testing.T) {
	scanner, pool, root, seed := interleavingFixture(t)
	incoming := seed
	incoming.VolumeID = "shiftpv-22222222222222222222222222222222"
	incoming.VolumeUID, incoming.CopyID, incoming.Role = "move-volume", "incoming-copy", volume.RoleIncoming
	serving := incoming
	serving.CopyID, serving.Role = "promoted-copy", volume.RoleServing
	authority := func(context.Context) error { return nil }
	if err := ownership.PopulateIncoming(context.Background(), root, incoming, "copy-operation", authority, func(_ context.Context, path string) error {
		return os.WriteFile(filepath.Join(path, "payload"), []byte("preserve"), 0600)
	}); err != nil {
		t.Fatal(err)
	}
	result := scanner.scan(context.Background(), pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
		paths, truncated, err := scanPhysical(ctx, root, known, limit)
		if err := ownership.PromoteIncoming(ctx, root, incoming, serving, "promote-operation", authority); err != nil {
			t.Fatal(err)
		}
		return paths, truncated, err
	}, physicalPathType)
	if result.Valid || result.Message != "PhysicalInventoryChanged" {
		t.Fatalf("changing promotion accepted: %+v", result)
	}
	fresh := scanner.Scan(context.Background(), pool, time.Now())
	if !fresh.Valid || fresh.Truncated || len(fresh.Copies) != 2 {
		t.Fatalf("promotion did not converge: %+v", fresh)
	}
	found := false
	for _, copy := range fresh.Copies {
		if copy.Identity != nil && *copy.Identity == serving && copy.Present {
			found = true
		}
	}
	if !found {
		t.Fatalf("serving copy not verified: %+v", fresh)
	}
	data, err := os.ReadFile(filepath.Join(root, "volumes", serving.VolumeID, "payload"))
	if err != nil || string(data) != "preserve" {
		t.Fatalf("payload changed: %q %v", data, err)
	}
}

func TestScannerReclaimAfterPhysicalCollection(t *testing.T) {
	scanner, pool, root, copy := interleavingFixture(t)
	result := scanner.scan(context.Background(), pool, time.Now(), func(ctx context.Context, root string, known map[string]struct{}, limit int) ([]volumeapi.CopyObservation, bool, error) {
		paths, truncated, err := scanPhysical(ctx, root, known, limit)
		receipt, digest, reclaimErr := ownership.ReclaimWithResume(ctx, root, copy, "cleanup-operation", func(context.Context, bool) error { return nil })
		if reclaimErr != nil || digest == "" || !receipt.Purged {
			t.Fatalf("reclaim failed: %+v %v", receipt, reclaimErr)
		}
		return paths, truncated, err
	}, physicalPathType)
	if result.Valid || result.Message != "PhysicalInventoryChanged" {
		t.Fatalf("changing reclaim accepted: %+v", result)
	}
	fresh := scanner.Scan(context.Background(), pool, time.Now())
	if !fresh.Valid || fresh.Truncated || len(fresh.Copies) != 0 {
		t.Fatalf("reclaim did not converge: %+v", fresh)
	}
	if _, err := os.Stat(filepath.Join(root, "volumes", copy.VolumeID)); !os.IsNotExist(err) {
		t.Fatalf("reclaimed copy remains: %v", err)
	}
}
