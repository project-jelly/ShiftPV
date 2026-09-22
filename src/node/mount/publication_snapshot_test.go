//go:build linux || darwin

package mount

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPublicationSnapshotReusesMountTableOnlyWithinScan(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "volumes", testVolumeID)
	if err := os.MkdirAll(source, 0755); err != nil {
		t.Fatal(err)
	}
	targets := filepath.Join(root, "pods")
	file := filepath.Join(root, "mountinfo")
	content := fmt.Sprintf("1 0 8:1 / / rw - ext4 /dev/test rw\n2 1 8:1 %s %s rw - ext4 /dev/test rw\n", source, filepath.Join(targets, "target"))
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	b := &Binder{MountInfoPath: file}
	snapshot, err := b.PublicationSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		published, err := snapshot.HasPublishedTarget(source, targets)
		if err != nil || !published {
			t.Fatalf("snapshot re-read mount table: published=%v err=%v", published, err)
		}
	}
	if _, err := b.PublicationSnapshot(); err == nil {
		t.Fatal("next scan reused an earlier snapshot")
	}
}

func TestPublicationSnapshotRequiresMatchingDeviceRootAndTarget(t *testing.T) {
	for _, scenario := range []string{"exact", "other device", "other root", "outside target", "missing source", "symlink source", "overmount"} {
		t.Run(scenario, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(root, "volumes", testVolumeID)
			if err := os.MkdirAll(source, 0755); err != nil {
				t.Fatal(err)
			}
			targets := filepath.Join(root, "pods")
			mountRoot, target, device := source, filepath.Join(targets, "target"), "8:1"
			switch scenario {
			case "other device":
				device = "8:2"
			case "other root":
				mountRoot += "-other"
			case "outside target":
				target = filepath.Join(root, "pods-other", "target")
			case "missing source":
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
			case "symlink source":
				link := filepath.Join(root, "link")
				if err := os.Symlink(source, link); err != nil {
					t.Fatal(err)
				}
				source = link
			}
			content := fmt.Sprintf("1 0 8:1 / / rw - ext4 /dev/test rw\n2 1 %s %s %s rw - ext4 /dev/test rw\n", device, mountRoot, target)
			if scenario == "overmount" {
				content += fmt.Sprintf("3 1 8:2 / %s rw - ext4 /dev/other rw\n", filepath.Dir(source))
			}
			file := filepath.Join(root, "mountinfo")
			if err := os.WriteFile(file, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			snapshot, err := (&Binder{MountInfoPath: file}).PublicationSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			got, err := snapshot.HasPublishedTarget(source, targets)
			want := scenario == "exact" || scenario == "symlink source"
			if err != nil || got != want {
				t.Fatalf("published=%v want=%v err=%v", got, want, err)
			}
		})
	}
}
