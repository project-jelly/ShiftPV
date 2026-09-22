//go:build linux

package mount

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	mountutils "k8s.io/mount-utils"
)

// Compare the batched lookup with the Linux implementation it replaces in the
// scanner, including a subvolume root and a mount hiding earlier mount entries.
func TestPublicationSnapshotMatchesLinuxMountReferenceSearch(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	targets := filepath.Join(root, "pods")
	if err := os.Mkdir(source, 0755); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"bind", "subvolume", "covered"} {
		t.Run(scenario, func(t *testing.T) {
			table := "1 0 8:1 / / rw - ext4 /dev/test rw\n"
			bindRoot := source
			if scenario == "subvolume" {
				table += fmt.Sprintf("2 1 8:2 /subvolume %s rw - btrfs /dev/other rw\n", root)
				bindRoot = "/subvolume/source"
			}
			device := "8:1"
			if scenario == "subvolume" {
				device = "8:2"
			}
			table += fmt.Sprintf("3 1 %s %s %s rw - ext4 /dev/test rw\n", device, bindRoot, filepath.Join(targets, "target"))
			if scenario == "covered" {
				table += fmt.Sprintf("4 1 8:3 / %s rw - ext4 /dev/third rw\n", root)
			}
			file := filepath.Join(root, "mountinfo")
			if err := os.WriteFile(file, []byte(table), 0600); err != nil {
				t.Fatal(err)
			}
			refs, err := mountutils.SearchMountPoints(source, file)
			if err != nil {
				t.Fatal(err)
			}
			want := false
			for _, ref := range refs {
				want = want || ValidateTarget(targets, ref) == nil
			}
			snapshot, err := (&Binder{MountInfoPath: file}).PublicationSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			got, err := snapshot.HasPublishedTarget(source, targets)
			if err != nil || got != want {
				t.Fatalf("snapshot=%v upstream=%v err=%v", got, want, err)
			}
		})
	}
}
