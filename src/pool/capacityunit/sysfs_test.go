package capacityunit

import (
	"os"
	"path/filepath"
	"testing"
)

func fakeSysfsBlock(t *testing.T, root, name, device, size string) string {
	t.Helper()
	path := filepath.Join(root, "devices", name)
	if err := os.MkdirAll(filepath.Join(path, "slaves"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "size"), []byte(size), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "dev"), []byte(device), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "dev/block"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, filepath.Join(root, "dev/block", device)); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestSysfsRejectsUnknownVirtualAndStackedBacking(t *testing.T) {
	root := t.TempDir()
	fakeSysfsBlock(t, root, "virtual/block/loop0", "7:0", "1024")
	path := fakeSysfsBlock(t, root, "pci/disk", "8:0", "1024")
	if err := os.Mkdir(filepath.Join(path, "slaves/another"), 0755); err != nil {
		t.Fatal(err)
	}
	r := NewResolver(root, "/missing-control")
	for _, device := range []string{"7:0", "8:0", "0:9", "../escape", "-1:0"} {
		if _, err := r.Resolve(device); err == nil {
			t.Fatalf("unsupported backing %s accepted", device)
		}
	}
}
func TestSysfsResolvesPartitionIntoFixedDiskRange(t *testing.T) {
	root := t.TempDir()
	fakeSysfsBlock(t, root, "pci/disk", "8:0", "4096")
	partition := fakeSysfsBlock(t, root, "pci/disk/part", "8:1", "1024")
	for name, value := range map[string]string{"partition": "1", "start": "256"} {
		if err := os.WriteFile(filepath.Join(partition, name), []byte(value), 0644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := NewResolver(root, "/missing-control").Resolve("8:1")
	if err != nil || len(got) != 1 || got[0].Device != "8:0" || got[0].Start != 256 || got[0].Sectors != 1024 {
		t.Fatalf("extent=%v err=%v", got, err)
	}
}

func TestNativePathsWithSameWWIDShareBackingIdentity(t *testing.T) {
	root := t.TempDir()
	for name, device := range map[string]string{"pci/a": "8:0", "pci/b": "8:16"} {
		path := fakeSysfsBlock(t, root, name, device, "1024")
		if err := os.WriteFile(filepath.Join(path, "wwid"), []byte("naa.shared-disk\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	r := NewResolver(root, "/missing-control")
	a, err := r.Resolve("8:0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Resolve("8:16")
	if err != nil {
		t.Fatal(err)
	}
	if overlap, err := Overlap(a, b); err != nil || !overlap {
		t.Fatalf("native aliases approved: %v %v %v", a, b, err)
	}
}
