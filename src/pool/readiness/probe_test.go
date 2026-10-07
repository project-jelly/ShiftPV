package readiness

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
)

func TestProbeInspect(t *testing.T) {
	permission := &os.PathError{Op: "mkdir", Path: "/host/pool", Err: os.ErrPermission}
	readOnly := &os.PathError{Op: "mkdir", Path: "/host/pool", Err: syscall.EROFS}
	noSpace := &os.PathError{Op: "mkdir", Path: "/host/pool", Err: syscall.ENOSPC}
	for name, test := range map[string]struct {
		inspectErr  error
		writeErr    error
		statErr     error
		ready       bool
		readyReason string
	}{
		"ordinary directory": {ready: true, readyReason: "PoolReady"},
		"missing":            {inspectErr: os.ErrNotExist, readyReason: "PathMissing"},
		"not a directory":    {inspectErr: syscall.ENOTDIR, readyReason: "NotDirectory"},
		"permission denied":  {writeErr: permission, readyReason: "PermissionDenied"},
		"read only":          {writeErr: readOnly, readyReason: "ReadOnly"},
		"no space":           {writeErr: noSpace, readyReason: "NoSpace"},
		"statfs failure":     {statErr: errors.New("statfs failed"), readyReason: "ProbeFailed"},
	} {
		t.Run(name, func(t *testing.T) {
			probe := &Probe{
				HostRoot: "/host",
				inspect: func(path string) error {
					if path != "/host/pool" {
						t.Fatalf("path = %q", path)
					}
					return test.inspectErr
				},
				write:  func(string) error { return test.writeErr },
				statFS: func(string) (poolcapacity.Filesystem, error) { return poolcapacity.Filesystem{}, test.statErr },
			}
			result := probe.Inspect(volumeapi.Pool{MountPath: "/pool"})
			conditions := conditions(result, 1, testTime)
			ready := conditions[len(conditions)-1]
			if (ready.Status == "True") != test.ready || ready.Reason != test.readyReason {
				t.Fatalf("ready = %#v", ready)
			}
		})
	}
}

func TestNewProbeAcceptsOrdinaryDirectory(t *testing.T) {
	hostRoot := t.TempDir()
	poolPath := filepath.Join(hostRoot, "var", "lib", "shiftpv")
	if err := os.MkdirAll(poolPath, 0o700); err != nil {
		t.Fatal(err)
	}
	result := NewProbe(hostRoot).Inspect(volumeapi.Pool{MountPath: "/var/lib/shiftpv"})
	conditions := conditions(result, 1, testTime)
	ready := conditions[len(conditions)-1]
	if ready.Status != "True" || ready.Reason != "PoolReady" {
		t.Fatalf("ordinary directory readiness = %#v", ready)
	}
	entries, err := os.ReadDir(poolPath)
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe leftovers = %v err=%v", entries, err)
	}
}

func TestRequiredMountPointAnchorsIdentityAndSkipsWritesOnMismatch(t *testing.T) {
	base := []mountutils.MountInfo{
		{MountPoint: "/host", Major: 8, Minor: 1, Root: "/", Source: "/dev/root", FsType: "ext4"},
		{MountPoint: "/host/pool", Major: 8, Minor: 2, Root: "/", Source: "/dev/disk-a", FsType: "ext4"},
	}
	writes, stats := 0, 0
	mounts := append([]mountutils.MountInfo(nil), base...)
	probe := &Probe{
		HostRoot: "/host",
		inspect:  func(string) error { return nil },
		write: func(string) error {
			writes++
			return nil
		},
		statFS: func(string) (poolcapacity.Filesystem, error) {
			stats++
			return poolcapacity.Filesystem{}, nil
		},
		listMounts: func() ([]mountutils.MountInfo, error) { return mounts, nil },
	}
	pool := volumeapi.Pool{MountPath: "/pool", MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint}
	first := probe.Inspect(pool)
	if !first.Mounted.OK || first.MountIdentity == nil || first.MountIdentity.Device != "8:2" || writes != 1 || stats != 1 {
		t.Fatalf("initial mount probe = %#v, writes=%d stats=%d", first, writes, stats)
	}
	pool.Status.MountIdentity = first.MountIdentity
	mounts[1].Source = "/dev/disk-b"
	changed := probe.Inspect(pool)
	if changed.Mounted.Reason != "MountIdentityChanged" || writes != 1 || stats != 1 {
		t.Fatalf("replacement mount probe = %#v, writes=%d stats=%d", changed, writes, stats)
	}
	mounts = mounts[:1]
	missing := probe.Inspect(pool)
	if missing.Mounted.Reason != "MountMissing" || writes != 1 || stats != 1 {
		t.Fatalf("unmounted path probe = %#v, writes=%d stats=%d", missing, writes, stats)
	}
}

func TestRequiredMountPointRejectsSharedAndSubdirectoryMounts(t *testing.T) {
	for _, test := range []struct {
		name   string
		device int
		root   string
		reason string
	}{
		{name: "parent filesystem alias", device: 1, root: "/", reason: "MountSharesParent"},
		{name: "filesystem subdirectory", device: 2, root: "/subdir", reason: "MountNotFilesystemRoot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &Probe{listMounts: func() ([]mountutils.MountInfo, error) {
				return []mountutils.MountInfo{
					{MountPoint: "/host", Major: 8, Minor: 1},
					{MountPoint: "/host/pool", Major: 8, Minor: test.device, Root: test.root},
				}, nil
			}}
			_, check := probe.inspectMount("/host/pool", nil)
			if check.Reason != test.reason {
				t.Fatalf("mount check = %#v", check)
			}
		})
	}
}

func TestInspectDirectoryRejectsNonDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool")
	if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := inspectDirectory(path)
	if err == nil || !strings.Contains(err.Error(), "not a directory") || !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("inspect error = %v", err)
	}
}

func TestWriteProbeCleansUp(t *testing.T) {
	root := t.TempDir()
	if err := writeProbe(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe leftovers = %v err=%v", entries, err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeProbe(file); err == nil {
		t.Fatal("write probe accepted a file")
	}
}

func TestHostPathValidation(t *testing.T) {
	if got, err := hostPath("/host", "/mnt/data"); err != nil || got != "/host/mnt/data" {
		t.Fatalf("host path = %q err=%v", got, err)
	}
	for _, paths := range [][2]string{{"relative", "/pool"}, {"/host", "relative"}, {"/host", "/"}} {
		if _, err := hostPath(paths[0], paths[1]); err == nil {
			t.Fatalf("accepted hostRoot=%q mountPath=%q", paths[0], paths[1])
		}
	}
}

func TestProbeDependencyValidationAndInjectedFailure(t *testing.T) {
	deps := HostProbeDependencies("/host")
	for _, change := range []func(*ProbeDependencies){
		func(d *ProbeDependencies) { d.Allocations = nil },
		func(d *ProbeDependencies) { d.InspectDirectory = nil },
		func(d *ProbeDependencies) { d.Write = nil },
		func(d *ProbeDependencies) { d.StatFS = nil },
		func(d *ProbeDependencies) { d.ListMounts = nil },
	} {
		d := deps
		change(&d)
		if p, err := NewProbeWithDependencies("/host", d); err == nil || p != nil {
			t.Fatal("missing host dependency accepted")
		}
	}
	if _, err := NewProbeWithDependencies("relative", deps); err == nil {
		t.Fatal("invalid host root accepted")
	}
	writes := 0
	deps.InspectDirectory = func(string) error { return os.ErrPermission }
	deps.Write = func(string) error { writes++; return nil }
	p, err := NewProbeWithDependencies("/host", deps)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Inspect(volumeapi.Pool{MountPath: "/pool"})
	if result.Accessible.Reason != "PermissionDenied" || writes != 0 {
		t.Fatalf("failed prerequisite performed writes: %+v", result)
	}
}
