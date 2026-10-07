package readiness

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	poolcapacity "github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/registration"
)

type Check = registration.Check

type Result struct {
	Independent      Check
	CapacityUnit     *volumeapi.PoolCapacityUnit
	Accessible       Check
	Mounted          Check
	Writable         Check
	CapacityReadable Check
	Filesystem       poolcapacity.Filesystem
	MountIdentity    *volumeapi.PoolMountIdentity
}

type Inspector interface {
	AllocationInspector
	Inspect(volumeapi.Pool) Result
}

type AllocationResolver interface {
	Resolve(string) ([]capacityunit.Extent, error)
}

// ProbeDependencies exposes only host operations used by registration checks.
type ProbeDependencies struct {
	Allocations      AllocationResolver
	InspectDirectory func(string) error
	Write            func(string) error
	StatFS           func(string) (poolcapacity.Filesystem, error)
	ListMounts       func() ([]mountutils.MountInfo, error)
}

type Probe struct {
	allocations AllocationResolver
	HostRoot    string
	inspect     func(string) error
	write       func(string) error
	statFS      func(string) (poolcapacity.Filesystem, error)
	listMounts  func() ([]mountutils.MountInfo, error)
}

func NewProbeWithDependencies(hostRoot string, d ProbeDependencies) (*Probe, error) {
	if !filepath.IsAbs(hostRoot) || d.Allocations == nil || d.InspectDirectory == nil || d.Write == nil || d.StatFS == nil || d.ListMounts == nil {
		return nil, fmt.Errorf("Pool probe configuration is incomplete")
	}
	return &Probe{HostRoot: hostRoot, allocations: d.Allocations, inspect: d.InspectDirectory, write: d.Write, statFS: d.StatFS, listMounts: d.ListMounts}, nil
}

func NewProbe(hostRoot string) *Probe {
	d := HostProbeDependencies(hostRoot)
	return &Probe{HostRoot: hostRoot, allocations: d.Allocations, inspect: d.InspectDirectory, write: d.Write, statFS: d.StatFS, listMounts: d.ListMounts}
}

func HostProbeDependencies(hostRoot string) ProbeDependencies {
	return ProbeDependencies{
		Allocations:      capacityunit.NewResolver(filepath.Join(hostRoot, "sys"), filepath.Join(hostRoot, "dev/mapper/control")),
		InspectDirectory: inspectDirectory,
		Write:            writeProbe,
		ListMounts: func() ([]mountutils.MountInfo, error) {
			return mountutils.ParseMountInfo("/proc/self/mountinfo")
		},
		StatFS: func(path string) (poolcapacity.Filesystem, error) {
			var stat syscall.Statfs_t
			if err := syscall.Statfs(path, &stat); err != nil {
				return poolcapacity.Filesystem{}, err
			}
			return poolcapacity.ParseStatOutput(fmt.Sprintf("%d %d %d %d", stat.Blocks, stat.Bavail, stat.Bsize, stat.Ffree))
		},
	}
}

func (p *Probe) Inspect(pool volumeapi.Pool) Result {
	if pool.MountPolicy != "" && pool.MountPolicy != volumeapi.PoolMountPolicyRequireMountPoint {
		return Result{Accessible: Check{Known: true, Reason: "MountPolicyInvalid", Message: "unsupported Pool mount policy"}}
	}
	path, err := hostPath(p.HostRoot, pool.MountPath)
	if err != nil {
		failed := failure(err)
		return Result{Accessible: failed, Writable: skipped(), CapacityReadable: skipped()}
	}
	if err := p.inspect(path); err != nil {
		failed := failure(err)
		return Result{Accessible: failed, Writable: skipped(), CapacityReadable: skipped()}
	}
	result := Result{
		Accessible: Check{OK: true, Known: true, Reason: "DirectoryAccessible", Message: fmt.Sprintf("%s is an accessible directory", pool.MountPath)},
	}
	if pool.MountPolicy == volumeapi.PoolMountPolicyRequireMountPoint {
		identity, check := p.inspectMount(path, pool.Status.MountIdentity)
		result.Mounted = check
		if !check.OK {
			result.Writable, result.CapacityReadable = skipped(), skipped()
			return result
		}
		result.MountIdentity = &identity
	}
	if pool.CapacityPolicy == volumeapi.PoolCapacityPolicyFixedBlock {
		result.CapacityUnit, result.Independent = p.InspectCapacityUnit(pool)
		if !result.Independent.OK {
			result.Writable, result.CapacityReadable = skipped(), skipped()
			return result
		}
	} else if pool.CapacityPolicy != "" {
		result.Independent = capacityFailure("CapacityPolicyInvalid", fmt.Errorf("unsupported capacity policy"))
		return result
	}
	if err := p.write(path); err != nil {
		result.Writable = failure(err)
	} else {
		result.Writable = Check{OK: true, Known: true, Reason: "Writable", Message: "temporary directory, write, sync, and cleanup succeeded"}
	}
	stats, err := p.statFS(path)
	if err != nil {
		result.CapacityReadable = failure(err)
	} else {
		result.Filesystem = stats
		result.CapacityReadable = Check{OK: true, Known: true, Reason: "CapacityReadable", Message: "filesystem capacity is readable"}
	}
	if result.MountIdentity != nil {
		// Detect a mount replaced during the write and statfs probes before
		// publishing a successful readiness observation.
		_, check := p.inspectMount(path, result.MountIdentity)
		if !check.OK {
			result.Mounted = check
		}
	}
	if result.CapacityUnit != nil {
		candidate := pool
		candidate.Status.CapacityUnit = result.CapacityUnit
		_, result.Independent = p.InspectCapacityUnit(candidate)
	}
	return result
}

func (p *Probe) inspectMount(path string, expected *volumeapi.PoolMountIdentity) (volumeapi.PoolMountIdentity, Check) {
	if p.listMounts == nil {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountInspectionFailed", Message: "mount table reader is unavailable"}
	}
	mounts, err := p.listMounts()
	if err != nil {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountInspectionFailed", Message: err.Error()}
	}
	var target *mountutils.MountInfo
	var parent *mountutils.MountInfo
	for i := range mounts {
		entry := &mounts[i]
		point := unescapeMountPath(entry.MountPoint)
		if point == path {
			if target != nil {
				return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountAmbiguous", Message: "multiple mounts occupy the Pool path"}
			}
			target = entry
		} else if strings.HasPrefix(path, strings.TrimSuffix(point, "/")+"/") &&
			(parent == nil || len(point) > len(unescapeMountPath(parent.MountPoint))) {
			parent = entry
		}
	}
	if target == nil {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountMissing", Message: "Pool path is not a mount point"}
	}
	if parent == nil {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountInspectionFailed", Message: "Pool parent mount is missing"}
	}
	if target.Root != "/" {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountNotFilesystemRoot", Message: "Pool mount is a filesystem subdirectory"}
	}
	if target.Major == parent.Major && target.Minor == parent.Minor {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountSharesParent", Message: "Pool mount shares the parent filesystem capacity"}
	}
	identity := volumeapi.PoolMountIdentity{
		Device: fmt.Sprintf("%d:%d", target.Major, target.Minor), Root: target.Root,
		Source: target.Source, Filesystem: target.FsType,
	}
	if expected != nil && identity != *expected {
		return volumeapi.PoolMountIdentity{}, Check{Known: true, Reason: "MountIdentityChanged", Message: "mounted filesystem no longer matches the Pool's recorded identity"}
	}
	return identity, Check{OK: true, Known: true, Reason: "MountVerified", Message: "Pool path is the expected filesystem mount point"}
}

// VerifyMountedPath checks the live mount at a node path or helper Pod bind
// root before an opted-in Pool performs a storage operation. It is read-only.
func VerifyMountedPath(path string, pool volumeapi.Pool) error {
	if err := verifyCapacityMountedPath(path, pool); err != nil {
		return err
	}
	if pool.MountPolicy == "" {
		return nil
	}
	if pool.MountPolicy != volumeapi.PoolMountPolicyRequireMountPoint {
		return fmt.Errorf("unsupported Pool mount policy %q", pool.MountPolicy)
	}
	if pool.Status.MountIdentity == nil {
		return fmt.Errorf("Pool mount identity has not been recorded")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
		return fmt.Errorf("Pool root %q must be an absolute non-root path", path)
	}
	if err := inspectDirectory(path); err != nil {
		return fmt.Errorf("inspect Pool root: %w", err)
	}
	_, check := NewProbe("/").inspectMount(filepath.Clean(path), pool.Status.MountIdentity)
	if !check.OK {
		return fmt.Errorf("verify Pool mount at %q: %s: %s", path, check.Reason, check.Message)
	}
	return nil
}

func unescapeMountPath(path string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(path)
}

func inspectDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory: %w", path, syscall.ENOTDIR)
	}
	return nil
}

func hostPath(hostRoot, mountPath string) (string, error) {
	hostRoot = filepath.Clean(hostRoot)
	mountPath = filepath.Clean(mountPath)
	if !filepath.IsAbs(hostRoot) || !filepath.IsAbs(mountPath) || mountPath == string(filepath.Separator) {
		return "", fmt.Errorf("host root and Pool mount path must be absolute non-root paths")
	}
	path := filepath.Join(hostRoot, strings.TrimPrefix(mountPath, string(filepath.Separator)))
	relative, err := filepath.Rel(hostRoot, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("Pool mount path %q escapes host root %q", mountPath, hostRoot)
	}
	return path, nil
}

func writeProbe(root string) (result error) {
	directory, err := os.MkdirTemp(root, ".shiftpv-probe-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(directory); result == nil && err != nil {
			result = err
		}
	}()
	file, err := os.OpenFile(filepath.Join(directory, "writable"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write([]byte("shiftpv pool readiness\n")); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func failure(err error) Check {
	reason := "ProbeFailed"
	switch {
	case errors.Is(err, fs.ErrNotExist):
		reason = "PathMissing"
	case errors.Is(err, fs.ErrPermission):
		reason = "PermissionDenied"
	case errors.Is(err, syscall.ENOTDIR):
		reason = "NotDirectory"
	case errors.Is(err, syscall.EROFS):
		reason = "ReadOnly"
	case errors.Is(err, syscall.ENOSPC):
		reason = "NoSpace"
	}
	return Check{Known: true, Reason: reason, Message: err.Error()}
}

func skipped() Check {
	return Check{Reason: "ProbeSkipped", Message: "check skipped because the directory prerequisite failed"}
}
