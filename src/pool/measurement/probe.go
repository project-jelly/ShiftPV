package measurement

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacity"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

// Probe checks authority and the mounted identity before and after reading.
// Keeping the directory open pins the syscall to the filesystem being read.
type Probe struct {
	Pools       probeRegistry
	HostRoot    string
	VerifyMount func(string, volumeapi.Pool) error
	Read        func(string) (capacity.Filesystem, error)
}

func (p *Probe) StatFSForPool(ctx context.Context, expected volumeapi.Pool) (capacity.Filesystem, error) {
	if !filepath.IsAbs(p.HostRoot) || !filepath.IsAbs(expected.MountPath) || filepath.Clean(expected.MountPath) == "/" {
		return capacity.Filesystem{}, fmt.Errorf("capacity probe requires an absolute host root and non-root Pool path")
	}
	root := filepath.Join(p.HostRoot, strings.TrimPrefix(filepath.Clean(expected.MountPath), "/"))
	if err := p.verify(ctx, expected, root); err != nil {
		return capacity.Filesystem{}, err
	}
	read := p.Read
	if read == nil {
		read = readFilesystem
	}
	stats, err := read(root)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if err := p.verify(ctx, expected, root); err != nil {
		return capacity.Filesystem{}, err
	}
	return stats, nil
}

func (p *Probe) verify(ctx context.Context, expected volumeapi.Pool, root string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pool, err := p.Pools.ReadyPoolForIdentity(ctx, expected.Name, expected.UID, expected.NodeName)
	if err != nil {
		return err
	}
	if _, err := matchingEvidence(expected, pool); err != nil {
		return err
	}
	verify := p.VerifyMount
	if verify == nil {
		verify = readiness.VerifyMountedPath
	}
	return verify(root, pool)
}

func readFilesystem(root string) (capacity.Filesystem, error) {
	directory, err := os.Open(root)
	if err != nil {
		return capacity.Filesystem{}, err
	}
	defer directory.Close()
	info, err := directory.Stat()
	if err != nil {
		return capacity.Filesystem{}, err
	}
	if !info.IsDir() {
		return capacity.Filesystem{}, fmt.Errorf("Pool path is not a directory")
	}
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(directory.Fd()), &stat); err != nil {
		return capacity.Filesystem{}, err
	}
	return capacity.ParseStatOutput(fmt.Sprintf("%d %d %d %d", stat.Blocks, stat.Bavail, stat.Bsize, stat.Ffree))
}
