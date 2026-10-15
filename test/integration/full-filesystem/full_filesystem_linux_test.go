//go:build linux

package fullfilesystem_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// These tests use only the disposable ext4 images mounted by run.sh. Ordinary
// go test, including privileged CI jobs, must not fill a host filesystem.
func TestFullFilesystemReadinessAndCleanupRecovery(t *testing.T) {
	root := os.Getenv("SHIFTPV_ENOSPC_FIXTURE")
	if os.Getenv("SHIFTPV_FULL_FILESYSTEM") != "1" || os.Geteuid() != 0 || root == "" {
		t.Skip("requires the opt-in full-filesystem/run.sh fixture as root")
	}
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	host, err := os.Readlink("/proc/1/ns/mnt")
	if err != nil || self == host {
		t.Fatal("a private mount namespace is required", err)
	}
	if !filepath.IsAbs(root) || !strings.HasPrefix(filepath.Base(root), "shiftpv-enospc.") {
		t.Fatal("unexpected fixture path")
	}
	for _, mode := range []string{"blocks", "inodes"} {
		t.Run(mode, func(t *testing.T) { qualifyFullFilesystem(t, filepath.Join(root, mode), mode) })
	}
}

func qualifyFullFilesystem(t *testing.T, path, mode string) {
	t.Helper()
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil || fs.Type != unix.EXT4_SUPER_MAGIC {
		t.Fatal("fixture must be an ext4 filesystem", err)
	}
	const payload = "existing-volume-payload-must-survive-enospc\n"
	sentinel := filepath.Join(path, "existing-data")
	if err := os.WriteFile(sentinel, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	target := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool", PoolUID: "pool-uid", NodeName: "node",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid", CopyID: "copy-" + mode, Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), path, target, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(path, "volumes", target.VolumeID)
	copyPayload := filepath.Join(copyPath, "payload")
	if err := os.WriteFile(copyPayload, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	copyInfo, err := os.Stat(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	repository := &poolRepository{pool: volumeapi.Pool{
		Name: "pool", UID: "pool-uid", NodeName: "node", PoolGroup: "default",
		MountPath: path, MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint,
		CapacityLimit: "64Mi", Generation: 1, Finalizers: []string{volumeapi.PoolProtectionFinalizer},
	}}
	reconciler, err := readiness.NewReconciler("node", repository, readiness.NewProbe("/"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Inventory transport is injected; the writable and statfs probes are real.
	reconciler.Inventory = func(_ context.Context, _ volumeapi.Pool, now time.Time) volumeapi.PoolInventory {
		return volumeapi.PoolInventory{ObservedAt: metav1.NewTime(now), Valid: true}
	}
	observe := func(allowed bool, reason string) {
		t.Helper()
		if err := reconciler.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, why := repository.pool.ReadyAt(time.Now(), time.Minute); got != allowed || (!allowed && why != reason) {
			t.Fatalf("placement: allowed=%t reason=%s, want allowed=%t reason=%s", got, why, allowed, reason)
		}
		writable := meta.FindStatusCondition(repository.pool.Status.Conditions, volumeapi.PoolConditionWritable)
		capacity := meta.FindStatusCondition(repository.pool.Status.Conditions, volumeapi.PoolConditionCapacityReadable)
		if writable == nil || capacity == nil || capacity.Status != metav1.ConditionTrue {
			t.Fatalf("missing independent filesystem observations: writable=%+v capacity=%+v", writable, capacity)
		}
		// A full filesystem may still accept a small probe. Existing exact
		// effects use that observation and their own durable intent, not Ready.
		cleanupAllowed := writable.Status == metav1.ConditionTrue
		for name, check := range map[string]func(time.Time, time.Duration) (bool, string){
			"helper cleanup": repository.pool.CleanupReadyAt,
			"native cleanup": repository.pool.ExecutionReadyAt,
		} {
			got, why := check(time.Now(), time.Minute)
			if got != cleanupAllowed || (!cleanupAllowed && why != writable.Reason) {
				t.Fatalf("%s: allowed=%t reason=%s, writable=%+v", name, got, why, writable)
			}
		}
		t.Logf("allocation allowed=%t; Writable=%s/%s", allowed, writable.Status, writable.Reason)
	}
	observe(true, "")
	anchor := *repository.pool.Status.MountIdentity
	filler := filepath.Join(path, "test-filler")
	if mode == "blocks" {
		fillBlocks(t, filler)
	} else {
		fillInodes(t, filler)
	}
	if err := unix.Statfs(path, &fs); err != nil {
		t.Fatal(err)
	}
	if (mode == "blocks" && fs.Bavail != 0) || (mode == "inodes" && (fs.Ffree != 0 || fs.Bavail == 0)) {
		t.Fatalf("wrong exhaustion: free blocks=%d free inodes=%d", fs.Bavail, fs.Ffree)
	}
	t.Logf("%s exhausted: available bytes=%d inodes=%d", mode, fs.Bavail*uint64(fs.Bsize), fs.Ffree)
	observe(false, "NoSpace")
	checks := 0
	operationID := "cleanup-full-" + mode
	authority := func(_ context.Context, effectStarted bool) error {
		checks++
		if effectStarted {
			return fmt.Errorf("fresh cleanup must not have started")
		}
		return nil
	}
	receipt, digest, cleanupErr := ownership.ReclaimWithResume(context.Background(), path, target, operationID, authority)
	if !errors.Is(cleanupErr, syscall.ENOSPC) || !strings.Contains(cleanupErr.Error(), "persist local cleanup intent") ||
		receipt != (ownership.Receipt{}) || digest != "" || checks != 1 {
		t.Fatalf("cleanup did not stop at write-ahead intent: receipt=%+v digest=%q checks=%d err=%v", receipt, digest, checks, cleanupErr)
	}
	if current, err := os.Stat(copyPath); err != nil || !os.SameFile(copyInfo, current) {
		t.Fatal("failed cleanup intent retired or replaced the source inode", err)
	}
	if data, err := os.ReadFile(copyPayload); err != nil || string(data) != payload {
		t.Fatal("failed cleanup intent changed existing payload", err)
	}
	if err := ownership.VerifyServingPath(path, target); err != nil {
		t.Fatal("failed cleanup intent changed serving identity", err)
	}
	if _, err := os.Stat(filepath.Join(path, ".shiftpv", "retired", target.CopyID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed cleanup intent created a retired copy", err)
	}
	if err := os.RemoveAll(filler); err != nil {
		t.Fatal(err)
	}
	observe(true, "")
	receipt, digest, cleanupErr = ownership.ReclaimWithResume(context.Background(), path, target, operationID, authority)
	if cleanupErr != nil || !receipt.Retired || !receipt.Purged || digest == "" || checks != 3 {
		t.Fatalf("cleanup did not recover with the same operation: receipt=%+v digest=%q checks=%d err=%v", receipt, digest, checks, cleanupErr)
	}
	if _, err := os.Stat(copyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovered exact cleanup retained its source path", err)
	}
	if *repository.pool.Status.MountIdentity != anchor {
		t.Fatal("recovery replaced the anchored filesystem identity")
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != payload {
		t.Fatal("existing payload changed", err)
	}
	t.Log("freeing only test filler restored allocation and exact cleanup; unrelated existing payload retained")
}

func fillBlocks(t *testing.T, path string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var offset int64
	for _, size := range []int64{1 << 20, 4096} {
		for {
			err := unix.Fallocate(int(file.Fd()), 0, offset, size)
			if errors.Is(err, syscall.ENOSPC) {
				break
			}
			if err != nil || offset > 128<<20 {
				t.Fatal("block filler exceeded its isolated fixture budget", err)
			}
			offset += size
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
}

func fillInodes(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		file, err := os.OpenFile(filepath.Join(path, fmt.Sprint(index)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if errors.Is(err, syscall.ENOSPC) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("inode filler exceeded its isolated fixture budget")
}

type poolRepository struct{ pool volumeapi.Pool }

func (r *poolRepository) ListPoolRegistrations(context.Context) ([]volumeapi.Pool, error) {
	return []volumeapi.Pool{r.pool}, nil
}

func (r *poolRepository) SetPoolStatus(_ context.Context, name, uid, node string, status volumeapi.PoolStatus) error {
	if name != r.pool.Name || uid != r.pool.UID || node != r.pool.NodeName {
		return volumeapi.ErrStateConflict
	}
	r.pool.Status = status
	return nil
}
