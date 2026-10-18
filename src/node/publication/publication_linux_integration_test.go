//go:build linux

package publication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	shiftmount "github.com/project-jelly/ShiftPV/src/node/mount"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
)

func TestLinuxPublicationUnpublishReadOnlyPool(t *testing.T) {
	if os.Getenv("SHIFTPV_LINUX_MOUNT_INTEGRATION") != "1" {
		t.Skip("requires an isolated Linux mount namespace")
	}
	parent := os.Getenv("SHIFTPV_PARENT_MOUNT_NAMESPACE")
	current, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil || os.Geteuid() != 0 || parent == "" || current == parent {
		t.Fatalf("requires root in a private mount namespace: parent=%q current=%q err=%v", parent, current, err)
	}

	ctx := context.Background()
	root := t.TempDir()
	poolRoot, targetRoot := filepath.Join(root, "pool"), filepath.Join(root, "pods")
	for _, path := range []string{poolRoot, targetRoot} {
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mount("tmpfs", poolRoot, "tmpfs", 0, "size=16m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Unmount(poolRoot, 0); err != nil {
			t.Errorf("unmount test Pool: %v", err)
		}
	})
	if err := ownership.PrepareServing(ctx, poolRoot, testCopy(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	request := unpublishRequest(poolRoot)
	sentinel := filepath.Join(request.Source, "sentinel")
	if err := os.WriteFile(sentinel, []byte("preserve original copy"), 0600); err != nil {
		t.Fatal(err)
	}
	binder := shiftmount.NewBinder(targetRoot)
	volumes := &fakeRegistry{state: readyState()}
	publisher := newPublisher(poolRoot, binder, volumes, nil)
	publisher.TargetRoot = targetRoot
	targets := []string{filepath.Join(targetRoot, "pod-a", "mount"), filepath.Join(targetRoot, "pod-b", "mount")}
	t.Cleanup(func() {
		for _, target := range targets {
			_ = binder.Unpublish(request.VolumeID, target)
		}
	})
	for _, target := range targets {
		publish := publishRequest(poolRoot)
		publish.TargetPath = target
		if err := publisher.Publish(ctx, publish); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mount("", poolRoot, "", syscall.MS_REMOUNT|syscall.MS_RDONLY, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(poolRoot, "must-not-write"), nil, 0600); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("test Pool is not read-only: %v", err)
	}
	if file, err := os.OpenFile(filepath.Join(poolRoot, ".shiftpv", "lock-"+request.VolumeID), os.O_RDWR, 0); !errors.Is(err, syscall.EROFS) {
		if file != nil {
			file.Close()
		}
		t.Fatalf("write-open lock did not reproduce EROFS: %v", err)
	}
	for index, target := range targets {
		if err := binder.Unpublish(request.VolumeID, target); err != nil {
			t.Fatalf("actual unmount %d: %v", index, err)
		}
		entered, err := publisher.Unpublish(ctx, request)
		if err != nil || !entered {
			t.Fatalf("reconcile after actual unmount %d on read-only Pool: entered=%v err=%v", index, entered, err)
		}
		if volumes.published != (index == 0) || volumes.publishedSet != index+1 {
			t.Fatalf("incorrect remaining reference: index=%d published=%v reconciles=%d", index, volumes.published, volumes.publishedSet)
		}
	}
	if entered, err := publisher.Unpublish(ctx, request); err != nil || !entered || volumes.published {
		t.Fatalf("idempotent read-only unpublish: entered=%v published=%v err=%v", entered, volumes.published, err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "preserve original copy" {
		t.Fatalf("authoritative data changed: data=%q err=%v", data, err)
	}
	if !reflect.DeepEqual(volumes.state, readyState()) {
		t.Fatalf("unpublish changed copy authority: %#v", volumes.state)
	}
}
