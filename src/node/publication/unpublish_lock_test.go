package publication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/project-jelly/ShiftPV/src/node/ownership"
)

func TestUnpublishSharesExistingLockWithWriters(t *testing.T) {
	ctx := context.Background()
	poolRoot := servingRoot(t)
	copy := testCopy()
	store, err := ownership.OpenExisting(poolRoot, poolIdentity(copy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lock, err := store.Acquire(ctx, copy.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	binder := &fakeBinder{}
	volumes := &fakeRegistry{state: readyState(), published: true}
	publisher := newPublisher(poolRoot, binder, volumes, nil)
	entered, err := publisher.Unpublish(ctx, unpublishRequest(poolRoot))
	if entered || !errors.Is(err, ownership.ErrBusy) {
		t.Fatalf("write lock did not exclude unpublish: entered=%v err=%v", entered, err)
	}
	if volumes.calls != 0 || volumes.publishedSet != 0 || binder.inspected != 0 || !volumes.published {
		t.Fatalf("unpublish passed the busy lock: volumes=%#v binder=%#v", volumes, binder)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	entered, err = publisher.Unpublish(ctx, unpublishRequest(poolRoot))
	if !entered || err != nil || volumes.published || volumes.publishedSet != 1 {
		t.Fatalf("unpublish did not progress after writer left: entered=%v err=%v volumes=%#v", entered, err, volumes)
	}
}

func TestUnpublishInitializesMissingLockOnWritablePool(t *testing.T) {
	poolRoot := servingRoot(t)
	path := filepath.Join(poolRoot, ".shiftpv", "lock-"+testCopy().VolumeID)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	volumes := &fakeRegistry{state: readyState(), published: true}
	entered, err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !entered || err != nil || volumes.published || volumes.publishedSet != 1 {
		t.Fatalf("missing lock did not reconcile: entered=%v err=%v volumes=%#v", entered, err, volumes)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("missing writable lock was not initialized: %v", err)
	}
}

func TestUnpublishDoesNotRetryCallbackNotFound(t *testing.T) {
	poolRoot := servingRoot(t)
	binder := &fakeBinder{remainingErr: os.ErrNotExist}
	volumes := &fakeRegistry{state: readyState(), published: true}
	entered, err := newPublisher(poolRoot, binder, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !entered || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("callback error changed: entered=%v err=%v", entered, err)
	}
	if binder.inspected != 1 || volumes.calls != 1 || volumes.publishedSet != 0 || !volumes.published {
		t.Fatalf("callback was retried or unknown publication cleared: binder=%#v volumes=%#v", binder, volumes)
	}
}
