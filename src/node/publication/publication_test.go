package publication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type fakeBinder struct {
	publishedSource string
	publishedTarget string
	publishErr      error
	inspected       int
	remaining       bool
	remainingErr    error
}

func (f *fakeBinder) Publish(source, target string) error {
	f.publishedSource, f.publishedTarget = source, target
	return f.publishErr
}

func (f *fakeBinder) Unpublish(string, string) error { return nil }

func (f *fakeBinder) HasPublishedTarget(string, string) (bool, error) {
	f.inspected++
	return f.remaining, f.remainingErr
}

type fakeRegistry struct {
	states       []volumeapi.State
	state        volumeapi.State
	getErr       error
	beginCalls   int
	beginErr     error
	published    bool
	publishedSet int
	reconcileErr error
	calls        int
}

func (f *fakeRegistry) Get(context.Context, string) (volumeapi.State, error) {
	call := f.calls
	f.calls++
	if call < len(f.states) {
		return f.states[call], f.getErr
	}
	return f.state, f.getErr
}

func (f *fakeRegistry) BeginPublish(context.Context, string, string, volume.CopyIdentity) error {
	f.beginCalls++
	if f.beginErr != nil {
		return f.beginErr
	}
	f.published = true
	return nil
}

func (f *fakeRegistry) ReconcilePublished(_ context.Context, _ string, _ string, _ volume.CopyIdentity, published bool) error {
	f.publishedSet++
	if f.reconcileErr != nil {
		return f.reconcileErr
	}
	f.published = published
	return nil
}

func testCopy() volume.CopyIdentity {
	return volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-0123456789abcdef0123456789abcdef", VolumeUID: "volume-uid",
		CopyID: "copy-id", NodeName: "worker-a", Role: volume.RoleServing,
	}
}

func testPool() volumeapi.Pool {
	copy := testCopy()
	return volumeapi.Pool{Name: copy.PoolName, UID: copy.PoolUID, NodeName: copy.NodeName, MountPath: "/pool"}
}

func servingRoot(t *testing.T) string {
	t.Helper()
	poolRoot := filepath.Join(t.TempDir(), "pool")
	if err := os.Mkdir(poolRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, testCopy(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	return poolRoot
}

func newPublisher(poolRoot string, binder Binder, volumes VolumeRegistry, pools PoolResolver) Publisher {
	if pools == nil {
		pools = func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
			return testPool(), poolRoot, nil
		}
	}
	return Publisher{
		NodeName: "worker-a", TargetRoot: "/var/lib/kubelet/pods",
		Binder: binder, Volumes: volumes, Pools: pools,
	}
}

func readyState() volumeapi.State {
	copy := testCopy()
	return volumeapi.State{UID: copy.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: copy.NodeName, CurrentCopy: &copy}
}

func publishRequest(poolRoot string) PublishRequest {
	copy := testCopy()
	return PublishRequest{
		VolumeID:   copy.VolumeID,
		TargetPath: "/var/lib/kubelet/pods/uid/volumes/csi/mount",
		Source:     filepath.Join(poolRoot, "volumes", copy.VolumeID),
		PoolRoot:   poolRoot,
		Copy:       copy,
	}
}

func unpublishRequest(poolRoot string) UnpublishRequest {
	copy := testCopy()
	return UnpublishRequest{
		VolumeID: copy.VolumeID,
		Source:   filepath.Join(poolRoot, "volumes", copy.VolumeID),
		PoolRoot: poolRoot,
		StateUID: copy.VolumeUID,
		Copy:     copy,
	}
}

func TestPublishRecordsIntentBeforeBinding(t *testing.T) {
	poolRoot := servingRoot(t)
	binder := &fakeBinder{}
	volumes := &fakeRegistry{state: readyState()}
	request := publishRequest(poolRoot)

	if err := newPublisher(poolRoot, binder, volumes, nil).Publish(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if volumes.beginCalls != 1 || !volumes.published {
		t.Fatalf("publication intent was not recorded: %#v", volumes)
	}
	if binder.publishedSource != request.Source || binder.publishedTarget != request.TargetPath {
		t.Fatalf("unexpected bind: %#v", binder)
	}
}

func TestPublishFailsClosedWhenPoolChangesUnderLock(t *testing.T) {
	poolRoot := servingRoot(t)
	binder := &fakeBinder{}
	volumes := &fakeRegistry{state: readyState()}
	replacement := testPool()
	replacement.UID = "replacement-pool-uid"
	pools := func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
		return replacement, poolRoot, nil
	}

	err := newPublisher(poolRoot, binder, volumes, pools).Publish(context.Background(), publishRequest(poolRoot))
	if !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("replacement Pool error = %v", err)
	}
	if volumes.beginCalls != 0 || binder.publishedSource != "" {
		t.Fatalf("replacement Pool reached publish effects: volumes=%#v binder=%#v", volumes, binder)
	}
}

func TestPublishFailsClosedWhenPoolCannotBeRefreshed(t *testing.T) {
	poolRoot := servingRoot(t)
	volumes := &fakeRegistry{state: readyState()}
	pools := func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
		return volumeapi.Pool{}, "", apierrors.NewTimeoutError("Pool read timed out", 1)
	}

	err := newPublisher(poolRoot, &fakeBinder{}, volumes, pools).Publish(context.Background(), publishRequest(poolRoot))
	if !apierrors.IsTimeout(err) {
		t.Fatalf("Pool refresh error = %v", err)
	}
	if volumes.beginCalls != 0 {
		t.Fatalf("Pool refresh failure reached publish effects: %#v", volumes)
	}
}

func TestPublishFailsClosedWhenAuthorityChangesUnderLock(t *testing.T) {
	poolRoot := servingRoot(t)
	volumes := &fakeRegistry{state: volumeapi.State{Phase: volumeapi.PhaseMoving, OwnerNode: "worker-a"}}

	err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Publish(context.Background(), publishRequest(poolRoot))
	if !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("authority change error = %v", err)
	}
	if volumes.beginCalls != 0 {
		t.Fatalf("authority change reached publish effects: %#v", volumes)
	}
}

func TestPublishRejectsUnverifiableServingCopy(t *testing.T) {
	poolRoot := filepath.Join(t.TempDir(), "pool")
	if err := os.Mkdir(poolRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ownership.PrepareServing(context.Background(), poolRoot, testCopy(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	foreign := testCopy()
	foreign.CopyID = "other-copy"
	state := readyState()
	state.CurrentCopy = &foreign
	volumes := &fakeRegistry{state: state}
	request := publishRequest(poolRoot)
	request.Copy = foreign

	if err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Publish(context.Background(), request); err == nil {
		t.Fatal("unverified serving copy was published")
	}
	if volumes.beginCalls != 0 {
		t.Fatalf("unverified serving copy reached publish effects: %#v", volumes)
	}
}

func TestPublishFailureReconcilesActualRemainingMounts(t *testing.T) {
	for name, remaining := range map[string]bool{"another target remains": true, "no target remains": false} {
		t.Run(name, func(t *testing.T) {
			poolRoot := servingRoot(t)
			mountErr := errors.New("mount failed")
			binder := &fakeBinder{publishErr: mountErr, remaining: remaining}
			volumes := &fakeRegistry{state: readyState()}

			err := newPublisher(poolRoot, binder, volumes, nil).Publish(context.Background(), publishRequest(poolRoot))
			if !errors.Is(err, mountErr) {
				t.Fatalf("bind failure error = %v", err)
			}
			if binder.inspected != 1 || volumes.published != remaining {
				t.Fatalf("publication was not reconciled to the actual mount state: binder=%#v volumes=%#v", binder, volumes)
			}
		})
	}
}

func TestPublishFailureJoinsInspectionFailure(t *testing.T) {
	poolRoot := servingRoot(t)
	mountErr := errors.New("mount failed")
	inspectErr := errors.New("mountinfo unavailable")
	binder := &fakeBinder{publishErr: mountErr, remainingErr: inspectErr}
	volumes := &fakeRegistry{state: readyState()}

	err := newPublisher(poolRoot, binder, volumes, nil).Publish(context.Background(), publishRequest(poolRoot))
	if !errors.Is(err, mountErr) || !errors.Is(err, inspectErr) {
		t.Fatalf("inspection failure error = %v", err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("unknown mount state changed publication state: %#v", volumes)
	}
}

func TestUnpublishReconcilesUnderEnteredLock(t *testing.T) {
	poolRoot := servingRoot(t)
	binder := &fakeBinder{}
	volumes := &fakeRegistry{state: readyState(), published: true}

	enteredLock, err := newPublisher(poolRoot, binder, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if err != nil || !enteredLock {
		t.Fatalf("enteredLock=%v err=%v", enteredLock, err)
	}
	if binder.inspected != 1 || volumes.published {
		t.Fatalf("publication was not reconciled: binder=%#v volumes=%#v", binder, volumes)
	}
}

func TestUnpublishReportsLockThatWasNeverEntered(t *testing.T) {
	poolRoot := filepath.Join(t.TempDir(), "pool")
	if err := os.Mkdir(poolRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	volumes := &fakeRegistry{state: readyState()}

	enteredLock, err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if err == nil || enteredLock {
		t.Fatalf("enteredLock=%v err=%v", enteredLock, err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("unopened Pool changed publication state: %#v", volumes)
	}
}

func TestUnpublishLeavesStateWhenPoolIdentityIsGone(t *testing.T) {
	poolRoot := servingRoot(t)
	volumes := &fakeRegistry{state: readyState()}
	pools := func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
		return volumeapi.Pool{}, "", apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvpools"}, "pool-a")
	}

	enteredLock, err := newPublisher(poolRoot, &fakeBinder{}, volumes, pools).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !errors.Is(err, ErrIdentityUnavailable) || !enteredLock {
		t.Fatalf("enteredLock=%v err=%v", enteredLock, err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("absent Pool identity changed publication state: %#v", volumes)
	}
}

func TestUnpublishRetriesWhenPoolReadIsTransient(t *testing.T) {
	poolRoot := servingRoot(t)
	volumes := &fakeRegistry{state: readyState()}
	pools := func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
		return volumeapi.Pool{}, "", errors.New("pool down")
	}

	_, err := newPublisher(poolRoot, &fakeBinder{}, volumes, pools).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !errors.Is(err, ErrObservationRetry) {
		t.Fatalf("transient Pool read error = %v", err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("transient Pool read changed publication state: %#v", volumes)
	}
}

func TestUnpublishLeavesStateWhenPoolWasReplacedUnderLock(t *testing.T) {
	poolRoot := servingRoot(t)
	volumes := &fakeRegistry{state: readyState()}
	replacement := testPool()
	replacement.UID = "replacement-pool-uid"
	pools := func(context.Context, volume.CopyIdentity) (volumeapi.Pool, string, error) {
		return replacement, poolRoot, nil
	}

	_, err := newPublisher(poolRoot, &fakeBinder{}, volumes, pools).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !errors.Is(err, ErrIdentityUnavailable) {
		t.Fatalf("replacement Pool error = %v", err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("replacement Pool changed publication state: %#v", volumes)
	}
}

func TestUnpublishSkipsReconcileAfterOwnerCommittedElsewhere(t *testing.T) {
	poolRoot := servingRoot(t)
	destination := testCopy()
	destination.NodeName, destination.CopyID = "worker-b", "destination-copy"
	volumes := &fakeRegistry{state: volumeapi.State{
		UID: destination.VolumeUID, Phase: volumeapi.PhaseReady, OwnerNode: destination.NodeName, CurrentCopy: &destination,
	}}

	enteredLock, err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if err != nil || !enteredLock {
		t.Fatalf("enteredLock=%v err=%v", enteredLock, err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("committed owner publication state was changed: %#v", volumes)
	}
}

func TestUnpublishFailsClosedWhenVolumeIdentityRotated(t *testing.T) {
	poolRoot := servingRoot(t)
	rotated := readyState()
	rotated.UID = "rotated-volume-uid"
	volumes := &fakeRegistry{state: rotated}

	_, err := newPublisher(poolRoot, &fakeBinder{}, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !errors.Is(err, volumeapi.ErrStateConflict) {
		t.Fatalf("rotated volume identity error = %v", err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("rotated volume identity changed publication state: %#v", volumes)
	}
}

func TestUnpublishFailsClosedWhenRemainingTargetsCannotBeInspected(t *testing.T) {
	poolRoot := servingRoot(t)
	inspectErr := errors.New("mountinfo unavailable")
	volumes := &fakeRegistry{state: readyState()}

	_, err := newPublisher(poolRoot, &fakeBinder{remainingErr: inspectErr}, volumes, nil).Unpublish(context.Background(), unpublishRequest(poolRoot))
	if !errors.Is(err, inspectErr) {
		t.Fatalf("inspection error = %v", err)
	}
	if volumes.publishedSet != 0 {
		t.Fatalf("unknown mount state changed publication state: %#v", volumes)
	}
}

func TestCopyMatchesPool(t *testing.T) {
	copy := testCopy()
	if !CopyMatchesPool(copy, testPool()) {
		t.Fatal("registered Pool did not match its own copy identity")
	}
	for name, mutate := range map[string]func(*volumeapi.Pool){
		"name":    func(p *volumeapi.Pool) { p.Name = "other" },
		"uid":     func(p *volumeapi.Pool) { p.UID = "other" },
		"node":    func(p *volumeapi.Pool) { p.NodeName = "worker-b" },
		"unnamed": func(p *volumeapi.Pool) { *p = volumeapi.Pool{} },
	} {
		t.Run(name, func(t *testing.T) {
			pool := testPool()
			mutate(&pool)
			if CopyMatchesPool(copy, pool) {
				t.Fatalf("copy matched a changed Pool: %#v", pool)
			}
		})
	}
}

func TestPoolIdentityUnavailable(t *testing.T) {
	for name, test := range map[string]struct {
		err  error
		want bool
	}{
		"not found":     {apierrors.NewNotFound(schema.GroupResource{Group: "shiftpv.io", Resource: "shiftpvpools"}, "pool-a"), true},
		"pool absent":   {volumeapi.ErrPoolNotFound, true},
		"misconfigured": {volumeapi.ErrPoolConfiguration, true},
		"timeout":       {apierrors.NewTimeoutError("Pool read timed out", 1), false},
		"unknown":       {errors.New("pool down"), false},
		"none":          {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := PoolIdentityUnavailable(test.err); got != test.want {
				t.Fatalf("PoolIdentityUnavailable(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}
