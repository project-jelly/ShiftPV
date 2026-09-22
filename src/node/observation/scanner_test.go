//go:build linux || darwin

package observation

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/ownership"
	"github.com/project-jelly/ShiftPV/src/volume"
)

type installation struct {
	id  string
	err error
}

func (i installation) InstallationID(context.Context) (string, error) { return i.id, i.err }

type publications struct {
	published bool
	err       error
}

func (p publications) HasPublishedTarget(string, string) (bool, error) { return p.published, p.err }

func TestScannerReleasesExactEmptyPoolForReregistration(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	old := ownership.PoolIdentity{InstallationID: "installation", PoolUID: "old-pool-uid"}
	store, err := ownership.Open(root, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	scanner := &Scanner{HostRoot: host, Installation: installation{id: old.InstallationID}}
	if err := scanner.ReleasePool(context.Background(), volumeapi.Pool{UID: old.PoolUID, MountPath: "/pool"}); err != nil {
		t.Fatal(err)
	}
	replacement, err := ownership.Open(root, ownership.PoolIdentity{InstallationID: old.InstallationID, PoolUID: "new-pool-uid"})
	if err != nil {
		t.Fatalf("same path did not accept replacement Pool identity: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestScannerReleasePoolFailsClosedWithoutExactIdentity(t *testing.T) {
	pool := volumeapi.Pool{UID: "pool-uid", MountPath: "/pool"}
	if err := (*Scanner)(nil).ReleasePool(context.Background(), pool); !errors.Is(err, ownership.ErrIdentity) {
		t.Fatalf("nil scanner error=%v", err)
	}
	if err := (&Scanner{HostRoot: "/host", Installation: installation{err: errors.New("identity unavailable")}}).ReleasePool(context.Background(), pool); err == nil || err.Error() != "identity unavailable" {
		t.Fatalf("installation error=%v", err)
	}
}

func TestScannerReportsExactCopiesAndPreservesUnrecordedPaths(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	identity := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
		VolumeID: "shiftpv-11111111111111111111111111111111", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), root, identity, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(root, "volumes", "shiftpv-22222222222222222222222222222222")
	if err := os.Mkdir(unknown, 0755); err != nil {
		t.Fatal(err)
	}
	scanner := &Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: identity.InstallationID}, Publications: publications{published: true}, Limit: 256}
	result := scanner.Scan(context.Background(), volumeapi.Pool{Name: identity.PoolName, UID: identity.PoolUID, NodeName: identity.NodeName, MountPath: "/pool"}, time.Unix(1, 0))
	if result.Valid || result.Message != "CopyObservationProblem" || result.Truncated || len(result.Copies) != 2 {
		t.Fatalf("inventory=%#v", result)
	}
	var exact, preserved bool
	for _, item := range result.Copies {
		exact = exact || item.Identity != nil && *item.Identity == identity && item.Present && item.Published
		preserved = preserved || item.Identity == nil && item.Present && item.Problem == "UnrecordedPath"
	}
	if !exact || !preserved {
		t.Fatalf("exact=%v preserved=%v inventory=%#v", exact, preserved, result.Copies)
	}
	if _, err := os.Stat(unknown); err != nil {
		t.Fatal("inventory modified unrecorded data")
	}
}

func TestScannerIsBoundedAndSurfacesIdentityFailure(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.MkdirAll(filepath.Join(root, "volumes"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"one", "two"} {
		if err := os.Mkdir(filepath.Join(root, "volumes", name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/pool"}
	result := (&Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 1}).Scan(context.Background(), pool, time.Now())
	if result.Valid || result.Message != "CopyObservationProblem" || !result.Truncated || len(result.Copies) != 1 {
		t.Fatalf("bounded inventory=%#v", result)
	}
	failed := (&Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{err: errors.New("api unavailable")}, Publications: publications{}, Limit: 1}).Scan(context.Background(), pool, time.Now())
	if failed.Valid || failed.Message == "" {
		t.Fatalf("identity failure hidden: %#v", failed)
	}
}

func TestScannerRejectsCopyRegisteredToDifferentPoolNameOrNode(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*volume.CopyIdentity)
	}{
		{name: "pool name", mutate: func(identity *volume.CopyIdentity) { identity.PoolName = "other-pool" }},
		{name: "node", mutate: func(identity *volume.CopyIdentity) { identity.NodeName = "other-node" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := t.TempDir()
			root := filepath.Join(host, "pool")
			if err := os.Mkdir(root, 0755); err != nil {
				t.Fatal(err)
			}
			identity := volume.CopyIdentity{
				InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid",
				VolumeID: "shiftpv-44444444444444444444444444444444", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
			}
			test.mutate(&identity)
			if err := ownership.PrepareServing(context.Background(), root, identity, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			pool := volumeapi.Pool{Name: "pool-a", UID: identity.PoolUID, NodeName: "node-a", MountPath: "/pool"}
			result := (&Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: identity.InstallationID}, Publications: publications{}, Limit: 256}).Scan(context.Background(), pool, time.Now())
			if result.Valid || result.Message != "CopyObservationProblem" || len(result.Copies) != 1 || result.Copies[0].Problem != "PoolIdentityMismatch" {
				t.Fatalf("cross-pool identity accepted: %#v", result)
			}
		})
	}
}

func TestScannerDoesNotTruncateAtExactLimit(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.MkdirAll(filepath.Join(root, "volumes"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "volumes", "only"), 0755); err != nil {
		t.Fatal(err)
	}
	pool := volumeapi.Pool{Name: "pool-a", UID: "pool-uid", NodeName: "node-a", MountPath: "/pool"}
	result := (&Scanner{HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: "installation"}, Publications: publications{}, Limit: 1}).Scan(context.Background(), pool, time.Now())
	if result.Valid || result.Message != "CopyObservationProblem" || result.Truncated || len(result.Copies) != 1 {
		t.Fatalf("exact-limit inventory=%#v", result)
	}
}

func TestScannerFailsClosedWhenPublicationObservationFails(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	identity := volume.CopyIdentity{
		InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: "shiftpv-33333333333333333333333333333333",
		VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing,
	}
	if err := ownership.PrepareServing(context.Background(), root, identity, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	scanner := &Scanner{
		HostRoot: host, TargetRoot: "/kubelet/pods", Installation: installation{id: identity.InstallationID},
		Publications: publications{err: errors.New("mount namespace unavailable")}, Limit: 256,
	}
	result := scanner.Scan(context.Background(), volumeapi.Pool{Name: identity.PoolName, UID: identity.PoolUID, NodeName: identity.NodeName, MountPath: "/pool"}, time.Now())
	if result.Valid || result.Message != "CopyObservationProblem" || len(result.Copies) != 1 || result.Copies[0].Problem == "" || result.Copies[0].Published {
		t.Fatalf("publication observation failure was not preserved: %#v", result)
	}
}

func TestScannerRefreshesPublicationSnapshotOncePerScan(t *testing.T) {
	host := t.TempDir()
	root := filepath.Join(host, "pool")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	identity := volume.CopyIdentity{InstallationID: "installation", PoolName: "pool-a", PoolUID: "pool-uid", VolumeID: "shiftpv-11111111111111111111111111111111", VolumeUID: "volume-uid", CopyID: "copy-id", NodeName: "node-a", Role: volume.RoleServing}
	for _, id := range []string{"shiftpv-11111111111111111111111111111111", "shiftpv-22222222222222222222222222222222"} {
		copy := identity
		copy.VolumeID, copy.VolumeUID, copy.CopyID = id, id, id
		if err := ownership.PrepareServing(context.Background(), root, copy, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	scanner := &Scanner{HostRoot: host, TargetRoot: "/pods", Installation: installation{id: identity.InstallationID}, Publications: publications{err: errors.New("live query must not run")}, Limit: 256}
	scanner.SnapshotPublications = func() (Publications, error) { calls++; return publications{published: calls == 1}, nil }
	pool := volumeapi.Pool{Name: identity.PoolName, UID: identity.PoolUID, NodeName: identity.NodeName, MountPath: "/pool"}
	for pass := 1; pass <= 2; pass++ {
		result := scanner.Scan(context.Background(), pool, time.Now())
		if calls != pass || !result.Valid || len(result.Copies) != 2 {
			t.Fatalf("calls=%d result=%+v", calls, result)
		}
		for _, copy := range result.Copies {
			if copy.Published != (pass == 1) {
				t.Fatal("publication snapshot leaked between scans")
			}
		}
	}
	scanner.SnapshotPublications = func() (Publications, error) { return nil, errors.New("unreadable mount table") }
	if result := scanner.Scan(context.Background(), pool, time.Now()); result.Valid || result.Message == "" {
		t.Fatal("failed snapshot accepted as absence")
	}
}
