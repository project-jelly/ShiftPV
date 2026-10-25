package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	mountutils "k8s.io/mount-utils"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"github.com/project-jelly/ShiftPV/src/pool/readiness"
)

type fixedAllocation []capacityunit.Extent

func (a fixedAllocation) Resolve(string) ([]capacityunit.Extent, error) { return a, nil }

type changingAllocation func(string) ([]capacityunit.Extent, error)

func (a changingAllocation) Resolve(device string) ([]capacityunit.Extent, error) { return a(device) }

// Host observations are injected, but the real Pool verifier and ownership
// locks run. This exercises the capacity check the legacy directory fixtures
// cannot reach, including a same-device LVM backing change.
func fixedPoolService(t *testing.T, binder *fakeBinder) (*Service, volumeapi.Pool, *readiness.ProbeDependencies) {
	t.Helper()
	s := configuredService(t, binder)
	root, err := filepath.EvalSymlinks(s.HostRoot)
	if err != nil {
		t.Fatal(err)
	}
	s.HostRoot = root
	pool := s.Pools.(fakePoolRegistry).pool
	pool.Generation = 1
	pool.CapacityPolicy = volumeapi.PoolCapacityPolicyFixedBlock
	pool.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint
	pool.Status.CapacityUnit = &volumeapi.PoolCapacityUnit{
		Device: "8:1", Source: "/dev/test", Filesystem: "ext4",
		Extents: []capacityunit.Extent{{Device: "8:0", Start: 2048, Sectors: 4096}},
	}
	pool.Status.MountIdentity = &volumeapi.PoolMountIdentity{Device: "8:1", Source: "/dev/test", Filesystem: "ext4", Root: "/"}
	pool.Status.Conditions = []metav1.Condition{{Type: volumeapi.PoolConditionCapacityIndependent, Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	d := readiness.HostProbeDependencies(root)
	d.Allocations = fixedAllocation(pool.Status.CapacityUnit.Extents)
	d.ListMounts = func() ([]mountutils.MountInfo, error) {
		return []mountutils.MountInfo{
			{MountPoint: root, Major: 8, Minor: 2, Source: "/dev/parent", Root: "/", FsType: "ext4"},
			{MountPoint: filepath.Join(root, "pool"), Major: 8, Minor: 1, Source: "/dev/test", Root: "/", FsType: "ext4"},
		}, nil
	}
	setPoolProbe(t, s, d)
	s.Pools = fakePoolRegistry{pool: pool}
	return s, pool, &d
}

func setPoolProbe(t *testing.T, s *Service, d readiness.ProbeDependencies) {
	t.Helper()
	probe, err := readiness.NewProbeWithDependencies(s.HostRoot, d)
	if err != nil {
		t.Fatal(err)
	}
	s.PoolVerifier = probe
}

func TestNodeUnpublishProgressDuringSiblingDeletionScan(t *testing.T) {
	for _, underLock := range []bool{false, true} {
		for _, remaining := range []bool{false, true} {
			t.Run(map[bool]string{false: "before resolve", true: "under lock"}[underLock]+map[bool]string{false: "/last target", true: "/other target remains"}[remaining], func(t *testing.T) {
				binder := &fakeBinder{remaining: remaining}
				s, pool, _ := fixedPoolService(t, binder)
				registry := s.Volumes.(*fakeVolumeRegistry)
				registry.published = true
				fresh := pool
				// Deleting another copy requests a new absence scan via
				// spec.scanEpoch, advancing generation without changing backing.
				pool.Generation++
				first := pool
				if underLock {
					first = fresh
				}
				s.Pools = &sequencedPoolRegistry{pools: []volumeapi.Pool{first, pool}}
				request := &csi.NodeUnpublishVolumeRequest{VolumeId: registry.state.CurrentCopy.VolumeID, TargetPath: validPublishRequest().TargetPath}
				if _, err := s.NodeUnpublishVolume(context.Background(), request); err != nil {
					t.Fatalf("sibling scan blocked unpublish: %v", err)
				}
				if registry.reconcileCalls.Load() != 1 || registry.published != remaining || binder.unpublished != request.TargetPath {
					t.Fatalf("unpublish did not reconcile actual targets: calls=%d published=%v", registry.reconcileCalls.Load(), registry.published)
				}
				if _, err := s.NodePublishVolume(context.Background(), validPublishRequest()); err != nil || binder.publishedTarget == "" {
					t.Fatalf("sibling scan blocked an existing copy's publish: %v", err)
				}
			})
		}
	}
}

func TestNodeUnpublishStaleObservationStillChecksBacking(t *testing.T) {
	for _, scenario := range []string{"missing mount", "changed extents", "changed extents under lock", "missing anchor", "unknown targets"} {
		t.Run(scenario, func(t *testing.T) {
			binder := &fakeBinder{}
			s, pool, deps := fixedPoolService(t, binder)
			pool.Generation++
			switch scenario {
			case "missing mount":
				deps.ListMounts = func() ([]mountutils.MountInfo, error) { return nil, nil }
			case "changed extents":
				deps.Allocations = fixedAllocation{{Device: "8:0", Start: 8192, Sectors: 4096}}
			case "changed extents under lock":
				calls := 0
				deps.Allocations = changingAllocation(func(string) ([]capacityunit.Extent, error) {
					calls++
					if calls == 1 {
						return pool.Status.CapacityUnit.Extents, nil
					}
					return []capacityunit.Extent{{Device: "8:0", Start: 8192, Sectors: 4096}}, nil
				})
			case "missing anchor":
				pool.Status.CapacityUnit = nil
			case "unknown targets":
				binder.remainingErr = errors.New("mount observation unavailable")
			}
			setPoolProbe(t, s, *deps)
			s.Pools = fakePoolRegistry{pool: pool}
			registry := s.Volumes.(*fakeVolumeRegistry)
			registry.published = true
			request := &csi.NodeUnpublishVolumeRequest{VolumeId: registry.state.CurrentCopy.VolumeID, TargetPath: validPublishRequest().TargetPath}
			_, err := s.NodeUnpublishVolume(context.Background(), request)
			if status.Code(err) == codes.OK || registry.reconcileCalls.Load() != 0 || !registry.published || binder.unpublished != request.TargetPath {
				t.Fatalf("uncertain backing/publication was cleared: err=%v calls=%d published=%v", err, registry.reconcileCalls.Load(), registry.published)
			}
		})
	}
}
