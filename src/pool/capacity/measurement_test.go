package capacity

import (
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

func TestMeasurementEvidenceRejectsMissingOrUnsupportedProof(t *testing.T) {
	base := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool"}
	for _, mutate := range []func(*volumeapi.Pool){
		func(p *volumeapi.Pool) { p.UID = "" },
		func(p *volumeapi.Pool) { p.MountPath = "/" },
		func(p *volumeapi.Pool) { p.CapacityPolicy = volumeapi.PoolCapacityPolicyFixedBlock },
		func(p *volumeapi.Pool) { p.CapacityPolicy = "unknown" },
		func(p *volumeapi.Pool) { p.MountPolicy = volumeapi.PoolMountPolicyRequireMountPoint },
		func(p *volumeapi.Pool) { p.MountPolicy = "unknown" },
	} {
		pool := base
		mutate(&pool)
		if _, err := MeasurementEvidence(pool); err == nil {
			t.Fatalf("unproven Pool accepted: %+v", pool)
		}
		if _, err := UsageConfigurationEvidence(pool); err == nil {
			t.Fatalf("unproven usage Pool accepted: %+v", pool)
		}
	}
}

func TestMeasurementEvidenceWireDigestUnchanged(t *testing.T) {
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool", Generation: 7}
	const want = "5ed757da7bb9fb00990dcc064b41c03546c71e8bdb95058b465749e4f280c610"
	got, err := MeasurementEvidence(pool)
	if err != nil || got != want {
		t.Fatalf("existing helper wire digest changed: %q %v", got, err)
	}
}

func TestUsageConfigurationEvidenceSeparatesRefreshFromConfiguration(t *testing.T) {
	base := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool", PoolGroup: "group", CapacityLimit: "1Gi", Generation: 1,
		CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock, MountPolicy: volumeapi.PoolMountPolicyRequireMountPoint,
		Status: volumeapi.PoolStatus{
			CapacityUnit:  &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}},
			MountIdentity: &volumeapi.PoolMountIdentity{Device: "8:1", Source: "/dev/a", Root: "/", Filesystem: "ext4"}}}
	want, err := UsageConfigurationEvidence(base)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*volumeapi.Pool){
		"name":                func(p *volumeapi.Pool) { p.Name = "other" },
		"UID":                 func(p *volumeapi.Pool) { p.UID = "other" },
		"node":                func(p *volumeapi.Pool) { p.NodeName = "other" },
		"path":                func(p *volumeapi.Pool) { p.MountPath = "/mnt/other" },
		"group":               func(p *volumeapi.Pool) { p.PoolGroup = "other" },
		"limit":               func(p *volumeapi.Pool) { p.CapacityLimit = "2Gi" },
		"capacity policy":     func(p *volumeapi.Pool) { p.CapacityPolicy = "" },
		"mount policy":        func(p *volumeapi.Pool) { p.MountPolicy = "" },
		"capacity device":     func(p *volumeapi.Pool) { p.Status.CapacityUnit.Device = "8:2" },
		"capacity source":     func(p *volumeapi.Pool) { p.Status.CapacityUnit.Source = "/dev/b" },
		"capacity filesystem": func(p *volumeapi.Pool) { p.Status.CapacityUnit.Filesystem = "xfs" },
		"capacity extent":     func(p *volumeapi.Pool) { p.Status.CapacityUnit.Extents[0].Start++ },
		"mount device":        func(p *volumeapi.Pool) { p.Status.MountIdentity.Device = "8:2" },
		"mount source":        func(p *volumeapi.Pool) { p.Status.MountIdentity.Source = "/dev/b" },
		"mount root":          func(p *volumeapi.Pool) { p.Status.MountIdentity.Root = "/subdir" },
		"mount filesystem":    func(p *volumeapi.Pool) { p.Status.MountIdentity.Filesystem = "xfs" },
	} {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(base)
			var changed volumeapi.Pool
			if err := json.Unmarshal(encoded, &changed); err != nil {
				t.Fatal(err)
			}
			change(&changed)
			got, err := UsageConfigurationEvidence(changed)
			if err != nil || got == want {
				t.Fatalf("configuration change was not bound: hash=%q err=%v", got, err)
			}
		})
	}
	base.Generation++
	base.Status.ObservedGeneration++
	base.Status.LastProbeTime = metav1.NewTime(time.Now())
	base.Status.Inventory = &volumeapi.PoolInventory{Valid: true, ObservedAt: metav1.NewTime(time.Now())}
	base.Status.FilesystemTotalBytes = 4096
	if got, err := UsageConfigurationEvidence(base); err != nil || got != want {
		t.Fatalf("observation refresh changed stable configuration: %q %v", got, err)
	}
}

func TestMeasurementEvidenceBindsGenerationAndBacking(t *testing.T) {
	pool := volumeapi.Pool{Name: "pool", UID: "pool-uid", NodeName: "node", MountPath: "/mnt/pool", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock,
		Status: volumeapi.PoolStatus{CapacityUnit: &volumeapi.PoolCapacityUnit{Device: "8:1", Source: "/dev/a", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: 100, Sectors: 100}}}}}
	original, err := MeasurementEvidence(pool)
	if err != nil {
		t.Fatal(err)
	}
	pool.Generation++
	changed, err := MeasurementEvidence(pool)
	if err != nil || changed == original {
		t.Fatal("generation change did not invalidate measurement evidence")
	}
	pool.Generation--
	pool.Status.CapacityUnit.Extents[0].Start++
	changed, err = MeasurementEvidence(pool)
	if err != nil || changed == original {
		t.Fatal("same-device backing change did not invalidate measurement evidence")
	}
}
