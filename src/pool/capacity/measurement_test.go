package capacity

import (
	"testing"

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
