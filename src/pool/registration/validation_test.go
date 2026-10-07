package registration

import (
	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
	"testing"
)

func allocation(uid string, start uint64, approved bool) Allocation {
	return Allocation{UID: uid, NodeName: "node", MountPath: "/" + uid, Policy: FixedBlock, Approved: approved,
		Unit:     &CapacityUnit{Device: "8:1", Source: "/dev/test", Filesystem: "ext4", Extents: []capacityunit.Extent{{Device: "8:0", Start: start, Sectors: 1024}}},
		Evidence: Check{Known: true, OK: true}}
}

func TestRegistrationIsolation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(*Allocation, *Allocation)
		left, right string
	}{
		{"independent candidates", func(a, b *Allocation) {}, "", ""},
		{"shared candidates", func(a, b *Allocation) { b.Unit = a.Unit }, "CapacityBackingOverlap", "CapacityBackingOverlap"},
		{"candidate cannot poison approved", func(a, b *Allocation) { a.Approved = true; b.Unit = a.Unit }, "", "CapacityBackingOverlap"},
		{"approved conflicts fail closed", func(a, b *Allocation) { a.Approved = true; b.Approved = true; b.Unit = a.Unit }, "CapacityBackingOverlap", "CapacityBackingOverlap"},
		{"legacy candidate", func(a, b *Allocation) { a.Approved = true; b.Policy = ""; b.Unit = nil }, "", "CapacityIsolationRequired"},
		{"legacy approved", func(a, b *Allocation) { a.Approved = true; a.Policy = ""; a.Unit = nil }, "", "CapacityPeerUnproven"},
		{"unproven candidate", func(a, b *Allocation) {
			a.Approved = true
			b.Unit = nil
			b.Evidence = failed("CapacityAllocationUnproven", "unproven")
		}, "", "CapacityAllocationUnproven"},
		{"unproven approved peer", func(a, b *Allocation) {
			a.Approved = true
			b.Approved = true
			b.Unit = nil
			b.Evidence = failed("CapacityAllocationUnproven", "unproven")
		}, "CapacityPeerUnproven", "CapacityAllocationUnproven"},
		{"nested candidate", func(a, b *Allocation) { a.Approved = true; b.MountPath = a.MountPath + "/nested" }, "", "CapacityPathsOverlap"},
		{"deleting approved still reserves backing", func(a, b *Allocation) { a.Approved = true; a.Deleting = true; b.Unit = a.Unit }, "", "CapacityBackingOverlap"},
		{"other node", func(a, b *Allocation) { b.NodeName = "other"; b.Unit = a.Unit }, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := allocation("a", 0, false), allocation("b", 1024, false)
			tc.change(&a, &b)
			for _, peers := range [][]Allocation{{a, b}, {b, a}} {
				for i, target := range []Allocation{a, b} {
					want := []string{tc.left, tc.right}[i]
					got := Independent(target, peers)
					if got.OK != (want == "") || (!got.OK && got.Reason != want) {
						t.Fatalf("target=%s check=%+v want=%s", target.UID, got, want)
					}
				}
			}
		})
	}
}

func TestUnknownEvidenceDoesNotApprove(t *testing.T) {
	a := allocation("a", 0, false)
	a.Evidence = Check{Reason: "CapacityProbePending"}
	if got := Independent(a, []Allocation{a}); got.OK || got.Known {
		t.Fatalf("unknown became approval or rejection: %+v", got)
	}
	a.Evidence = Check{Known: true, OK: true}
	a.Unit.Extents = nil
	if got := Independent(a, []Allocation{a}); got.OK {
		t.Fatal("invalid extents approved")
	}
}

func TestValidateConfiguration(t *testing.T) {
	valid := Configuration{NodeName: "node", PoolGroup: "default", MountPath: "/mnt/pool", CapacityPolicy: FixedBlock, CapacityLimit: "1Gi"}
	for _, tc := range []struct {
		name   string
		change func(*Configuration)
		valid  bool
	}{
		{"valid", func(c *Configuration) {}, true},
		{"root", func(c *Configuration) { c.MountPath = "/a/.." }, false},
		{"relative", func(c *Configuration) { c.MountPath = "data" }, false},
		{"group", func(c *Configuration) { c.PoolGroup = "Invalid" }, false},
		{"policy", func(c *Configuration) { c.CapacityPolicy = "Thin" }, false},
		{"zero", func(c *Configuration) { c.CapacityLimit = "0" }, false},
		{"negative", func(c *Configuration) { c.CapacityLimit = "-1Gi" }, false},
		{"invalid quantity", func(c *Configuration) { c.CapacityLimit = "lots" }, false},
		{"fractional bytes", func(c *Configuration) { c.CapacityLimit = "1500m" }, false},
		{"overflow", func(c *Configuration) { c.CapacityLimit = "9223372036854775808" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.change(&c)
			if got := ValidateConfiguration(c); got.OK != tc.valid {
				t.Fatalf("check=%+v", got)
			}
		})
	}
}
