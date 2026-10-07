package registration

import (
	"fmt"
	"reflect"

	"github.com/project-jelly/ShiftPV/src/pool/capacityunit"
)

const FixedBlock = "FixedBlock"

type Check struct {
	OK      bool
	Known   bool
	Reason  string
	Message string
}

// CapacityUnit is immutable evidence of one filesystem's fixed allocation.
type CapacityUnit struct {
	Device     string                `json:"device"`
	Source     string                `json:"source"`
	Filesystem string                `json:"filesystem"`
	Extents    []capacityunit.Extent `json:"extents"`
}

func (u *CapacityUnit) Validate() error {
	if u == nil || capacityunit.ValidateDevice(u.Device) != nil || u.Source == "" || (u.Filesystem != "ext4" && u.Filesystem != "xfs") {
		return fmt.Errorf("fixed filesystem capacity identity is incomplete or unsupported")
	}
	normalized, err := capacityunit.Normalize(u.Extents)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalized, u.Extents) {
		return fmt.Errorf("capacity allocation evidence is not normalized")
	}
	return nil
}

func SameCapacityUnit(left, right *CapacityUnit) bool {
	return left != nil && right != nil && left.Validate() == nil && right.Validate() == nil && reflect.DeepEqual(left, right)
}

// Allocation carries either live node evidence or its current API observation.
// Approved records history; it must not be inferred from today's probe alone.
type Allocation struct {
	UID       string
	NodeName  string
	MountPath string
	Policy    string
	Approved  bool
	Deleting  bool
	Unit      *CapacityUnit
	Evidence  Check
}
