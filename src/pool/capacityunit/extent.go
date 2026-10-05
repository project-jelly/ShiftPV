// Package capacityunit resolves fixed block allocations to their backing ranges.
// Different filesystem devices are independent only when these ranges do not overlap.
package capacityunit

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
)

const maxExtents = 256
const maxDepth = 16

// Extent is a fixed allocation in 512-byte sectors on one backing device.
type Extent struct {
	Device  string `json:"device"`
	Start   uint64 `json:"start"`
	Sectors uint64 `json:"sectors"`
}

type segment struct {
	start, sectors uint64
	device         string
	offset         uint64
}
type block struct {
	size     uint64
	identity string
	parent   string
	start    uint64
	mapped   bool
	segments []segment
}
type backend interface{ inspect(string) (block, error) }

// Resolver reads the live kernel block allocation. It rejects thin, snapshot,
// shared or unsupported targets instead of claiming independent capacity.
type Resolver struct{ backend backend }

func (r *Resolver) Resolve(device string) ([]Extent, error) {
	if r == nil || r.backend == nil {
		return nil, fmt.Errorf("block allocation inspector is unavailable")
	}
	b, err := r.backend.inspect(device)
	if err != nil {
		return nil, err
	}
	extents, err := r.resolve(device, 0, b.size, map[string]bool{}, 0)
	if err != nil {
		return nil, err
	}
	return Normalize(extents)
}

func (r *Resolver) resolve(device string, start, sectors uint64, visiting map[string]bool, depth int) ([]Extent, error) {
	if depth >= maxDepth || visiting[device] {
		return nil, fmt.Errorf("block allocation dependency cycle or depth exceeded")
	}
	visiting[device] = true
	defer delete(visiting, device)
	b, err := r.backend.inspect(device)
	if err != nil {
		return nil, err
	}
	if sectors == 0 || start > b.size || sectors > b.size-start {
		return nil, fmt.Errorf("block allocation range is invalid")
	}
	if b.parent != "" {
		if start > math.MaxUint64-b.start {
			return nil, fmt.Errorf("partition offset overflows")
		}
		return r.resolve(b.parent, b.start+start, sectors, visiting, depth+1)
	}
	if !b.mapped {
		identity := b.identity
		if identity == "" {
			identity = device
		}
		return []Extent{{Device: identity, Start: start, Sectors: sectors}}, nil
	}
	if err := validateSegments(b); err != nil {
		return nil, err
	}
	return r.resolveSegments(b, start, sectors, visiting, depth)
}

func validateSegments(b block) error {
	if len(b.segments) == 0 || len(b.segments) > maxExtents {
		return fmt.Errorf("mapped allocation has no complete bounded table")
	}
	var end uint64
	for _, s := range b.segments {
		if s.start != end || s.sectors == 0 || s.sectors > math.MaxUint64-end || s.device == "" {
			return fmt.Errorf("mapped allocation table has gaps or invalid ranges")
		}
		end += s.sectors
	}
	if end != b.size {
		return fmt.Errorf("mapped allocation table does not cover the device")
	}
	return nil
}

func (r *Resolver) resolveSegments(b block, start, sectors uint64, visiting map[string]bool, depth int) ([]Extent, error) {
	var extents []Extent
	for _, s := range b.segments {
		first, last := max(start, s.start), min(start+sectors, s.start+s.sectors)
		if first >= last {
			continue
		}
		offset := first - s.start
		if offset > math.MaxUint64-s.offset {
			return nil, fmt.Errorf("mapped allocation offset overflows")
		}
		resolved, err := r.resolve(s.device, s.offset+offset, last-first, visiting, depth+1)
		if err != nil {
			return nil, err
		}
		extents = append(extents, resolved...)
		if len(extents) > maxExtents {
			return nil, fmt.Errorf("block allocation extent limit exceeded")
		}
	}
	return extents, nil
}

// Normalize returns a stable bounded allocation and rejects internal aliases.
func Normalize(extents []Extent) ([]Extent, error) {
	if len(extents) == 0 || len(extents) > maxExtents {
		return nil, fmt.Errorf("block allocation must contain bounded extents")
	}
	result := append([]Extent(nil), extents...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Device != result[j].Device {
			return result[i].Device < result[j].Device
		}
		return result[i].Start < result[j].Start
	})
	var normalized []Extent
	for _, e := range result {
		if err := validateBackingIdentity(e.Device); err != nil {
			return nil, err
		}
		if e.Sectors == 0 || e.Sectors > math.MaxUint64-e.Start {
			return nil, fmt.Errorf("block allocation extent is invalid")
		}
		if len(normalized) > 0 {
			previous := &normalized[len(normalized)-1]
			if previous.Device == e.Device {
				end := previous.Start + previous.Sectors
				if e.Start < end {
					return nil, fmt.Errorf("block allocation contains overlapping extents")
				}
				if e.Start == end {
					previous.Sectors += e.Sectors
					continue
				}
			}
		}
		normalized = append(normalized, e)
	}
	return normalized, nil
}

func validateBackingIdentity(device string) error {
	if strings.HasPrefix(device, "wwid:") {
		data, err := hex.DecodeString(strings.TrimPrefix(device, "wwid:"))
		if err != nil || len(data) != 32 || device != strings.ToLower(device) {
			return fmt.Errorf("invalid backing WWID digest")
		}
		return nil
	}
	return ValidateDevice(device)
}

// Overlap rejects incomplete evidence as well as shared backing allocations.
func Overlap(left, right []Extent) (bool, error) {
	l, err := Normalize(left)
	if err != nil {
		return false, err
	}
	r, err := Normalize(right)
	if err != nil {
		return false, err
	}
	for _, a := range l {
		for _, b := range r {
			if a.Device == b.Device && a.Start < b.Start+b.Sectors && b.Start < a.Start+a.Sectors {
				return true, nil
			}
		}
	}
	return false, nil
}
