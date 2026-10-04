package capacityunit

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

type testBackend map[string]block

func (b testBackend) inspect(device string) (block, error) {
	value, ok := b[device]
	if !ok {
		return block{}, fmt.Errorf("unknown block device")
	}
	return value, nil
}
func TestIndependentPartitionsAndThickLVs(t *testing.T) {
	b := testBackend{
		"8:0":   {size: 4096},
		"8:1":   {size: 1024, parent: "8:0", start: 128},
		"8:2":   {size: 1024, parent: "8:0", start: 2048},
		"253:0": {size: 128, mapped: true, segments: []segment{{start: 0, sectors: 128, device: "8:1", offset: 64}}},
		"253:1": {size: 128, mapped: true, segments: []segment{{start: 0, sectors: 128, device: "8:1", offset: 512}}},
		"253:2": {size: 128, mapped: true, segments: []segment{{start: 0, sectors: 128, device: "253:0", offset: 0}}},
	}
	r := &Resolver{backend: b}
	first, err := r.Resolve("253:0")
	if err != nil {
		t.Fatal(err)
	}
	if want := []Extent{{Device: "8:0", Start: 192, Sectors: 128}}; !reflect.DeepEqual(first, want) {
		t.Fatalf("extents=%+v", first)
	}
	for device, want := range map[string]bool{"253:1": false, "8:2": false, "253:2": true, "8:1": true, "8:0": true} {
		other, err := r.Resolve(device)
		if err != nil {
			t.Fatal(err)
		}
		shared, err := Overlap(first, other)
		if err != nil || shared != want {
			t.Fatalf("%s overlap=%v want=%v err=%v", device, shared, want, err)
		}
	}
}
func TestSpanningLVDoesNotReserveUnallocatedDiskRanges(t *testing.T) {
	b := testBackend{"8:0": {size: 1000}, "8:16": {size: 1000}, "253:0": {size: 200, mapped: true, segments: []segment{
		{start: 0, sectors: 100, device: "8:0", offset: 200}, {start: 100, sectors: 100, device: "8:16", offset: 400},
	}}}
	r := &Resolver{backend: b}
	got, err := r.Resolve("253:0")
	if err != nil {
		t.Fatal(err)
	}
	want := []Extent{{Device: "8:0", Start: 200, Sectors: 100}, {Device: "8:16", Start: 400, Sectors: 100}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extents=%+v", got)
	}
}
func TestInvalidAllocationsFailClosed(t *testing.T) {
	for name, b := range map[string]testBackend{
		"cycle":               {"8:0": {size: 64, parent: "8:0"}},
		"gap":                 {"8:0": {size: 64, mapped: true, segments: []segment{{start: 1, sectors: 63, device: "8:16"}}}},
		"short table":         {"8:0": {size: 64, mapped: true, segments: []segment{{start: 0, sectors: 63, device: "8:16"}}}},
		"empty":               {"8:0": {size: 0}},
		"outside backing":     {"8:0": {size: 128, parent: "8:16", start: 1}, "8:16": {size: 128}},
		"overlapping backing": {"8:0": {size: 128, mapped: true, segments: []segment{{start: 0, sectors: 64, device: "8:16"}, {start: 64, sectors: 64, device: "8:16"}}}, "8:16": {size: 128}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (&Resolver{backend: b}).Resolve("8:0"); err == nil {
				t.Fatal("invalid allocation accepted")
			}
		})
	}
}
func TestNormalizeRejectsIncompleteAndOverflowEvidence(t *testing.T) {
	for _, extents := range [][]Extent{nil, {{Device: "8:0", Sectors: 0}}, {{Device: "8:0", Start: math.MaxUint64, Sectors: 1}}} {
		if _, err := Normalize(extents); err == nil {
			t.Fatal("invalid evidence accepted")
		}
		if _, err := Overlap(extents, []Extent{{Device: "8:0", Sectors: 1}}); err == nil {
			t.Fatal("invalid overlap evidence accepted")
		}
	}
	got, err := Normalize([]Extent{{Device: "8:0", Start: 10, Sectors: 10}, {Device: "8:0", Sectors: 10}})
	if err != nil || !reflect.DeepEqual(got, []Extent{{Device: "8:0", Sectors: 20}}) {
		t.Fatalf("normalization=%v err=%v", got, err)
	}
}
