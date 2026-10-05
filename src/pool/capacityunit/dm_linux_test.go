//go:build linux

package capacityunit

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func tableResponse(t testing.TB, kind string) []byte {
	t.Helper()
	storage := make([]uint64, 128)
	data := unsafe.Slice((*byte)(unsafe.Pointer(&storage[0])), len(storage)*8)
	target := (*unix.DmTargetSpec)(unsafe.Pointer(&data[unix.SizeofDmIoctl]))
	target.Sector_start, target.Length = 0, 128
	copy(target.Target_type[:], kind)
	copy(data[unix.SizeofDmIoctl+unix.SizeofDmTargetSpec:], "8:1 256\x00")
	return data
}
func TestDMTableAcceptsOnlyFixedLinearMappings(t *testing.T) {
	for _, kind := range []string{"linear", "thin", "thin-pool", "snapshot", "snapshot-origin", "crypt", "striped", "multipath"} {
		data := tableResponse(t, kind)
		got, err := decodeDMTable(data, unix.SizeofDmIoctl, uint32(len(data)), 1)
		if kind == "linear" {
			if err != nil || len(got) != 1 || got[0].device != "8:1" || got[0].offset != 256 || got[0].sectors != 128 {
				t.Fatalf("linear=%v err=%v", got, err)
			}
		} else if err == nil {
			t.Fatalf("shared or unsupported target %s accepted", kind)
		}
	}
}
func TestDMTableRejectsTruncatedAndMalformedResponses(t *testing.T) {
	for _, mutate := range []func([]byte){
		func(data []byte) { copy(data[unix.SizeofDmIoctl+unix.SizeofDmTargetSpec:], "invalid\x00") },
		func(data []byte) {
			for i := unix.SizeofDmIoctl + unix.SizeofDmTargetSpec; i < len(data); i++ {
				data[i] = 'x'
			}
		},
	} {
		data := tableResponse(t, "linear")
		mutate(data)
		if _, err := decodeDMTable(data, unix.SizeofDmIoctl, uint32(len(data)), 1); err == nil {
			t.Fatal("malformed target accepted")
		}
	}
	data := tableResponse(t, "linear")
	for _, args := range [][3]uint32{{0, uint32(len(data)), 1}, {unix.SizeofDmIoctl, uint32(len(data)) + 1, 1}, {unix.SizeofDmIoctl, uint32(len(data)), 0}, {unix.SizeofDmIoctl, unix.SizeofDmIoctl + 1, 1}, {unix.SizeofDmIoctl, uint32(len(data)), 2}} {
		if _, err := decodeDMTable(data, args[0], args[1], args[2]); err == nil {
			t.Fatalf("invalid table response accepted: %v", args)
		}
	}
}
func TestDMTableOffsetsAreRelativeToFirstTarget(t *testing.T) {
	data := tableResponse(t, "linear")
	for i := 0; i < 3; i++ {
		offset := unix.SizeofDmIoctl + i*64
		target := (*unix.DmTargetSpec)(unsafe.Pointer(&data[offset]))
		target.Sector_start = uint64(i * 128)
		target.Length = 128
		target.Next = uint32((i + 1) * 64)
		copy(target.Target_type[:], "linear")
		copy(data[offset+unix.SizeofDmTargetSpec:], "8:1 256\x00")
	}
	got, err := decodeDMTable(data, unix.SizeofDmIoctl, uint32(len(data)), 3)
	if err != nil || len(got) != 3 || got[2].start != 256 {
		t.Fatalf("multi-target response=%v err=%v", got, err)
	}
}

func FuzzDecodeDMTable(f *testing.F) {
	f.Add([]byte{}, uint32(0), uint32(0), uint32(0))
	data := tableResponse(f, "linear")
	f.Add(data, uint32(unix.SizeofDmIoctl), uint32(len(data)), uint32(1))
	f.Fuzz(func(t *testing.T, data []byte, start, size, count uint32) {
		_, _ = decodeDMTable(data, start, size, count)
	})
}
