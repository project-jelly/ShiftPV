//go:build linux

package capacityunit

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// DM_TABLE_STATUS with DM_STATUS_TABLE_FLAG reads the active mapping only.
// The ABI defines Next relative to the first target, not to the current one:
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/dm-ioctl.h
func readDMTable(control, device string) ([]segment, error) {
	major, minor, err := parseDevice(device)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(control)
	if err != nil {
		return nil, fmt.Errorf("open device-mapper allocation inspector: %w", err)
	}
	defer file.Close()
	storage := make([]uint64, 8192) // aligned, bounded 64 KiB response
	data := unsafe.Slice((*byte)(unsafe.Pointer(&storage[0])), len(storage)*8)
	header := (*unix.DmIoctl)(unsafe.Pointer(&storage[0]))
	header.Version = [3]uint32{4, 0, 0}
	header.Data_size = uint32(len(data))
	header.Data_start = unix.SizeofDmIoctl
	header.Dev = unix.Mkdev(major, minor)
	header.Flags = unix.DM_STATUS_TABLE_FLAG
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), unix.DM_TABLE_STATUS, uintptr(unsafe.Pointer(header)))
	runtime.KeepAlive(storage)
	if errno != 0 {
		return nil, fmt.Errorf("read active device-mapper allocation: %w", errno)
	}
	if header.Flags&unix.DM_BUFFER_FULL_FLAG != 0 || header.Flags&unix.DM_ACTIVE_PRESENT_FLAG == 0 || header.Flags&unix.DM_SUSPEND_FLAG != 0 {
		return nil, fmt.Errorf("active device-mapper allocation is incomplete or suspended")
	}
	return decodeDMTable(data, header.Data_start, header.Data_size, header.Target_count)
}

func decodeDMTable(data []byte, start, size, count uint32) ([]segment, error) {
	if size > uint32(len(data)) || start < unix.SizeofDmIoctl || start >= size || count == 0 || count > maxExtents {
		return nil, fmt.Errorf("device-mapper allocation response is invalid")
	}
	offset := start
	var segments []segment
	for i := uint32(0); i < count; i++ {
		if offset%8 != 0 || offset > size || size-offset < unix.SizeofDmTargetSpec {
			return nil, fmt.Errorf("device-mapper target response is truncated")
		}
		target := data[offset : offset+unix.SizeofDmTargetSpec]
		next := binary.NativeEndian.Uint32(target[20:24])
		end := size
		if i+1 < count {
			if next > size-start || next <= offset-start+unix.SizeofDmTargetSpec {
				return nil, fmt.Errorf("device-mapper target offset is invalid")
			}
			end = start + next
		}
		kind := strings.TrimRight(string(target[24:40]), "\x00")
		if kind != "linear" {
			return nil, fmt.Errorf("device-mapper target %q does not prove fixed allocation", kind)
		}
		parameters := data[offset+unix.SizeofDmTargetSpec : end]
		terminator := bytes.IndexByte(parameters, 0)
		if terminator < 0 {
			return nil, fmt.Errorf("device-mapper target parameters are incomplete")
		}
		fields := strings.Fields(string(parameters[:terminator]))
		if len(fields) != 2 {
			return nil, fmt.Errorf("linear allocation parameters are invalid")
		}
		if _, _, err := parseDevice(fields[0]); err != nil {
			return nil, err
		}
		backingStart, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("linear backing offset is invalid")
		}
		segments = append(segments, segment{start: binary.NativeEndian.Uint64(target[:8]), sectors: binary.NativeEndian.Uint64(target[8:16]), device: fields[0], offset: backingStart})
		offset = end
	}
	return segments, nil
}
