package capacityunit

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type sysfsBackend struct{ root, control string }

// NewResolver uses the node's sysfs and host device-mapper control device.
// Only read-only table queries are issued; no device or filesystem is created.
func NewResolver(sysRoot, controlPath string) *Resolver {
	return &Resolver{backend: sysfsBackend{root: sysRoot, control: controlPath}}
}

func (b sysfsBackend) inspect(device string) (block, error) {
	if !filepath.IsAbs(b.root) || !filepath.IsAbs(b.control) {
		return block{}, fmt.Errorf("sysfs and device-mapper control paths must be absolute")
	}
	if _, _, err := parseDevice(device); err != nil {
		return block{}, err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(b.root, "dev/block", device))
	if err != nil {
		return block{}, fmt.Errorf("inspect backing block device %s: %w", device, err)
	}
	size, err := readNumber(filepath.Join(path, "size"))
	if err != nil {
		return block{}, err
	}
	result := block{size: size}
	if _, err := os.Stat(filepath.Join(path, "partition")); err == nil {
		result.start, err = readNumber(filepath.Join(path, "start"))
		if err != nil {
			return block{}, err
		}
		parent, err := os.ReadFile(filepath.Join(filepath.Dir(path), "dev"))
		if err != nil {
			return block{}, err
		}
		result.parent = strings.TrimSpace(string(parent))
		return result, nil
	} else if !os.IsNotExist(err) {
		return block{}, err
	}
	if _, err := os.Stat(filepath.Join(path, "dm")); err == nil {
		result.mapped = true
		result.segments, err = readDMTable(b.control, device)
		return result, err
	} else if !os.IsNotExist(err) {
		return block{}, err
	}
	if strings.Contains(filepath.ToSlash(path), "/virtual/block/") {
		return block{}, fmt.Errorf("virtual block allocation %s is unsupported", device)
	}
	slaves, err := os.ReadDir(filepath.Join(path, "slaves"))
	if err != nil {
		return block{}, err
	}
	if len(slaves) != 0 {
		return block{}, fmt.Errorf("stacked block allocation %s is unsupported", device)
	}
	result.identity, err = backingIdentity(path, device)
	if err != nil {
		return block{}, err
	}
	return result, nil
}

// Prefer the kernel's WWID when exposed so multiple native paths to the same
// allocation cannot become separate capacity. Otherwise identity is scoped to
// this node's kernel device namespace; hidden hypervisor/SAN aliases are not
// evidence this inspector can establish.
func backingIdentity(path, device string) (string, error) {
	for _, name := range []string{"wwid", "device/wwid"} {
		data, err := os.ReadFile(filepath.Join(path, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(string(data))
		if value == "" {
			return "", fmt.Errorf("backing WWID is empty")
		}
		return fmt.Sprintf("wwid:%x", sha256.Sum256([]byte(value))), nil
	}
	return device, nil
}

// ValidateDevice requires a canonical kernel major:minor identity.
func ValidateDevice(device string) error {
	_, _, err := parseDevice(device)
	return err
}

func readNumber(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid block allocation number")
	}
	return value, nil
}
func parseDevice(device string) (uint32, uint32, error) {
	fields := strings.Split(device, ":")
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("invalid block device identity")
	}
	major, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid block device major")
	}
	minor, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid block device minor")
	}
	if device != fmt.Sprintf("%d:%d", major, minor) {
		return 0, 0, fmt.Errorf("block device identity is not canonical")
	}
	return uint32(major), uint32(minor), nil
}
