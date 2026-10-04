//go:build !linux

package capacityunit

import "fmt"

func readDMTable(_, _ string) ([]segment, error) {
	return nil, fmt.Errorf("device-mapper allocation inspection requires Linux")
}
