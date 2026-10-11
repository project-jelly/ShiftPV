package capacity

import (
	"fmt"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/volume"
)

// Measure the production Pool ledger with owner and in-flight Move holds.
// This is CPU/allocation evidence, not API or disk provisioning latency.
func BenchmarkReservedBytesForPool(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("volumes-%d", count), func(b *testing.B) {
			volumes := make(map[string]volumeapi.State, count)
			var moves []volumeapi.Move
			for i := range count {
				id := fmt.Sprintf("volume-%d", i)
				copy := &volume.CopyIdentity{PoolUID: fmt.Sprintf("pool-%d", i%4), NodeName: "worker"}
				state := volumeapi.State{OwnerNode: copy.NodeName, CurrentCopy: copy, CapacityBytes: 1 << 20}
				if i%10 == 0 {
					state.ActiveMove = fmt.Sprintf("move-%d", i)
					moves = append(moves, volumeapi.Move{Name: state.ActiveMove, Spec: volumeapi.MoveSpec{VolumeID: id}, Status: volumeapi.MoveStatus{CapacityApproved: true, SourceCopy: copy, DestinationPoolUID: "pool-destination"}})
				}
				volumes[id] = state
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ReservedBytesForPool(volumes, moves, "pool-0"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
