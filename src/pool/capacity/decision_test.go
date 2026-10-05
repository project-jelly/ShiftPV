package capacity

import (
	"math"
	"testing"
)

func TestFitsReservationBoundaries(t *testing.T) {
	for _, test := range []struct {
		request, reserved, limit int64
		fits                     bool
	}{
		{1, 0, 1, true}, {1, 1, 1, false}, {1, 2, 1, false}, {0, 0, 1, false}, {1, -1, 1, false},
		{math.MaxInt64, 0, math.MaxInt64, true}, {1, math.MaxInt64, math.MaxInt64, false},
	} {
		if got := FitsReservation(test.request, test.reserved, test.limit); got != test.fits {
			t.Fatalf("%+v got %v", test, got)
		}
	}
}
