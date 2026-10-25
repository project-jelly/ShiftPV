package capacity

import (
	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/pool/registration"
)

var (
	// ErrLimitInexact reports a quantity that cannot be represented as bytes.
	ErrLimitInexact = registration.ErrLimitInexact
	// ErrLimitNotPositive reports a quantity that reserves nothing.
	ErrLimitNotPositive = registration.ErrLimitNotPositive
)

// LimitBytes is the single rule for reading a Pool's reservation limit: the
// declared quantity must parse, be exactly representable in bytes, and be
// positive. Callers wrap the failure in their own diagnosis. An unset limit
// fails as an unparseable quantity; a caller that wants to name that case
// separately checks CapacityLimit before calling.
func LimitBytes(pool volumeapi.Pool) (int64, error) {
	return registration.ParseLimitBytes(pool.CapacityLimit)
}
