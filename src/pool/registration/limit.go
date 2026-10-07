package registration

import (
	"errors"

	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	ErrLimitInexact     = errors.New("value cannot be represented as bytes")
	ErrLimitNotPositive = errors.New("value must be greater than zero")
)

// ParseLimitBytes requires an exact, positive int64 byte count for registration
// and reservation admission.
func ParseLimitBytes(limit string) (int64, error) {
	quantity, err := resource.ParseQuantity(limit)
	if err != nil {
		return 0, err
	}
	value, exact := quantity.AsInt64()
	if !exact {
		return 0, ErrLimitInexact
	}
	if value <= 0 {
		return 0, ErrLimitNotPositive
	}
	return value, nil
}
