package capacity

import (
	"errors"
	"testing"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
)

func TestLimitBytesAcceptsExactPositiveQuantity(t *testing.T) {
	got, err := LimitBytes(volumeapi.Pool{CapacityLimit: "1Gi"})
	if err != nil || got != 1<<30 {
		t.Fatalf("limit=%d err=%v", got, err)
	}
}

func TestLimitBytesRejectsInvalidQuantity(t *testing.T) {
	for _, limit := range []string{"", "not-a-quantity"} {
		got, err := LimitBytes(volumeapi.Pool{CapacityLimit: limit})
		if err == nil || got != 0 {
			t.Fatalf("limit %q: got=%d err=%v", limit, got, err)
		}
		if errors.Is(err, ErrLimitInexact) || errors.Is(err, ErrLimitNotPositive) {
			t.Fatalf("limit %q must report a parse failure, got %v", limit, err)
		}
	}
}

func TestLimitBytesRejectsQuantityThatIsNotWholeBytes(t *testing.T) {
	for _, limit := range []string{"1500m", "9223372036854775808"} {
		got, err := LimitBytes(volumeapi.Pool{CapacityLimit: limit})
		if !errors.Is(err, ErrLimitInexact) || got != 0 {
			t.Fatalf("limit %q: got=%d err=%v", limit, got, err)
		}
	}
}

func TestLimitBytesRejectsNonPositiveQuantity(t *testing.T) {
	for _, limit := range []string{"0", "-1Gi"} {
		got, err := LimitBytes(volumeapi.Pool{CapacityLimit: limit})
		if !errors.Is(err, ErrLimitNotPositive) || got != 0 {
			t.Fatalf("limit %q: got=%d err=%v", limit, got, err)
		}
	}
}
