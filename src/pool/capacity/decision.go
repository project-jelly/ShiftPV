package capacity

// FitsReservation handles a full or overcommitted ledger without subtraction
// overflow. A positive result is only a logical fit, not filesystem admission.
func FitsReservation(requested, reserved, limit int64) bool {
	return requested > 0 && reserved >= 0 && reserved < limit && requested <= limit-reserved
}

func ExceedsTotal(requested, total int64) bool {
	return total > 0 && requested > total
}
