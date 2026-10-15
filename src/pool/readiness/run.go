package readiness

import (
	"context"
	"errors"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

type inventoryRetry struct {
	due   time.Time
	delay time.Duration
}

// Run owns recovery scheduling. Only explicitly changing inventories receive
// short retries; unknown or unsafe observations retain the periodic fallback.
func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	c := r.Clock
	if c == nil {
		c = clock.RealClock{}
	}
	ticker := c.NewTicker(r.Interval)
	defer ticker.Stop()
	pending := map[string]inventoryRetry{}
	var only map[string]bool
	for {
		if ctx.Err() != nil {
			return nil
		}
		retry, err := r.reconcile(ctx, only)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			klog.Errorf("reconcile ShiftPVPool readiness on node %s: %v", r.NodeName, err)
		}
		updateInventoryRetries(pending, only, retry, c.Now())

		var timer clock.Timer
		var timerC <-chan time.Time
		if len(pending) > 0 {
			timer = c.NewTimer(timerDelay(pending, c.Now()))
			timerC = timer.C()
		}
		only = nil
		select {
		case <-ctx.Done():
		case <-ticker.C():
		case <-r.Wake:
		case <-timerC:
			only = map[string]bool{}
			for uid, retry := range pending {
				if !retry.due.After(c.Now()) {
					only[uid] = true
				}
			}
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func timerDelay(pending map[string]inventoryRetry, now time.Time) time.Duration {
	var earliest time.Time
	for _, retry := range pending {
		if earliest.IsZero() || retry.due.Before(earliest) {
			earliest = retry.due
		}
	}
	return max(0, earliest.Sub(now))
}

func updateInventoryRetries(pending map[string]inventoryRetry, only, retry map[string]bool, now time.Time) {
	// A failed List must not strand a previously scheduled observation.
	if retry == nil {
		retry = map[string]bool{}
		for uid := range pending {
			if only == nil || only[uid] {
				retry[uid] = true
			}
		}
	}
	for uid := range pending {
		if (only == nil || only[uid]) && !retry[uid] {
			delete(pending, uid)
		}
	}
	for uid, needed := range retry {
		if !needed {
			continue
		}
		delay := min(2*pending[uid].delay, 30*time.Second)
		if delay == 0 {
			delay = time.Second
		}
		pending[uid] = inventoryRetry{due: now.Add(delay), delay: delay}
	}

}

func needsInventoryRetry(inventory *volumeapi.PoolInventory) bool {
	return inventory != nil && !inventory.Valid && inventory.Reason == volumeapi.InventoryPhysicalChanged
}
