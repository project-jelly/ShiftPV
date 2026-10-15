package readiness

import (
	"context"
	"testing"
	"time"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"k8s.io/apimachinery/pkg/util/wait"
	clocktesting "k8s.io/utils/clock/testing"
)

// Exercise the resident Run loop, not a test-driven second Reconcile call.
func TestChangingInventoryRecoversWithoutExternalWake(t *testing.T) {
	c := clocktesting.NewFakeClock(testTime)
	repo := &fakeRepository{pools: []volumeapi.Pool{
		{Name: "pool-a", UID: "changing-uid", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock},
		{Name: "pool-b", UID: "peer-uid", NodeName: "node", Generation: 1, CapacityPolicy: volumeapi.PoolCapacityPolicyFixedBlock},
	}}
	ok := Check{OK: true, Known: true, Reason: "OK"}
	inspector := fakeAllocationInspector{result: Result{Accessible: ok, Writable: ok, CapacityReadable: ok}}
	for i := range repo.pools {
		repo.pools[i].Status.RegistrationApproved = true
		repo.pools[i].Status.CapacityUnit, _ = inspector.InspectCapacityUnit(repo.pools[i])
	}
	r, err := NewReconciler("node", repo, inspector, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	r.Clock = c
	passes := make(chan []Observation, 8)
	scans := map[string]int{}
	r.Inventory = func(_ context.Context, p volumeapi.Pool, _ time.Time) volumeapi.PoolInventory {
		scans[p.UID]++
		if p.UID == "changing-uid" && scans[p.UID] <= 2 {
			return volumeapi.PoolInventory{Reason: volumeapi.InventoryPhysicalChanged, Message: volumeapi.InventoryPhysicalChanged}
		}
		return volumeapi.PoolInventory{Valid: true}
	}
	r.ObserveAll = func(observations []Observation, _ error) { passes <- observations }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Run did not stop")
		}
	}()
	if first := nextReadinessPass(t, passes); len(first) != 2 || !first[0].Result.RetryInventory {
		t.Fatalf("initial observations: %+v", first)
	}
	for _, delay := range []time.Duration{time.Second, 2 * time.Second} {
		awaitReadinessTimers(t, c, 2)
		c.Step(delay)
		pass := nextReadinessPass(t, passes)
		if len(pass) != 2 || pass[1].Pool.UID != "peer-uid" || !pass[1].Result.CapacityReadable.OK {
			t.Fatalf("partial retry dropped peer metrics: %+v", pass)
		}
	}
	awaitReadinessTimers(t, c, 1)
	// Once recovered, no short timer remains. A watch event still performs a
	// full pass; the periodic fallback is retained for every Pool.
	c.Step(time.Minute)
	select {
	case pass := <-passes:
		t.Fatalf("recovered inventory kept retrying: %+v", pass)
	default:
	}
	if scans["changing-uid"] != 3 || scans["peer-uid"] != 1 || !repo.statuses["pool-a"].Inventory.Valid {
		t.Fatalf("missing autonomous recovery: scans=%v status=%+v", scans, repo.statuses["pool-a"])
	}
}

func TestUnsafeInventoryDoesNotScheduleShortRetries(t *testing.T) {
	for _, inventory := range []volumeapi.PoolInventory{
		{Message: "PoolIdentityInvalid"},
		{Message: "UnrecordedPath"},
		{Valid: true, Truncated: true},
		{Reason: "unknown", Message: "unknown"},
	} {
		t.Run(inventory.Message, func(t *testing.T) {
			c := clocktesting.NewFakeClock(testTime)
			r, _ := NewReconciler("node", &fakeRepository{pool: volumeapi.Pool{Name: "pool", UID: "uid", NodeName: "node"}}, fakeInspector{}, time.Hour)
			r.Clock = c
			r.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory { return inventory }
			passes := make(chan []Observation, 2)
			r.ObserveAll = func(o []Observation, _ error) { passes <- o }
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			defer func() { cancel(); <-done }()
			nextReadinessPass(t, passes)
			awaitReadinessTimers(t, c, 1)
			c.Step(time.Minute)
			select {
			case <-passes:
				t.Fatal("unsafe or unclassified inventory scheduled a short retry")
			default:
			}
		})
	}
}

func nextReadinessPass(t *testing.T, passes <-chan []Observation) []Observation {
	t.Helper()
	select {
	case pass := <-passes:
		return pass
	case <-time.After(3 * time.Second):
		t.Fatal("readiness pass did not complete")
		return nil
	}
}

func awaitReadinessTimers(t *testing.T, c *clocktesting.FakeClock, count int) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, 3*time.Second, true, func(context.Context) (bool, error) {
		return c.Waiters() == count, nil
	}); err != nil {
		t.Fatalf("scheduler has %d timers, want %d", c.Waiters(), count)
	}
}

func TestChangingInventoryRetryBackoffIsCapped(t *testing.T) {
	c := clocktesting.NewFakeClock(testTime)
	r, _ := NewReconciler("node", &fakeRepository{pool: volumeapi.Pool{Name: "pool", UID: "uid", NodeName: "node"}}, fakeInspector{}, time.Hour)
	r.Clock = c
	r.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		return volumeapi.PoolInventory{Reason: volumeapi.InventoryPhysicalChanged, Message: volumeapi.InventoryPhysicalChanged}
	}
	passes := make(chan []Observation, 2)
	r.ObserveAll = func(o []Observation, _ error) { passes <- o }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()
	nextReadinessPass(t, passes)
	for _, delay := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
		awaitReadinessTimers(t, c, 2)
		c.Step(delay*time.Second - time.Millisecond)
		select {
		case <-passes:
			t.Fatal("retry ran before its backoff expired")
		default:
		}
		c.Step(time.Millisecond)
		nextReadinessPass(t, passes)
	}
}

func TestScheduledInventoryRetrySurvivesListFailure(t *testing.T) {
	c := clocktesting.NewFakeClock(testTime)
	repo := &fakeRepository{pool: volumeapi.Pool{Name: "pool", UID: "uid", NodeName: "node"}}
	r, _ := NewReconciler("node", repo, fakeInspector{}, time.Hour)
	r.Clock = c
	scans := 0
	r.Inventory = func(context.Context, volumeapi.Pool, time.Time) volumeapi.PoolInventory {
		scans++
		if scans == 1 {
			return volumeapi.PoolInventory{Reason: volumeapi.InventoryPhysicalChanged, Message: volumeapi.InventoryPhysicalChanged}
		}
		return volumeapi.PoolInventory{Valid: true}
	}
	passes := make(chan []Observation, 3)
	r.ObserveAll = func(o []Observation, _ error) { passes <- o }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()
	nextReadinessPass(t, passes)
	awaitReadinessTimers(t, c, 2)
	repo.err = context.DeadlineExceeded
	c.Step(time.Second)
	nextReadinessPass(t, passes)
	awaitReadinessTimers(t, c, 2)
	repo.err = nil
	c.Step(2 * time.Second)
	nextReadinessPass(t, passes)
	awaitReadinessTimers(t, c, 1)
	if scans != 2 || !repo.status.Inventory.Valid {
		t.Fatal("failed List stranded the pending inventory retry")
	}
}
