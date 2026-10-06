package execution

import (
	"context"
	"errors"
	"testing"
)

func TestSameVolumeSerializesWhileIndependentVolumeProceeds(t *testing.T) {
	var gate Gate
	unlock, err := gate.Lock(context.Background(), "volume-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.Lock(ctx, "volume-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter=%v", err)
	}
	other, err := gate.Lock(context.Background(), "volume-b")
	if err != nil {
		t.Fatal(err)
	}
	other()
	acquired := make(chan func(), 1)
	go func() { next, _ := gate.Lock(context.Background(), "volume-a"); acquired <- next }()
	select {
	case <-acquired:
		t.Fatal("same Volume overlapped")
	default:
	}
	unlock()
	(<-acquired)()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.entries) != 0 {
		t.Fatal("completed Volume locks retained")
	}
}
