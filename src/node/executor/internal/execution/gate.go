package execution

import (
	"context"
	"sync"
)

// Gate serializes API recovery and direct RPC for a Volume. Entries disappear
// after the last waiter, and cancelled calls never wait for another operation.
type Gate struct {
	mu      sync.Mutex
	entries map[string]*entry
}
type entry struct {
	token chan struct{}
	users int
}

func (g *Gate) Lock(ctx context.Context, key string) (func(), error) {
	g.mu.Lock()
	if g.entries == nil {
		g.entries = map[string]*entry{}
	}
	e := g.entries[key]
	if e == nil {
		e = &entry{token: make(chan struct{}, 1)}
		g.entries[key] = e
	}
	e.users++
	g.mu.Unlock()
	select {
	case e.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-e.token
			g.release(key, e)
			return nil, err
		}
		return func() { <-e.token; g.release(key, e) }, nil
	case <-ctx.Done():
		g.release(key, e)
		return nil, ctx.Err()
	}
}
func (g *Gate) release(key string, e *entry) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e.users--
	if e.users == 0 {
		delete(g.entries, key)
	}
}
