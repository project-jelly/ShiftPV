package capacity

import "sync"

// Locker is a keyed mutex: it serializes work for one key, such as a Pool
// capacity decision or a Volume lifecycle, while allowing independent keys to
// proceed concurrently.
type Locker struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

type lockEntry struct {
	mu   sync.Mutex
	refs int
}

func (l *Locker) Lock(pool string) func() {
	entry := l.reference(pool)
	entry.mu.Lock()
	return func() { l.unlock(pool, entry) }
}

// TryLock shares the same exclusion boundary as Lock without waiting for a
// busy key. A failed attempt retains neither the entry nor an unlock function.
func (l *Locker) TryLock(pool string) (func(), bool) {
	entry := l.reference(pool)
	if !entry.mu.TryLock() {
		l.release(pool, entry)
		return nil, false
	}
	return func() { l.unlock(pool, entry) }, true
}

func (l *Locker) reference(pool string) *lockEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entries == nil {
		l.entries = make(map[string]*lockEntry)
	}
	entry := l.entries[pool]
	if entry == nil {
		entry = &lockEntry{}
		l.entries[pool] = entry
	}
	entry.refs++
	return entry
}

func (l *Locker) unlock(pool string, entry *lockEntry) {
	entry.mu.Unlock()
	l.release(pool, entry)
}

func (l *Locker) release(pool string, entry *lockEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(l.entries, pool)
	}
}
