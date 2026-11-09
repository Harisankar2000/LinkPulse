package cache

import (
	"sync"
	"time"

	"github.com/Harisankar2000/linkpulse/internal/store"
)

// local is a small in-process cache (L1) in front of Redis (L2). Hot links,
// which take most of the traffic, are then served without any network call.
//
// Only found links are stored. Links are immutable once created, so a short
// TTL just bounds memory; there's nothing to invalidate across instances.
// Negative results stay in Redis only, so a freshly created alias is visible
// on every instance right away.
type local struct {
	mu      sync.RWMutex
	items   map[string]localItem
	ttl     time.Duration
	maxSize int
}

type localItem struct {
	link    store.Link
	expires time.Time
}

func newLocal(ttl time.Duration, maxSize int) *local {
	return &local{items: make(map[string]localItem, maxSize), ttl: ttl, maxSize: maxSize}
}

func (l *local) get(code string) (store.Link, bool) {
	l.mu.RLock()
	it, ok := l.items[code]
	l.mu.RUnlock()
	if !ok || time.Now().After(it.expires) {
		return store.Link{}, false
	}
	return it.link, true
}

func (l *local) set(link store.Link) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.items) >= l.maxSize {
		// Cheap eviction: drop about a quarter of the entries. Map iteration
		// order is random, so this approximates random eviction.
		drop := l.maxSize / 4
		for k := range l.items {
			delete(l.items, k)
			if drop--; drop <= 0 {
				break
			}
		}
	}
	l.items[link.Code] = localItem{link: link, expires: time.Now().Add(l.ttl)}
}
