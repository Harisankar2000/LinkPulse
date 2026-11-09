// Package cache is a Redis read-through cache for link lookups.
//
// Lookups go L1 (in-process, 10s) -> L2 (Redis) -> Postgres.
//
// Cache stampede protection, three layers:
//  1. singleflight: when a hot key misses, only one goroutine per instance
//     queries Postgres; the others wait for and share its result.
//  2. TTL jitter: entries don't all expire at the same instant after a
//     deploy or a traffic spike.
//  3. Negative caching: unknown codes are cached briefly too, so a flood of
//     requests for random codes can't reach the database.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/Harisankar2000/linkpulse/internal/metrics"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

const (
	notFoundMark = "!"
	loadTimeout  = 3 * time.Second
)

// Loader fetches a link from the source of truth.
type Loader func(ctx context.Context, code string) (store.Link, error)

type LinkCache struct {
	rdb    *redis.Client
	load   Loader
	ttl    time.Duration
	negTTL time.Duration
	sf     singleflight.Group
	l1     *local
	prefix string // e.g. "lp:link:"
	log    *slog.Logger
}

// New returns a cache whose Redis keys start with namespace (e.g. "lp:"), so
// LinkPulse can share a Redis database with other apps.
func New(rdb *redis.Client, namespace string, load Loader, ttl, negTTL time.Duration, log *slog.Logger) *LinkCache {
	return &LinkCache{
		rdb: rdb, load: load, ttl: ttl, negTTL: negTTL, log: log,
		prefix: namespace + "link:",
		l1:     newLocal(10*time.Second, 50_000),
	}
}

type entry struct {
	URL       string     `json:"u"`
	ExpiresAt *time.Time `json:"e,omitempty"`
}

// Get returns the link for code, or store.ErrNotFound. If Redis is down it
// falls back to the loader, so the cache never makes redirects unavailable.
func (c *LinkCache) Get(ctx context.Context, code string) (store.Link, error) {
	if l, ok := c.l1.get(code); ok {
		metrics.CacheLookups.WithLabelValues("l1_hit").Inc()
		return l, nil
	}

	raw, err := c.rdb.Get(ctx, c.prefix+code).Result()
	switch {
	case err == nil && raw == notFoundMark:
		metrics.CacheLookups.WithLabelValues("negative_hit").Inc()
		return store.Link{}, store.ErrNotFound
	case err == nil:
		var e entry
		if jerr := json.Unmarshal([]byte(raw), &e); jerr == nil {
			metrics.CacheLookups.WithLabelValues("hit").Inc()
			l := store.Link{Code: code, LongURL: e.URL, ExpiresAt: e.ExpiresAt}
			c.l1.set(l)
			return l, nil
		}
		metrics.CacheLookups.WithLabelValues("corrupt").Inc()
	case errors.Is(err, redis.Nil):
		metrics.CacheLookups.WithLabelValues("miss").Inc()
	default:
		metrics.CacheLookups.WithLabelValues("error").Inc()
		c.log.Warn("redis get failed, falling back to db", "err", err)
	}

	v, err, _ := c.sf.Do(code, func() (any, error) {
		// Detach from the first caller's context: if that client disconnects,
		// the other callers waiting on this load should still get a result.
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loadTimeout)
		defer cancel()
		// Double-check: a caller that missed just after another flight finished
		// would otherwise start a second DB load for a value that is now cached.
		if raw, err := c.rdb.Get(lctx, c.prefix+code).Result(); err == nil {
			if raw == notFoundMark {
				return store.Link{}, store.ErrNotFound
			}
			var e entry
			if json.Unmarshal([]byte(raw), &e) == nil {
				return store.Link{Code: code, LongURL: e.URL, ExpiresAt: e.ExpiresAt}, nil
			}
		}
		return c.loadAndFill(lctx, code)
	})
	if err != nil {
		return store.Link{}, err
	}
	l := v.(store.Link)
	c.l1.set(l)
	return l, nil
}

func (c *LinkCache) loadAndFill(ctx context.Context, code string) (store.Link, error) {
	metrics.CacheLoads.Inc()
	l, err := c.load(ctx, code)
	if errors.Is(err, store.ErrNotFound) {
		if serr := c.rdb.Set(ctx, c.prefix+code, notFoundMark, c.negTTL).Err(); serr != nil {
			c.log.Warn("redis set negative failed", "err", serr)
		}
		return store.Link{}, err
	}
	if err != nil {
		return store.Link{}, err
	}

	ttl := jitter(c.ttl)
	if l.ExpiresAt != nil {
		untilExpiry := time.Until(*l.ExpiresAt)
		if untilExpiry <= 0 {
			return l, nil // expired: nothing worth caching
		}
		ttl = min(ttl, untilExpiry)
	}
	b, _ := json.Marshal(entry{URL: l.LongURL, ExpiresAt: l.ExpiresAt})
	if serr := c.rdb.Set(ctx, c.prefix+code, b, ttl).Err(); serr != nil {
		c.log.Warn("redis set failed", "err", serr)
	}
	return l, nil
}

// Invalidate removes any cached entry, including a negative one. Called after
// a link is created so an earlier "not found" doesn't hide it.
func (c *LinkCache) Invalidate(ctx context.Context, code string) error {
	return c.rdb.Del(ctx, c.prefix+code).Err()
}

// jitter returns d plus or minus up to 10%.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := int64(d) / 5
	if spread == 0 {
		return d
	}
	return d - time.Duration(spread/2) + time.Duration(rand.Int64N(spread))
}
