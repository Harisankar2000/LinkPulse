//go:build integration

// Integration tests against real Postgres and Redis. Run with:
//
//	make test-integration
package internal_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Harisankar2000/linkpulse/internal/cache"
	"github.com/Harisankar2000/linkpulse/internal/links"
	"github.com/Harisankar2000/linkpulse/internal/ratelimit"
	"github.com/Harisankar2000/linkpulse/internal/shortid"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func setup(t *testing.T) (*store.Store, *redis.Client) {
	t.Helper()
	dbURL, redisURL := os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("DATABASE_URL and REDIS_URL must be set")
	}
	ctx := context.Background()
	db, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { rdb.Close() })
	return db, rdb
}

// 500 concurrent requests for the same uncached code must cause one DB query.
func TestCacheStampedeProtection(t *testing.T) {
	db, rdb := setup(t)
	ctx := context.Background()

	code := fmt.Sprintf("hot-%d", time.Now().UnixNano()%1e9)
	if _, err := db.CreateLink(ctx, store.Link{Code: code, LongURL: "https://example.com/hot", IsCustom: true}); err != nil {
		t.Fatal(err)
	}

	var loads atomic.Int64
	slowLoader := func(ctx context.Context, c string) (store.Link, error) {
		loads.Add(1)
		time.Sleep(50 * time.Millisecond) // a slow query widens the stampede window
		return db.GetLink(ctx, c)
	}
	lc := cache.New(rdb, slowLoader, time.Minute, time.Second, quiet)
	_ = lc.Invalidate(ctx, code)

	var wg sync.WaitGroup
	for range 500 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := lc.Get(ctx, code)
			if err != nil || l.LongURL != "https://example.com/hot" {
				t.Errorf("Get = %+v, %v", l, err)
			}
		}()
	}
	wg.Wait()

	if n := loads.Load(); n != 1 {
		t.Fatalf("database loads = %d, want 1", n)
	}
}

func TestNegativeCacheAndInvalidate(t *testing.T) {
	db, rdb := setup(t)
	ctx := context.Background()
	lc := cache.New(rdb, db.GetLink, time.Minute, time.Minute, quiet)
	svc := links.NewService(db, lc, shortid.NewAllocator(db, store.IDBlockSize), "https://lnk.test")

	alias := fmt.Sprintf("later-%d", time.Now().UnixNano()%1e9)
	if _, err := svc.Resolve(ctx, alias); err != links.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	// The miss is now negatively cached; creating the alias must clear it.
	if _, err := svc.Create(ctx, links.CreateInput{URL: "https://example.com", Alias: alias}); err != nil {
		t.Fatal(err)
	}
	if l, err := svc.Resolve(ctx, alias); err != nil || l.LongURL != "https://example.com" {
		t.Fatalf("Resolve after create = %+v, %v", l, err)
	}
}

func TestConcurrentCreatesGetUniqueCodes(t *testing.T) {
	db, rdb := setup(t)
	ctx := context.Background()
	lc := cache.New(rdb, db.GetLink, time.Minute, time.Second, quiet)
	// Two allocators simulate two API instances sharing one sequence.
	a1 := shortid.NewAllocator(db, store.IDBlockSize)
	a2 := shortid.NewAllocator(db, store.IDBlockSize)
	svcs := []*links.Service{
		links.NewService(db, lc, a1, "https://lnk.test"),
		links.NewService(db, lc, a2, "https://lnk.test"),
	}

	var (
		mu    sync.Mutex
		codes = map[string]bool{}
		wg    sync.WaitGroup
	)
	for i := range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := svcs[i%2].Create(ctx, links.CreateInput{URL: fmt.Sprintf("https://example.com/%d", i)})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if codes[l.Code] {
				t.Errorf("duplicate code %s", l.Code)
			}
			codes[l.Code] = true
		}()
	}
	wg.Wait()
	if len(codes) != 400 {
		t.Fatalf("got %d unique codes, want 400", len(codes))
	}
}

func TestRateLimiter(t *testing.T) {
	_, rdb := setup(t)
	ctx := context.Background()
	l := ratelimit.New(rdb, fmt.Sprintf("test-%d", time.Now().UnixNano()), 1, 5, quiet)

	allowed := 0
	var retry time.Duration
	for range 8 {
		ok, ra := l.Allow(ctx, "1.2.3.4")
		if ok {
			allowed++
		} else {
			retry = ra
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed %d of 8 with burst 5", allowed)
	}
	if retry <= 0 || retry > time.Second {
		t.Fatalf("retry-after = %v, want (0, 1s]", retry)
	}
	if ok, _ := l.Allow(ctx, "5.6.7.8"); !ok {
		t.Fatal("a different client must have its own bucket")
	}
}

func TestStatsRollups(t *testing.T) {
	db, _ := setup(t)
	ctx := context.Background()
	code := fmt.Sprintf("stat-%d", time.Now().UnixNano()%1e9)
	if _, err := db.CreateLink(ctx, store.Link{Code: code, LongURL: "https://example.com", IsCustom: true}); err != nil {
		t.Fatal(err)
	}
	hour := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	batch := map[store.StatKey]int64{
		{Code: code, Bucket: hour, Country: "IN", Device: "mobile", Referrer: "direct"}:      3,
		{Code: code, Bucket: hour, Country: "US", Device: "desktop", Referrer: "google.com"}: 1,
	}
	// Writing the same batch twice must add up, not overwrite.
	for range 2 {
		if err := db.AddClicks(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	st, err := db.Stats(ctx, code, hour.Add(-time.Hour), hour.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if st.TotalClicks != 8 || len(st.ByCountry) != 2 || st.ByCountry[0].Key != "IN" || st.ByCountry[0].Clicks != 6 {
		t.Fatalf("unexpected stats %+v", st)
	}
}
