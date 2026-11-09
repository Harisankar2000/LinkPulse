// Package store is the PostgreSQL persistence layer for links and click stats.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// IDBlockSize must match INCREMENT BY of the link_id_blocks sequence.
const IDBlockSize = 1000

var (
	ErrNotFound  = errors.New("link not found")
	ErrCodeTaken = errors.New("code already in use")
)

//go:embed migrations/*.sql
var migrations embed.FS

type Link struct {
	Code      string     `json:"code"`
	LongURL   string     `json:"long_url"`
	IsCustom  bool       `json:"is_custom"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// Expired reports whether the link has an expiry at or before now.
func (l Link) Expired(now time.Time) bool {
	return l.ExpiresAt != nil && !now.Before(*l.ExpiresAt)
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies the embedded SQL files in name order. The statements are
// idempotent; a real deployment would use a migration tool with versioning.
func (s *Store) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}

	var inc int64
	err = s.pool.QueryRow(ctx,
		`SELECT increment_by FROM pg_sequences WHERE sequencename = 'link_id_blocks'`).Scan(&inc)
	if err != nil {
		return fmt.Errorf("read id sequence: %w", err)
	}
	if inc != IDBlockSize {
		return fmt.Errorf("link_id_blocks increments by %d, code expects %d", inc, IDBlockSize)
	}
	return nil
}

// NextBlock reserves IDBlockSize IDs and returns the first one.
func (s *Store) NextBlock(ctx context.Context) (uint64, error) {
	var start int64
	if err := s.pool.QueryRow(ctx, `SELECT nextval('link_id_blocks')`).Scan(&start); err != nil {
		return 0, fmt.Errorf("reserve id block: %w", err)
	}
	return uint64(start), nil
}

// CreateLink inserts l. It returns ErrCodeTaken if the code already exists.
func (s *Store) CreateLink(ctx context.Context, l Link) (Link, error) {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO links (code, long_url, is_custom, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (code) DO NOTHING
		RETURNING created_at`,
		l.Code, l.LongURL, l.IsCustom, l.ExpiresAt,
	).Scan(&l.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Link{}, ErrCodeTaken
	}
	if err != nil {
		return Link{}, fmt.Errorf("insert link: %w", err)
	}
	return l, nil
}

func (s *Store) GetLink(ctx context.Context, code string) (Link, error) {
	l := Link{Code: code}
	err := s.pool.QueryRow(ctx,
		`SELECT long_url, is_custom, created_at, expires_at FROM links WHERE code = $1`, code,
	).Scan(&l.LongURL, &l.IsCustom, &l.CreatedAt, &l.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Link{}, ErrNotFound
	}
	if err != nil {
		return Link{}, fmt.Errorf("get link: %w", err)
	}
	return l, nil
}

// StatKey identifies one hourly rollup row.
type StatKey struct {
	Code     string
	Bucket   time.Time
	Country  string
	Device   string
	Referrer string
}

// AddClicks adds the given counts to the hourly rollups in one transaction.
func (s *Store) AddClicks(ctx context.Context, counts map[StatKey]int64) error {
	if len(counts) == 0 {
		return nil
	}
	// Sort keys so concurrent writers lock rows in the same order (no deadlocks).
	keys := make([]StatKey, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if !a.Bucket.Equal(b.Bucket) {
			return a.Bucket.Before(b.Bucket)
		}
		if a.Country != b.Country {
			return a.Country < b.Country
		}
		if a.Device != b.Device {
			return a.Device < b.Device
		}
		return a.Referrer < b.Referrer
	})

	batch := &pgx.Batch{}
	for _, k := range keys {
		batch.Queue(`
			INSERT INTO link_clicks_hourly (code, bucket, country, device, referrer, clicks)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (code, bucket, country, device, referrer)
			DO UPDATE SET clicks = link_clicks_hourly.clicks + EXCLUDED.clicks`,
			k.Code, k.Bucket, k.Country, k.Device, k.Referrer, counts[k])
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, batch).Close()
	})
}

type Count struct {
	Key    string `json:"key"`
	Clicks int64  `json:"clicks"`
}

type Stats struct {
	Code        string  `json:"code"`
	From        string  `json:"from"`
	To          string  `json:"to"`
	TotalClicks int64   `json:"total_clicks"`
	ByDay       []Count `json:"by_day"`
	ByCountry   []Count `json:"by_country"`
	ByDevice    []Count `json:"by_device"`
	ByReferrer  []Count `json:"by_referrer"`
}

// Stats aggregates the rollups for code between from (inclusive) and to (exclusive).
func (s *Store) Stats(ctx context.Context, code string, from, to time.Time) (Stats, error) {
	st := Stats{
		Code: code,
		From: from.UTC().Format(time.RFC3339),
		To:   to.UTC().Format(time.RFC3339),
	}

	const where = `WHERE code = $1 AND bucket >= $2 AND bucket < $3`
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(clicks), 0) FROM link_clicks_hourly `+where, code, from, to,
	).Scan(&st.TotalClicks); err != nil {
		return st, fmt.Errorf("total clicks: %w", err)
	}

	queries := []struct {
		dst *[]Count
		sql string
	}{
		{&st.ByDay, `SELECT to_char(date_trunc('day', bucket AT TIME ZONE 'UTC'), 'YYYY-MM-DD'), SUM(clicks)
			FROM link_clicks_hourly ` + where + ` GROUP BY 1 ORDER BY 1`},
		{&st.ByCountry, `SELECT country, SUM(clicks) FROM link_clicks_hourly ` + where +
			` GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 20`},
		{&st.ByDevice, `SELECT device, SUM(clicks) FROM link_clicks_hourly ` + where +
			` GROUP BY 1 ORDER BY 2 DESC, 1`},
		{&st.ByReferrer, `SELECT referrer, SUM(clicks) FROM link_clicks_hourly ` + where +
			` GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 20`},
	}
	for _, q := range queries {
		rows, err := s.pool.Query(ctx, q.sql, code, from, to)
		if err != nil {
			return st, fmt.Errorf("stats query: %w", err)
		}
		counts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Count, error) {
			var c Count
			err := r.Scan(&c.Key, &c.Clicks)
			return c, err
		})
		if err != nil {
			return st, fmt.Errorf("stats scan: %w", err)
		}
		if counts == nil {
			counts = []Count{}
		}
		*q.dst = counts
	}
	return st, nil
}
