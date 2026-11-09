// Package links holds the business rules for creating and resolving short links.
package links

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Harisankar2000/linkpulse/internal/shortid"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

var (
	ErrInvalidURL   = errors.New("url must be an absolute http(s) URL with a public host, at most 2048 characters")
	ErrInvalidAlias = errors.New("alias must be 3-32 characters of letters, digits, '-' or '_', and not a reserved word")
	ErrAliasTaken   = errors.New("alias is already in use")
	ErrInvalidTTL   = errors.New("expiry must be in the future and at most 10 years away")
	ErrExpired      = errors.New("link has expired")
	ErrNotFound     = store.ErrNotFound
)

const (
	maxURLLength  = 2048
	maxExpiry     = 10 * 365 * 24 * time.Hour
	createRetries = 3
)

var (
	aliasPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{3,32}$`)
	reserved     = map[string]bool{
		"api": true, "healthz": true, "readyz": true, "metrics": true,
		"admin": true, "static": true, "favicon.ico": true, "robots.txt": true,
	}
)

// Repository is the persistence the service needs (implemented by *store.Store).
type Repository interface {
	CreateLink(ctx context.Context, l store.Link) (store.Link, error)
	GetLink(ctx context.Context, code string) (store.Link, error)
	Stats(ctx context.Context, code string, from, to time.Time) (store.Stats, error)
}

// Cache is the read-through cache (implemented by *cache.LinkCache).
type Cache interface {
	Get(ctx context.Context, code string) (store.Link, error)
	Invalidate(ctx context.Context, code string) error
}

// IDSource hands out unique numeric IDs (implemented by *shortid.Allocator).
type IDSource interface {
	Next(ctx context.Context) (uint64, error)
}

type Service struct {
	repo     Repository
	cache    Cache
	ids      IDSource
	selfHost string
	now      func() time.Time
}

func NewService(repo Repository, cache Cache, ids IDSource, baseURL string) *Service {
	host := ""
	if u, err := url.Parse(baseURL); err == nil {
		host = strings.ToLower(u.Hostname())
	}
	return &Service{repo: repo, cache: cache, ids: ids, selfHost: host, now: time.Now}
}

type CreateInput struct {
	URL       string
	Alias     string
	ExpiresAt *time.Time
}

func (s *Service) Create(ctx context.Context, in CreateInput) (store.Link, error) {
	longURL, err := s.normalizeURL(in.URL)
	if err != nil {
		return store.Link{}, err
	}
	if in.ExpiresAt != nil {
		d := in.ExpiresAt.Sub(s.now())
		if d <= 0 || d > maxExpiry {
			return store.Link{}, ErrInvalidTTL
		}
		t := in.ExpiresAt.UTC()
		in.ExpiresAt = &t
	}

	link := store.Link{LongURL: longURL, ExpiresAt: in.ExpiresAt}

	if in.Alias != "" {
		if !ValidAlias(in.Alias) {
			return store.Link{}, ErrInvalidAlias
		}
		link.Code, link.IsCustom = in.Alias, true
		created, err := s.repo.CreateLink(ctx, link)
		if errors.Is(err, store.ErrCodeTaken) {
			return store.Link{}, ErrAliasTaken
		}
		if err != nil {
			return store.Link{}, err
		}
		s.invalidate(ctx, created.Code)
		return created, nil
	}

	// Generated codes never collide with each other, but one can clash with a
	// custom alias that happens to be 7 base62 characters. Then take the next ID.
	for range createRetries {
		id, err := s.ids.Next(ctx)
		if err != nil {
			return store.Link{}, err
		}
		if link.Code, err = shortid.FromID(id); err != nil {
			return store.Link{}, err
		}
		created, err := s.repo.CreateLink(ctx, link)
		if errors.Is(err, store.ErrCodeTaken) {
			continue
		}
		if err != nil {
			return store.Link{}, err
		}
		s.invalidate(ctx, created.Code)
		return created, nil
	}
	return store.Link{}, fmt.Errorf("could not allocate a free code after %d attempts", createRetries)
}

// invalidate clears a negative-cache entry left by earlier lookups of the code.
func (s *Service) invalidate(ctx context.Context, code string) {
	_ = s.cache.Invalidate(ctx, code) // worst case: "not found" for up to NEGATIVE_CACHE_TTL
}

// Resolve returns the destination for a redirect, served from the cache.
func (s *Service) Resolve(ctx context.Context, code string) (store.Link, error) {
	l, err := s.cache.Get(ctx, code)
	if err != nil {
		return store.Link{}, err
	}
	if l.Expired(s.now()) {
		return store.Link{}, ErrExpired
	}
	return l, nil
}

// Get returns full link details straight from the database.
func (s *Service) Get(ctx context.Context, code string) (store.Link, error) {
	return s.repo.GetLink(ctx, code)
}

func (s *Service) Stats(ctx context.Context, code string, from, to time.Time) (store.Stats, error) {
	if _, err := s.repo.GetLink(ctx, code); err != nil {
		return store.Stats{}, err
	}
	return s.repo.Stats(ctx, code, from, to)
}

func (s *Service) normalizeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxURLLength {
		return "", ErrInvalidURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", ErrInvalidURL
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return "", ErrInvalidURL
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast()) {
		return "", ErrInvalidURL
	}
	// Shortening our own short links would create redirect loops.
	if s.selfHost != "" && host == s.selfHost {
		return "", ErrInvalidURL
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String(), nil
}

// ValidAlias reports whether s is acceptable as a custom alias.
func ValidAlias(s string) bool {
	return aliasPattern.MatchString(s) && !reserved[strings.ToLower(s)]
}

// PlausibleCode is a cheap check run before any cache or database lookup, so
// obviously bad paths are rejected without touching Redis.
func PlausibleCode(s string) bool {
	return aliasPattern.MatchString(s)
}
