package links

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Harisankar2000/linkpulse/internal/store"
)

type memRepo struct{ links map[string]store.Link }

func (m *memRepo) CreateLink(_ context.Context, l store.Link) (store.Link, error) {
	if _, ok := m.links[l.Code]; ok {
		return store.Link{}, store.ErrCodeTaken
	}
	l.CreatedAt = time.Now()
	m.links[l.Code] = l
	return l, nil
}

func (m *memRepo) GetLink(_ context.Context, code string) (store.Link, error) {
	l, ok := m.links[code]
	if !ok {
		return store.Link{}, store.ErrNotFound
	}
	return l, nil
}

func (m *memRepo) Stats(context.Context, string, time.Time, time.Time) (store.Stats, error) {
	return store.Stats{}, nil
}

type passthroughCache struct {
	repo        *memRepo
	invalidated []string
}

func (c *passthroughCache) Get(ctx context.Context, code string) (store.Link, error) {
	return c.repo.GetLink(ctx, code)
}

func (c *passthroughCache) Invalidate(_ context.Context, code string) error {
	c.invalidated = append(c.invalidated, code)
	return nil
}

type seqIDs struct{ n uint64 }

func (s *seqIDs) Next(context.Context) (uint64, error) { s.n++; return s.n, nil }

func newService() (*Service, *memRepo, *passthroughCache) {
	repo := &memRepo{links: map[string]store.Link{}}
	c := &passthroughCache{repo: repo}
	return NewService(repo, c, &seqIDs{}, "https://lnk.pulse"), repo, c
}

func TestNormalizeURL(t *testing.T) {
	s, _, _ := newService()
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"https://Example.com/Path?q=1", "https://example.com/Path?q=1", false},
		{"  http://example.com  ", "http://example.com", false},
		{"HTTPS://EXAMPLE.COM", "https://example.com", false},
		{"", "", true},
		{"example.com", "", true},
		{"javascript:alert(1)", "", true},
		{"ftp://example.com", "", true},
		{"http://localhost:8080", "", true},
		{"http://127.0.0.1/admin", "", true},
		{"http://10.0.0.5/", "", true},
		{"https://user:pass@example.com", "", true},
		{"https://lnk.pulse/abc1234", "", true}, // our own domain: redirect loop
	}
	for _, tt := range tests {
		got, err := s.normalizeURL(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("normalizeURL(%q) = %q, %v; want %q, err=%v", tt.in, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestValidAlias(t *testing.T) {
	for alias, want := range map[string]bool{
		"my-link": true, "Sale_2026": true, "ab": false, "api": false, "Metrics": false,
		"has space": false, "emoji🙂": false, "a-very-long-alias-that-is-over-32-chars": false,
	} {
		if got := ValidAlias(alias); got != want {
			t.Errorf("ValidAlias(%q) = %v, want %v", alias, got, want)
		}
	}
}

func TestCreateSkipsCodeTakenByAlias(t *testing.T) {
	s, repo, c := newService()

	// Squat on the code that ID 1 would produce.
	first, _ := s.Create(context.Background(), CreateInput{URL: "https://example.com/1"})
	delete(repo.links, first.Code)
	s.ids = &seqIDs{}
	repo.links[first.Code] = store.Link{Code: first.Code, LongURL: "https://squatter.example", IsCustom: true}

	got, err := s.Create(context.Background(), CreateInput{URL: "https://example.com/2"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Code == first.Code {
		t.Fatal("expected a different code after collision")
	}
	if len(c.invalidated) == 0 || c.invalidated[len(c.invalidated)-1] != got.Code {
		t.Fatal("new code was not invalidated in the cache")
	}
}

func TestCreateExpiry(t *testing.T) {
	s, _, _ := newService()
	past := time.Now().Add(-time.Minute)
	if _, err := s.Create(context.Background(), CreateInput{URL: "https://example.com", ExpiresAt: &past}); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("got %v, want ErrInvalidTTL", err)
	}
	future := time.Now().Add(time.Hour)
	l, err := s.Create(context.Background(), CreateInput{URL: "https://example.com", ExpiresAt: &future})
	if err != nil || l.ExpiresAt == nil {
		t.Fatalf("got %v %+v", err, l)
	}
}

func TestResolveExpired(t *testing.T) {
	s, repo, _ := newService()
	past := time.Now().Add(-time.Second)
	repo.links["gone123"] = store.Link{Code: "gone123", LongURL: "https://example.com", ExpiresAt: &past}
	if _, err := s.Resolve(context.Background(), "gone123"); !errors.Is(err, ErrExpired) {
		t.Fatalf("got %v, want ErrExpired", err)
	}
}

func TestAliasTaken(t *testing.T) {
	s, _, _ := newService()
	in := CreateInput{URL: "https://example.com", Alias: "promo"}
	if _, err := s.Create(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(context.Background(), in); !errors.Is(err, ErrAliasTaken) {
		t.Fatalf("got %v, want ErrAliasTaken", err)
	}
}
