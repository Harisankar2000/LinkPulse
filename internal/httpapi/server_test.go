package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harisankar2000/linkpulse/internal/analytics"
	"github.com/Harisankar2000/linkpulse/internal/links"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

type fakeLinks struct {
	mu    sync.Mutex
	links map[string]store.Link
	next  int
}

func (f *fakeLinks) Create(_ context.Context, in links.CreateInput) (store.Link, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(in.URL, "https://") {
		return store.Link{}, links.ErrInvalidURL
	}
	code := in.Alias
	if code == "" {
		f.next++
		code = "gen000" + string(rune('0'+f.next))
	}
	if _, ok := f.links[code]; ok {
		return store.Link{}, links.ErrAliasTaken
	}
	l := store.Link{Code: code, LongURL: in.URL, IsCustom: in.Alias != "", CreatedAt: time.Now(), ExpiresAt: in.ExpiresAt}
	f.links[code] = l
	return l, nil
}

func (f *fakeLinks) Resolve(ctx context.Context, code string) (store.Link, error) {
	l, err := f.Get(ctx, code)
	if err == nil && l.Expired(time.Now()) {
		return store.Link{}, links.ErrExpired
	}
	return l, err
}

func (f *fakeLinks) Get(_ context.Context, code string) (store.Link, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.links[code]
	if !ok {
		return store.Link{}, links.ErrNotFound
	}
	return l, nil
}

func (f *fakeLinks) Stats(_ context.Context, code string, _, _ time.Time) (store.Stats, error) {
	return store.Stats{Code: code, TotalClicks: 3}, nil
}

type fakeSink struct {
	mu     sync.Mutex
	events []analytics.ClickEvent
}

func (f *fakeSink) Emit(ev analytics.ClickEvent) {
	f.mu.Lock()
	f.events = append(f.events, ev)
	f.mu.Unlock()
}

type denyAll struct{}

func (denyAll) Name() string { return "test" }
func (denyAll) Allow(context.Context, string) (bool, time.Duration) {
	return false, 1500 * time.Millisecond
}

func newTestServer(t *testing.T, createLimiter Limiter) (*httptest.Server, *fakeLinks, *fakeSink) {
	t.Helper()
	fl := &fakeLinks{links: map[string]store.Link{}}
	fs := &fakeSink{}
	h := NewHandler(Deps{
		Links:         fl,
		Events:        fs,
		CreateLimiter: createLimiter,
		BaseURL:       "http://sho.rt",
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, fl, fs
}

var noFollow = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestCreateLink(t *testing.T) {
	srv, _, _ := newTestServer(t, nil)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"generated code", `{"url":"https://example.com/a"}`, http.StatusCreated},
		{"custom alias", `{"url":"https://example.com/b","alias":"my-link"}`, http.StatusCreated},
		{"alias taken", `{"url":"https://example.com/c","alias":"my-link"}`, http.StatusConflict},
		{"invalid url", `{"url":"ftp://example.com"}`, http.StatusBadRequest},
		{"bad json", `{"url":`, http.StatusBadRequest},
		{"unknown field", `{"url":"https://example.com","foo":1}`, http.StatusBadRequest},
		{"bad expires_in", `{"url":"https://example.com","expires_in":"soon"}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := post(t, srv.URL+"/api/v1/links", tt.body)
			if resp.StatusCode != tt.want {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want %d, body %s", resp.StatusCode, tt.want, b)
			}
		})
	}
}

func TestCreateReturnsShortURL(t *testing.T) {
	srv, _, _ := newTestServer(t, nil)
	resp := post(t, srv.URL+"/api/v1/links", `{"url":"https://example.com","alias":"hello"}`)
	var got linkResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ShortURL != "http://sho.rt/hello" || !got.IsCustom {
		t.Fatalf("unexpected response %+v", got)
	}
}

func TestRedirectEmitsClick(t *testing.T) {
	srv, fl, sink := newTestServer(t, nil)
	fl.links["abc1234"] = store.Link{Code: "abc1234", LongURL: "https://example.com/target"}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/abc1234", nil)
	req.Header.Set("CF-IPCountry", "in")
	req.Header.Set("Referer", "https://twitter.com/x")
	resp, err := noFollow.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "https://example.com/target" {
		t.Fatalf("Location = %q", loc)
	}
	if len(sink.events) != 1 || sink.events[0].Country != "IN" || sink.events[0].Code != "abc1234" {
		t.Fatalf("events = %+v", sink.events)
	}
}

func TestRedirectErrors(t *testing.T) {
	srv, fl, sink := newTestServer(t, nil)
	past := time.Now().Add(-time.Hour)
	fl.links["old-one"] = store.Link{Code: "old-one", LongURL: "https://example.com", ExpiresAt: &past}

	tests := []struct {
		path string
		want int
	}{
		{"/missing", http.StatusNotFound},
		{"/old-one", http.StatusGone},
		{"/a", http.StatusNotFound},          // too short to be a code
		{"/bad%20code", http.StatusNotFound}, // illegal characters
	}
	for _, tt := range tests {
		resp, err := noFollow.Get(srv.URL + tt.path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tt.want {
			t.Errorf("GET %s = %d, want %d", tt.path, resp.StatusCode, tt.want)
		}
	}
	if len(sink.events) != 0 {
		t.Fatalf("failed redirects must not emit clicks, got %d", len(sink.events))
	}
}

func TestRateLimitedCreate(t *testing.T) {
	srv, _, _ := newTestServer(t, denyAll{})
	resp := post(t, srv.URL+"/api/v1/links", `{"url":"https://example.com"}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "2" {
		t.Fatalf("Retry-After = %q, want 2", ra)
	}
}

func TestHealthAndStats(t *testing.T) {
	srv, fl, _ := newTestServer(t, nil)
	fl.links["abc1234"] = store.Link{Code: "abc1234", LongURL: "https://example.com"}

	for path, want := range map[string]int{
		"/healthz":                           http.StatusOK,
		"/readyz":                            http.StatusOK,
		"/api/v1/links/abc1234":              http.StatusOK,
		"/api/v1/links/abc1234/stats":        http.StatusOK,
		"/api/v1/links/nope123":              http.StatusNotFound,
		"/api/v1/links/abc1234/stats?from=x": http.StatusBadRequest,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
}
