// Package httpapi exposes the REST API and the redirect endpoint.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Harisankar2000/linkpulse/internal/analytics"
	"github.com/Harisankar2000/linkpulse/internal/links"
	"github.com/Harisankar2000/linkpulse/internal/metrics"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

// LinkService is implemented by *links.Service.
type LinkService interface {
	Create(ctx context.Context, in links.CreateInput) (store.Link, error)
	Resolve(ctx context.Context, code string) (store.Link, error)
	Get(ctx context.Context, code string) (store.Link, error)
	Stats(ctx context.Context, code string, from, to time.Time) (store.Stats, error)
}

// EventSink receives click events (implemented by *analytics.Emitter).
type EventSink interface {
	Emit(analytics.ClickEvent)
}

// Limiter is implemented by *ratelimit.Limiter.
type Limiter interface {
	Name() string
	Allow(ctx context.Context, key string) (bool, time.Duration)
}

// Checker is a named readiness check, e.g. a database ping.
type Checker struct {
	Name  string
	Check func(context.Context) error
}

type Deps struct {
	Links           LinkService
	Events          EventSink
	CreateLimiter   Limiter // nil disables
	RedirectLimiter Limiter // nil disables
	Ready           []Checker
	BaseURL         string
	TrustProxy      bool
	// DemoMode enables the click simulator used by the web dashboard.
	DemoMode bool
	Log      *slog.Logger
}

type server struct{ Deps }

func NewHandler(d Deps) http.Handler {
	s := &server{d}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.home)
	mux.HandleFunc("GET /api/v1/info", s.info)
	mux.Handle("POST /api/v1/links", s.limit(d.CreateLimiter, http.HandlerFunc(s.createLink)))
	if d.DemoMode {
		mux.Handle("POST /api/v1/links/{code}/simulate", s.limit(d.CreateLimiter, http.HandlerFunc(s.simulate)))
	}
	mux.HandleFunc("GET /api/v1/links/{code}", s.getLink)
	mux.HandleFunc("GET /api/v1/links/{code}/stats", s.linkStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /{code}", s.limit(d.RedirectLimiter, http.HandlerFunc(s.redirect)))

	return s.observe(s.recoverPanics(mux))
}

func (s *server) info(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":   "linkpulse",
		"base_url":  s.BaseURL,
		"demo_mode": s.DemoMode,
		"source":    "https://github.com/Harisankar2000/LinkPulse",
	})
}

type createRequest struct {
	URL       string     `json:"url"`
	Alias     string     `json:"alias,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// ExpiresIn is an alternative to ExpiresAt, e.g. "24h" or "30m".
	ExpiresIn string `json:"expires_in,omitempty"`
}

type linkResponse struct {
	Code      string     `json:"code"`
	ShortURL  string     `json:"short_url"`
	LongURL   string     `json:"long_url"`
	IsCustom  bool       `json:"is_custom"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (s *server) toResponse(l store.Link) linkResponse {
	return linkResponse{
		Code: l.Code, ShortURL: s.BaseURL + "/" + l.Code, LongURL: l.LongURL,
		IsCustom: l.IsCustom, CreatedAt: l.CreatedAt, ExpiresAt: l.ExpiresAt,
	}
}

func (s *server) createLink(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be JSON like {\"url\": \"https://...\"}")
		return
	}

	in := links.CreateInput{URL: req.URL, Alias: req.Alias, ExpiresAt: req.ExpiresAt}
	if req.ExpiresIn != "" {
		if req.ExpiresAt != nil {
			writeError(w, http.StatusBadRequest, "invalid_expiry", "send expires_at or expires_in, not both")
			return
		}
		d, err := time.ParseDuration(req.ExpiresIn)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_expiry", "expires_in must be a duration like \"24h\"")
			return
		}
		t := time.Now().Add(d)
		in.ExpiresAt = &t
	}

	link, err := s.Links.Create(r.Context(), in)
	switch {
	case errors.Is(err, links.ErrInvalidURL):
		writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
	case errors.Is(err, links.ErrInvalidAlias):
		writeError(w, http.StatusBadRequest, "invalid_alias", err.Error())
	case errors.Is(err, links.ErrInvalidTTL):
		writeError(w, http.StatusBadRequest, "invalid_expiry", err.Error())
	case errors.Is(err, links.ErrAliasTaken):
		writeError(w, http.StatusConflict, "alias_taken", err.Error())
	case err != nil:
		s.Log.Error("create link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "could not create link")
	default:
		w.Header().Set("Location", "/api/v1/links/"+link.Code)
		writeJSON(w, http.StatusCreated, s.toResponse(link))
	}
}

func (s *server) getLink(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !links.PlausibleCode(code) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	link, err := s.Links.Get(r.Context(), code)
	if errors.Is(err, links.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	if err != nil {
		s.Log.Error("get link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "could not load link")
		return
	}
	writeJSON(w, http.StatusOK, s.toResponse(link))
}

func (s *server) linkStats(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !links.PlausibleCode(code) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	to := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	from := to.AddDate(0, 0, -30)
	var err error
	if v := r.URL.Query().Get("from"); v != "" {
		if from, err = parseDay(v); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_range", "from must be YYYY-MM-DD or RFC 3339")
			return
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if to, err = parseDay(v); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_range", "to must be YYYY-MM-DD or RFC 3339")
			return
		}
	}
	if !from.Before(to) || to.Sub(from) > 366*24*time.Hour {
		writeError(w, http.StatusBadRequest, "invalid_range", "from must be before to, and the range at most a year")
		return
	}

	stats, err := s.Links.Stats(r.Context(), code, from, to)
	if errors.Is(err, links.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	if err != nil {
		s.Log.Error("link stats", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "could not load stats")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func parseDay(v string) (time.Time, error) {
	if t, err := time.Parse(time.DateOnly, v); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, v)
}

// redirect is the hot path: one Redis GET in the common case, then a 302.
// 302 rather than 301, because browsers cache 301s forever and later clicks
// would never reach us, so they couldn't be counted.
func (s *server) redirect(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !links.PlausibleCode(code) {
		http.NotFound(w, r)
		return
	}
	link, err := s.Links.Resolve(r.Context(), code)
	switch {
	case errors.Is(err, links.ErrNotFound):
		http.NotFound(w, r)
		return
	case errors.Is(err, links.ErrExpired):
		http.Error(w, "this link has expired", http.StatusGone)
		return
	case err != nil:
		s.Log.Error("resolve link", "err", err)
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Cache-Control", "private, max-age=0")
	w.Header().Set("Referrer-Policy", "unsafe-url")
	http.Redirect(w, r, link.LongURL, http.StatusFound)

	s.Events.Emit(analytics.ClickEvent{
		Code:      code,
		At:        time.Now().UTC(),
		Country:   countryFrom(r),
		UserAgent: truncate(r.UserAgent(), 256),
		Referer:   truncate(r.Referer(), 512),
	})
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	status := map[string]string{}
	code := http.StatusOK
	for _, c := range s.Ready {
		if err := c.Check(ctx); err != nil {
			status[c.Name] = err.Error()
			code = http.StatusServiceUnavailable
		} else {
			status[c.Name] = "ok"
		}
	}
	writeJSON(w, code, status)
}

// limit applies a per-client-IP rate limit to next.
func (s *server) limit(l Limiter, next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, retry := l.Allow(r.Context(), s.clientIP(r))
		if !ok {
			metrics.RateLimited.WithLabelValues(l.Name()).Inc()
			secs := int(retry.Seconds() + 0.999)
			w.Header().Set("Retry-After", itoa(max(secs, 1)))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests, slow down")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP uses X-Forwarded-For only when running behind a trusted proxy;
// otherwise any client could pick its own rate-limit key.
func (s *server) clientIP(r *http.Request) string {
	if s.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first, _, _ := strings.Cut(xff, ",")
			return strings.TrimSpace(first)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func countryFrom(r *http.Request) string {
	for _, h := range analytics.CountryHeaders {
		if v := r.Header.Get(h); v != "" {
			return analytics.NormalizeCountry(v)
		}
	}
	return "XX"
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
