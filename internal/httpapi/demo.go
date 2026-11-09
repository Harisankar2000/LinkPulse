package httpapi

import (
	_ "embed"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/Harisankar2000/linkpulse/internal/analytics"
	"github.com/Harisankar2000/linkpulse/internal/links"
)

//go:embed ui/index.html
var indexHTML []byte

// home serves the single-page dashboard.
func (s *server) home(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(indexHTML)
}

var (
	demoCountries = []string{"IN", "IN", "IN", "IN", "US", "US", "GB", "DE", "SG", "AE", "CA", "AU", "JP", "BR"}
	demoAgents    = []string{
		"Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) Mobile/15E148",
		"Mozilla/5.0 (Linux; Android 15; Pixel 9) Mobile Safari/537.36",
		"Mozilla/5.0 (Linux; Android 14; SM-S921B) Mobile Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Safari/605.1.15",
		"Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X)",
		"Googlebot/2.1 (+http://www.google.com/bot.html)",
	}
	demoReferrers = []string{
		"https://www.linkedin.com/feed/", "https://www.linkedin.com/feed/", "https://t.co/x",
		"https://www.google.com/", "https://github.com/", "https://web.whatsapp.com/", "", "",
	}
)

// simulate publishes synthetic clicks for a link through the real analytics
// pipeline (emitter -> Kafka -> consumer -> rollups), spread over the last
// 7 days, so the dashboard has something to show. Only enabled in demo mode.
func (s *server) simulate(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !links.PlausibleCode(code) {
		writeError(w, http.StatusNotFound, "not_found", "link not found")
		return
	}
	if _, err := s.Links.Get(r.Context(), code); err != nil {
		if errors.Is(err, links.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "link not found")
			return
		}
		s.Log.Error("simulate: get link", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "could not load link")
		return
	}

	n := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("n")); err == nil {
		n = min(max(v, 1), 200)
	}
	now := time.Now().UTC()
	for range n {
		// Exponential spread: most clicks are recent, a long tail goes back a week.
		ago := time.Duration(min(rand.ExpFloat64()/2, 7) * float64(24*time.Hour))
		s.Events.Emit(analytics.ClickEvent{
			Code:      code,
			At:        now.Add(-ago),
			Country:   pick(demoCountries),
			UserAgent: pick(demoAgents),
			Referer:   pick(demoReferrers),
		})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"queued": n})
}

func pick(xs []string) string { return xs[rand.IntN(len(xs))] }
