// Package analytics records clicks without slowing down redirects.
//
// Redirect handler -> Emitter (bounded channel, never blocks)
// -> Kafka topic (keyed by code) -> Consumer (batch + aggregate)
// -> hourly rollups in Postgres.
package analytics

import (
	"net/url"
	"strings"
	"time"
)

// ClickEvent is what the redirect path publishes. It holds raw request data;
// parsing (device type, referrer host) happens in the consumer, off the hot path.
// The client IP is deliberately not recorded.
type ClickEvent struct {
	Code      string    `json:"c"`
	At        time.Time `json:"t"`
	Country   string    `json:"cc,omitempty"`
	UserAgent string    `json:"ua,omitempty"`
	Referer   string    `json:"r,omitempty"`
}

// CountryHeaders are set by CDNs and load balancers that do GeoIP lookups.
// Behind Cloudflare, CF-IPCountry is present on every request; in local
// development none is set and clicks are recorded as "XX" (unknown).
var CountryHeaders = []string{"CF-IPCountry", "X-Vercel-IP-Country", "CloudFront-Viewer-Country", "X-Country-Code"}

// NormalizeCountry returns an upper-case ISO 3166 alpha-2 code, or "XX".
func NormalizeCountry(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if len(v) != 2 || v[0] < 'A' || v[0] > 'Z' || v[1] < 'A' || v[1] > 'Z' {
		return "XX"
	}
	return v
}

// DeviceType classifies a User-Agent as bot, tablet, mobile or desktop.
func DeviceType(ua string) string {
	u := strings.ToLower(ua)
	switch {
	case u == "":
		return "unknown"
	case containsAny(u, "bot", "crawler", "spider", "slurp", "curl/", "wget/", "python-requests", "go-http-client", "headless"):
		return "bot"
	case containsAny(u, "ipad", "tablet") || (strings.Contains(u, "android") && !strings.Contains(u, "mobile")):
		return "tablet"
	case containsAny(u, "mobile", "iphone", "ipod", "android", "windows phone"):
		return "mobile"
	default:
		return "desktop"
	}
}

// ReferrerHost reduces a Referer header to its host ("direct" if absent).
func ReferrerHost(ref string) string {
	if ref == "" {
		return "direct"
	}
	u, err := url.Parse(ref)
	if err != nil || u.Hostname() == "" {
		return "unknown"
	}
	return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
