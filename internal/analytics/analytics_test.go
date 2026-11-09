package analytics

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Harisankar2000/linkpulse/internal/store"
)

func TestDeviceType(t *testing.T) {
	tests := []struct{ ua, want string }{
		{"", "unknown"},
		{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0", "desktop"},
		{"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148", "mobile"},
		{"Mozilla/5.0 (Linux; Android 14; Pixel 8) Mobile Safari/537.36", "mobile"},
		{"Mozilla/5.0 (Linux; Android 13; SM-X700) Safari/537.36", "tablet"},
		{"Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X)", "tablet"},
		{"Googlebot/2.1 (+http://www.google.com/bot.html)", "bot"},
		{"curl/8.5.0", "bot"},
	}
	for _, tt := range tests {
		if got := DeviceType(tt.ua); got != tt.want {
			t.Errorf("DeviceType(%q) = %q, want %q", tt.ua, got, tt.want)
		}
	}
}

func TestReferrerHost(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", "direct"},
		{"https://www.Google.com/search?q=x", "google.com"},
		{"https://t.co/abc", "t.co"},
		{"not a url", "unknown"},
	}
	for _, tt := range tests {
		if got := ReferrerHost(tt.in); got != tt.want {
			t.Errorf("ReferrerHost(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeCountry(t *testing.T) {
	tests := map[string]string{"in": "IN", " US ": "US", "": "XX", "T1": "XX", "IND": "XX"}
	for in, want := range tests {
		if got := NormalizeCountry(in); got != want {
			t.Errorf("NormalizeCountry(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAggregate(t *testing.T) {
	at := time.Date(2026, 11, 10, 14, 35, 0, 0, time.UTC)
	rec := func(ev ClickEvent) *kgo.Record {
		b, _ := json.Marshal(ev)
		return &kgo.Record{Value: b}
	}
	records := []*kgo.Record{
		rec(ClickEvent{Code: "abc1234", At: at, Country: "in", UserAgent: "iPhone Mobile"}),
		rec(ClickEvent{Code: "abc1234", At: at.Add(10 * time.Minute), Country: "IN", UserAgent: "iPhone Mobile"}),
		rec(ClickEvent{Code: "abc1234", At: at.Add(time.Hour), Country: "IN", UserAgent: "iPhone Mobile"}),
		{Value: []byte("{not json")},
		rec(ClickEvent{Code: "", At: at}),
	}

	got := Aggregate(records)
	sameHour := store.StatKey{Code: "abc1234", Bucket: at.Truncate(time.Hour), Country: "IN", Device: "mobile", Referrer: "direct"}
	nextHour := sameHour
	nextHour.Bucket = sameHour.Bucket.Add(time.Hour)

	if len(got) != 2 || got[sameHour] != 2 || got[nextHour] != 1 {
		t.Fatalf("unexpected aggregation: %+v", got)
	}
}
