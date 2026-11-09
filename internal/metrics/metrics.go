// Package metrics defines the Prometheus metrics exported by the services.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "linkpulse_http_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "linkpulse_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"route"})

	CacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "linkpulse_cache_lookups_total",
		Help: "Link cache lookups by result (l1_hit, hit, miss, negative_hit, error, corrupt).",
	}, []string{"result"})

	CacheLoads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "linkpulse_cache_db_loads_total",
		Help: "Database loads after cache misses (after singleflight deduplication).",
	})

	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "linkpulse_rate_limited_total",
		Help: "Requests rejected by the rate limiter, by limiter name.",
	}, []string{"limiter"})

	AnalyticsEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "linkpulse_analytics_events_total",
		Help: "Click events on the producer side by result (queued, dropped, sent, failed).",
	}, []string{"result"})

	ConsumedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "linkpulse_analytics_consumed_total",
		Help: "Click events read by the analytics consumer, by result (ok, invalid).",
	}, []string{"result"})

	ConsumerBatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "linkpulse_analytics_batches_total",
		Help: "Batches written to Postgres by the analytics consumer.",
	})

	ConsumerRowsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "linkpulse_analytics_rollup_rows_total",
		Help: "Rollup rows upserted by the analytics consumer.",
	})
)
