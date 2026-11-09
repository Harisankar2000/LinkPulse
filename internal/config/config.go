// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string
	MetricsAddr string
	BaseURL     string

	DatabaseURL string
	RedisURL    string

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroup   string

	CacheTTL         time.Duration
	NegativeCacheTTL time.Duration

	// Rate limits. A rate of 0 disables that limiter.
	CreatePerMinute   float64
	CreateBurst       int
	RedirectPerSecond float64
	RedirectBurst     int
	TrustProxyHeaders bool

	EventBufferSize int
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:          getEnv("HTTP_ADDR", ":8080"),
		MetricsAddr:       getEnv("METRICS_ADDR", ":9091"),
		BaseURL:           strings.TrimRight(getEnv("BASE_URL", "http://localhost:8080"), "/"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://linkpulse:linkpulse@localhost:5433/linkpulse?sslmode=disable"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6380/0"),
		KafkaBrokers:      splitCSV(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "link-clicks"),
		KafkaGroup:        getEnv("KAFKA_GROUP", "linkpulse-analytics"),
		TrustProxyHeaders: getEnv("TRUST_PROXY_HEADERS", "false") == "true",
	}

	var err error
	if c.CacheTTL, err = getDuration("CACHE_TTL", 24*time.Hour); err != nil {
		return c, err
	}
	if c.NegativeCacheTTL, err = getDuration("NEGATIVE_CACHE_TTL", 30*time.Second); err != nil {
		return c, err
	}
	if c.CreatePerMinute, err = getFloat("RATE_CREATE_PER_MIN", 30); err != nil {
		return c, err
	}
	if c.CreateBurst, err = getInt("RATE_CREATE_BURST", 10); err != nil {
		return c, err
	}
	if c.RedirectPerSecond, err = getFloat("RATE_REDIRECT_RPS", 100); err != nil {
		return c, err
	}
	if c.RedirectBurst, err = getInt("RATE_REDIRECT_BURST", 200); err != nil {
		return c, err
	}
	if c.EventBufferSize, err = getInt("EVENT_BUFFER_SIZE", 10000); err != nil {
		return c, err
	}
	return c, nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config %s: %w", key, err)
	}
	return d, nil
}

func getInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config %s: %w", key, err)
	}
	return n, nil
}

func getFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("config %s: %w", key, err)
	}
	return f, nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
