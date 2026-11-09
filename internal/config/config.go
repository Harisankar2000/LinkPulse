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

	// DemoMode enables the dashboard's click simulator.
	DemoMode bool

	// AnalyticsInProcess runs the Kafka consumer inside the API process, for
	// hosts where a separate worker costs extra. Events still go through Kafka.
	AnalyticsInProcess bool

	Kafka KafkaAuth
}

// KafkaAuth holds credentials for a hosted Kafka cluster; see analytics.KafkaOpts.
// PEM values may contain real newlines or literal \n sequences.
type KafkaAuth struct {
	TLS           bool
	CACert        string
	ClientCert    string
	ClientKey     string
	SASLMechanism string
	Username      string
	Password      string
}

func Load() (Config, error) {
	// Platforms like Render tell the app which port to bind via PORT and
	// publish the service's public URL as RENDER_EXTERNAL_URL.
	defaultAddr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		defaultAddr = ":" + p
	}
	defaultBase := getEnv("RENDER_EXTERNAL_URL", "http://localhost:8080")

	c := Config{
		HTTPAddr:          getEnv("HTTP_ADDR", defaultAddr),
		MetricsAddr:       getEnv("METRICS_ADDR", ":9091"),
		BaseURL:           strings.TrimRight(getEnv("BASE_URL", defaultBase), "/"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://linkpulse:linkpulse@localhost:5433/linkpulse?sslmode=disable"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6380/0"),
		KafkaBrokers:      splitCSV(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "link-clicks"),
		KafkaGroup:        getEnv("KAFKA_GROUP", "linkpulse-analytics"),
		TrustProxyHeaders: getEnv("TRUST_PROXY_HEADERS", "false") == "true",
		DemoMode:          getEnv("DEMO_MODE", "false") == "true",

		AnalyticsInProcess: getEnv("ANALYTICS_IN_PROCESS", "false") == "true",
		Kafka: KafkaAuth{
			TLS:           getEnv("KAFKA_TLS", "false") == "true",
			CACert:        os.Getenv("KAFKA_CA_CERT"),
			ClientCert:    os.Getenv("KAFKA_CLIENT_CERT"),
			ClientKey:     os.Getenv("KAFKA_CLIENT_KEY"),
			SASLMechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
			Username:      os.Getenv("KAFKA_USERNAME"),
			Password:      os.Getenv("KAFKA_PASSWORD"),
		},
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
