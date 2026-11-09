// Command api serves link creation, redirects and stats.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Harisankar2000/linkpulse/internal/analytics"
	"github.com/Harisankar2000/linkpulse/internal/cache"
	"github.com/Harisankar2000/linkpulse/internal/config"
	"github.com/Harisankar2000/linkpulse/internal/httpapi"
	"github.com/Harisankar2000/linkpulse/internal/links"
	"github.com/Harisankar2000/linkpulse/internal/ratelimit"
	"github.com/Harisankar2000/linkpulse/internal/shortid"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	db, err := store.New(startCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(startCtx); err != nil {
		return err
	}

	ropts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		// Don't wrap err: go-redis echoes the whole URL, password included.
		return errors.New("invalid REDIS_URL: expected just the URL, e.g. rediss://default:<password>@<host>:6379 (not a redis-cli command)")
	}
	ropts.PoolSize = 100
	rdb := redis.NewClient(ropts)
	defer rdb.Close()
	if err := rdb.Ping(startCtx).Err(); err != nil {
		log.Warn("redis not reachable at startup; cache and rate limits degrade until it is", "err", err)
	}

	kafkaOpts, err := analytics.KafkaOpts(cfg.Kafka)
	if err != nil {
		return err
	}
	emitter, err := analytics.NewEmitter(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.EventBufferSize, log, kafkaOpts...)
	if err != nil {
		return err
	}
	go emitter.Run()

	// On free hosting tiers without background workers, run the consumer here.
	consumerDone := make(chan struct{})
	if cfg.AnalyticsInProcess {
		consumer, err := analytics.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, db, log, kafkaOpts...)
		if err != nil {
			return err
		}
		go func() {
			defer close(consumerDone)
			log.Info("analytics consumer running in-process", "topic", cfg.KafkaTopic)
			if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("in-process analytics consumer stopped", "err", err)
			}
		}()
	} else {
		close(consumerDone)
	}

	linkCache := cache.New(rdb, cfg.RedisNamespace, db.GetLink, cfg.CacheTTL, cfg.NegativeCacheTTL, log)
	service := links.NewService(db, linkCache, shortid.NewAllocator(db, store.IDBlockSize), cfg.BaseURL)

	deps := httpapi.Deps{
		Links:      service,
		Events:     emitter,
		BaseURL:    cfg.BaseURL,
		TrustProxy: cfg.TrustProxyHeaders,
		DemoMode:   cfg.DemoMode,
		Log:        log,
		Ready: []httpapi.Checker{
			{Name: "postgres", Check: db.Ping},
			{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
	}
	// Assign only non-nil limiters: a nil *Limiter in an interface is not a nil interface.
	if l := ratelimit.New(rdb, cfg.RedisNamespace, "create", cfg.CreatePerMinute/60, cfg.CreateBurst, log); l != nil {
		deps.CreateLimiter = l
	}
	if l := ratelimit.New(rdb, cfg.RedisNamespace, "redirect", cfg.RedirectPerSecond, cfg.RedirectBurst, log); l != nil {
		deps.RedirectLimiter = l
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(deps),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr, "base_url", cfg.BaseURL)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Order matters: stop taking requests first, then flush queued click events.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	emitter.Close(shutdownCtx)
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
	}
	log.Info("shutdown complete")
	return nil
}
