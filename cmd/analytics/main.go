// Command analytics consumes click events from Kafka and maintains the
// hourly rollups in Postgres. Run as many replicas as there are partitions.
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

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Harisankar2000/linkpulse/internal/analytics"
	"github.com/Harisankar2000/linkpulse/internal/config"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("analytics consumer stopped", "err", err)
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

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	defer metricsSrv.Close()

	consumer, err := analytics.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, db, log)
	if err != nil {
		return err
	}
	log.Info("analytics consumer started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)
	err = consumer.Run(ctx)
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	log.Info("analytics consumer stopped")
	return err
}
