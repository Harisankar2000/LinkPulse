package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Harisankar2000/linkpulse/internal/metrics"
	"github.com/Harisankar2000/linkpulse/internal/store"
)

// Sink persists aggregated counts (implemented by *store.Store).
type Sink interface {
	AddClicks(ctx context.Context, counts map[store.StatKey]int64) error
}

// Consumer reads click events in batches, aggregates them in memory and
// writes one upsert per (code, hour, country, device, referrer).
//
// Delivery is at-least-once: offsets are committed only after the batch is in
// Postgres. A crash between the two can count a batch twice.
type Consumer struct {
	client *kgo.Client
	sink   Sink
	log    *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, sink Sink, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, sink: sink, log: log}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()
	for {
		fetches := c.client.PollRecords(ctx, 10_000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			c.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
		})

		counts := Aggregate(fetches.Records())
		if err := c.writeWithRetry(ctx, counts); err != nil {
			c.client.AllowRebalance()
			return err
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && !errors.Is(err, context.Canceled) {
			c.log.Warn("commit offsets", "err", err)
		}
		c.client.AllowRebalance()
	}
}

func (c *Consumer) writeWithRetry(ctx context.Context, counts map[store.StatKey]int64) error {
	if len(counts) == 0 {
		return nil
	}
	backoff := 200 * time.Millisecond
	for {
		err := c.sink.AddClicks(ctx, counts)
		if err == nil {
			metrics.ConsumerBatches.Inc()
			metrics.ConsumerRowsWritten.Add(float64(len(counts)))
			return nil
		}
		c.log.Error("write rollups failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

// Aggregate turns raw records into rollup increments. Malformed records are
// counted and skipped rather than blocking the partition.
func Aggregate(records []*kgo.Record) map[store.StatKey]int64 {
	counts := make(map[store.StatKey]int64)
	for _, r := range records {
		var ev ClickEvent
		if err := json.Unmarshal(r.Value, &ev); err != nil || ev.Code == "" || ev.At.IsZero() {
			metrics.ConsumedEvents.WithLabelValues("invalid").Inc()
			continue
		}
		metrics.ConsumedEvents.WithLabelValues("ok").Inc()
		counts[store.StatKey{
			Code:     ev.Code,
			Bucket:   ev.At.UTC().Truncate(time.Hour),
			Country:  NormalizeCountry(ev.Country),
			Device:   DeviceType(ev.UserAgent),
			Referrer: ReferrerHost(ev.Referer),
		}]++
	}
	return counts
}
