package analytics

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Harisankar2000/linkpulse/internal/metrics"
)

// Emitter decouples redirects from Kafka. Emit puts the event on a bounded
// channel and returns immediately; if the channel is full (Kafka slow or down)
// the event is dropped and counted. Losing a few clicks is acceptable, delaying
// a redirect is not.
type Emitter struct {
	ch     chan ClickEvent
	client *kgo.Client
	topic  string
	log    *slog.Logger
	done   chan struct{}
	once   sync.Once
}

func NewEmitter(brokers []string, topic string, buffer int, log *slog.Logger, extra ...kgo.Opt) (*Emitter, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(20 * time.Millisecond), // batch records for throughput
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(), // required with LeaderAck; duplicates are tolerable for click counts
	}, extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Emitter{
		ch:     make(chan ClickEvent, buffer),
		client: client,
		topic:  topic,
		log:    log,
		done:   make(chan struct{}),
	}, nil
}

// Emit never blocks.
func (e *Emitter) Emit(ev ClickEvent) {
	select {
	case e.ch <- ev:
		metrics.AnalyticsEvents.WithLabelValues("queued").Inc()
	default:
		metrics.AnalyticsEvents.WithLabelValues("dropped").Inc()
	}
}

// Run forwards queued events to Kafka until Close is called.
func (e *Emitter) Run() {
	defer close(e.done)
	for ev := range e.ch {
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		// Keying by code sends all clicks of one link to the same partition, so a
		// single consumer owns each link's rollup rows.
		e.client.Produce(context.Background(), &kgo.Record{Key: []byte(ev.Code), Value: b},
			func(_ *kgo.Record, err error) {
				if err != nil {
					metrics.AnalyticsEvents.WithLabelValues("failed").Inc()
					e.log.Warn("produce click event failed", "err", err)
					return
				}
				metrics.AnalyticsEvents.WithLabelValues("sent").Inc()
			})
	}
}

// Close stops accepting events, drains the channel and flushes Kafka. Call it
// only after the HTTP server has stopped, so nothing calls Emit afterwards.
func (e *Emitter) Close(ctx context.Context) {
	e.once.Do(func() {
		close(e.ch)
		select {
		case <-e.done:
		case <-ctx.Done():
		}
		if err := e.client.Flush(ctx); err != nil {
			e.log.Warn("flush click events", "err", err)
		}
		e.client.Close()
	})
}
