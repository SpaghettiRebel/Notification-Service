package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/SpaghettiRebel/Notification-Service/internal/config"
)

type producerClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Ping(ctx context.Context) error
	Close()
}

// Producer publishes JSON events and waits until Kafka acknowledges each one.
type Producer struct {
	client          producerClient
	deliveryTimeout time.Duration
}

// NewProducer creates a producer that waits for all in-sync replicas;
// franz-go makes it idempotent by default. Missing topics are created on
// first publish, as librdkafka producers do.
func NewProducer(cfg config.Kafka) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, deliveryTimeout: cfg.DeliveryTimeout()}, nil
}

// Publish serialises event to JSON and blocks until Kafka acknowledges it.
func (p *Producer) Publish(ctx context.Context, topic, key string, event any) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event for %s: %w", topic, err)
	}
	slog.InfoContext(ctx, "kafka_message_publish_started", "topic", topic, "key", key)

	ctx, cancel := context.WithTimeout(ctx, p.deliveryTimeout+2*time.Second)
	defer cancel()
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("deliver message to %s: %w", topic, err)
	}

	slog.InfoContext(ctx, "kafka_message_delivered",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "key", key)
	return nil
}

// Ping checks that a broker is reachable; it backs GET /health.
func (p *Producer) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return p.client.Ping(ctx)
}

func (p *Producer) Close() { p.client.Close() }
