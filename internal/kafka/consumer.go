package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/SpaghettiRebel/Notification-Service/internal/config"
)

const (
	consumerGroup      = "notifications"
	maxRawPayloadRunes = 4096
)

// Message is one Kafka record as seen by the worker. Records that cannot be
// decoded still reach the handler, with ValidationError set, so they can be
// routed to the poison-message DLQ and committed.
type Message struct {
	Topic           string
	Partition       int32
	Offset          int64
	Key             string
	Payload         []byte
	ValidationError string
	RawPayload      *string
}

type consumerClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
	Close()
}

// Consumer reads the channel topics as part of the notifications group.
type Consumer struct {
	client consumerClient
}

func NewConsumer(cfg config.Worker) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers()...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(Topics()...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Partitions stay assigned while a record is processed; the group waits
		// up to the rebalance timeout, which librdkafka derives from max.poll.interval.ms.
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(cfg.MaxPollInterval()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Run hands records to handle one at a time and commits each offset only after
// handle succeeds. It returns ctx.Err() on shutdown and the first error otherwise.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, Message) error) error {
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if err := ctx.Err(); err != nil {
			return err
		}
		if fetches.IsClientClosed() {
			return kgo.ErrClientClosed
		}
		if err := fetchError(fetches); err != nil {
			return err
		}
		for _, record := range fetches.Records() {
			if err := handle(ctx, decode(record)); err != nil {
				return err
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
		}
		c.client.AllowRebalance()
	}
}

func (c *Consumer) Close() {
	c.client.AllowRebalance()
	c.client.Close()
}

// fetchError returns the first fatal fetch error. Data loss is only logged:
// franz-go has already moved the partition to the first valid offset.
func fetchError(fetches kgo.Fetches) error {
	var fatal error
	fetches.EachError(func(topic string, partition int32, err error) {
		var dataLoss *kgo.ErrDataLoss
		switch {
		case errors.As(err, &dataLoss):
			slog.Warn("kafka data loss detected", "topic", topic, "partition", partition, "error", err)
		case fatal == nil:
			fatal = fmt.Errorf("fetch %s[%d]: %w", topic, partition, err)
		}
	})
	return fatal
}

func decode(record *kgo.Record) Message {
	msg := Message{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset}
	if err := decodeInto(&msg, record); err != nil {
		slog.Warn("invalid kafka message",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
		msg.Payload = nil
		msg.ValidationError = err.Error()
		if record.Value != nil {
			raw := TruncateRunes(strings.ToValidUTF8(string(record.Value), "�"), maxRawPayloadRunes)
			msg.RawPayload = &raw
		}
	}
	return msg
}

func decodeInto(msg *Message, record *kgo.Record) error {
	if record.Key != nil {
		if !utf8.Valid(record.Key) {
			return errors.New("kafka message key is not valid UTF-8")
		}
		msg.Key = string(record.Key)
	}
	if record.Value == nil {
		return errors.New("kafka message has no value")
	}
	if !utf8.Valid(record.Value) {
		return errors.New("kafka message value is not valid UTF-8")
	}

	var payload map[string]json.RawMessage
	err := json.Unmarshal(record.Value, &payload)
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) || (err == nil && payload == nil) {
		return errors.New("kafka payload must be a JSON object")
	}
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	if record.Key == nil {
		// A missing or non-string event_id leaves the key empty and is rejected below.
		_ = json.Unmarshal(payload["event_id"], &msg.Key)
	}
	if msg.Key == "" {
		return errors.New("kafka message key/event_id is missing")
	}
	msg.Payload = record.Value
	return nil
}

// TruncateRunes cuts s to at most n characters without splitting a UTF-8 sequence.
func TruncateRunes(s string, n int) string {
	for i := range s {
		if n == 0 {
			return s[:i]
		}
		n--
	}
	return s
}
