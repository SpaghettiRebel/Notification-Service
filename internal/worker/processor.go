// Package worker processes notification events: validation, the processing
// lease, sender retries, DLQ routing and the final status.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SpaghettiRebel/Notification-Service/internal/kafka"
	"github.com/SpaghettiRebel/Notification-Service/internal/notification"
	"github.com/SpaghettiRebel/Notification-Service/internal/storage"
)

type DLQPublisher interface {
	Publish(ctx context.Context, topic, key string, event any) error
}

// Sender delivers an event through one channel. Implementations must return
// when ctx is done and should pass idempotencyKey on to the provider.
type Sender interface {
	SendTg(ctx context.Context, ev notification.Event, idempotencyKey string) error
	SendEmail(ctx context.Context, ev notification.Event, idempotencyKey string) error
	SendSMS(ctx context.Context, ev notification.Event, idempotencyKey string) error
}

type Repository interface {
	AcquireLock(ctx context.Context, eventID string, lease time.Duration) (storage.Acquisition, error)
	UpdateStatus(ctx context.Context, eventID string, status storage.Status, lastError *string) error
}

type Settings struct {
	ProcessingLease time.Duration
	MaxAttempts     int
	AttemptTimeout  time.Duration
	RetryMin        time.Duration
	RetryMax        time.Duration
}

var (
	errAttemptTimeout   = errors.New("sender attempt timed out")
	errAlreadyCompleted = errors.New("event is already completed")
)

// failedEvent is what lands in <topic>.dlq once sender retries are exhausted.
type failedEvent struct {
	notification.Event
	SourceTopic string `json:"source_topic"`
	Error       string `json:"error"`
}

// invalidEvent is what lands in notification.invalid.dlq for poison messages.
type invalidEvent struct {
	EventID     string  `json:"event_id"`
	EventType   string  `json:"event_type"`
	SourceTopic string  `json:"source_topic"`
	Partition   int32   `json:"partition"`
	Offset      int64   `json:"offset"`
	Error       string  `json:"error"`
	RawPayload  *string `json:"raw_payload"`
}

type Processor struct {
	dlq      DLQPublisher
	sender   Sender
	repo     Repository
	settings Settings
}

func NewProcessor(dlq DLQPublisher, sender Sender, repo Repository, settings Settings) *Processor {
	return &Processor{dlq: dlq, sender: sender, repo: repo, settings: settings}
}

// Process handles one Kafka message using recoverable lease semantics.
// A nil error means the offset may be committed.
func (p *Processor) Process(ctx context.Context, msg kafka.Message) error {
	if msg.ValidationError != "" {
		return p.publishInvalid(ctx, msg, msg.ValidationError)
	}

	ev, err := notification.ParseEvent(msg.Payload)
	if err == nil && ev.EventID != msg.Key {
		err = errors.New("kafka key does not match payload event_id")
	}
	if err == nil && msg.Topic != kafka.TopicFor(ev.Channel) {
		err = errors.New("kafka topic does not match payload channel")
	}
	if err != nil {
		raw := kafka.TruncateRunes(string(msg.Payload), 4096)
		msg.RawPayload = &raw
		return p.publishInvalid(ctx, msg, err.Error())
	}

	if err := p.acquire(ctx, ev.EventID); err != nil {
		if errors.Is(err, errAlreadyCompleted) {
			slog.InfoContext(ctx, "event is already completed, skipping", "event_id", ev.EventID)
			return nil
		}
		return err
	}

	sendErr := p.sendWithRetry(ctx, msg.Topic, ev)
	if sendErr == nil {
		return p.repo.UpdateStatus(ctx, ev.EventID, storage.StatusCompleted, nil)
	}
	if !isRetriable(sendErr) {
		return sendErr
	}

	slog.ErrorContext(ctx, "failed to send message after retries", "event_id", ev.EventID, "error", sendErr)
	reason := sendErr.Error()
	failed := failedEvent{Event: ev, SourceTopic: msg.Topic, Error: reason}
	if err := p.dlq.Publish(ctx, kafka.DLQTopicFor(msg.Topic), ev.EventID, failed); err != nil {
		return fmt.Errorf("publish %s to DLQ: %w", ev.EventID, err)
	}
	return p.repo.UpdateStatus(ctx, ev.EventID, storage.StatusFailed, &reason)
}

// acquire blocks until this worker holds the processing lease for eventID.
func (p *Processor) acquire(ctx context.Context, eventID string) error {
	for {
		acq, err := p.repo.AcquireLock(ctx, eventID, p.settings.ProcessingLease)
		if err != nil {
			return err
		}
		switch acq.State {
		case storage.Acquired:
			return nil
		case storage.AlreadyCompleted:
			return errAlreadyCompleted
		}

		retryAfter := time.Second
		if acq.LockedUntil != nil {
			retryAfter = max(time.Until(*acq.LockedUntil), 100*time.Millisecond)
		}
		slog.WarnContext(ctx, "event is being processed, retrying lock",
			"event_id", eventID, "retry_after", retryAfter)
		if err := sleep(ctx, retryAfter); err != nil {
			return err
		}
	}
}

func (p *Processor) sendWithRetry(ctx context.Context, topic string, ev notification.Event) error {
	var err error
	for attempt := 1; attempt <= p.settings.MaxAttempts; attempt++ {
		if attempt > 1 {
			if err := sleep(ctx, backoff(attempt-1, p.settings.RetryMin, p.settings.RetryMax)); err != nil {
				return err
			}
		}
		if err = p.sendOnce(ctx, topic, ev); err == nil || !isRetriable(err) {
			return err
		}
	}
	return err
}

func (p *Processor) sendOnce(ctx context.Context, topic string, ev notification.Event) error {
	attemptCtx, cancel := context.WithTimeoutCause(ctx, p.settings.AttemptTimeout, errAttemptTimeout)
	defer cancel()

	var err error
	switch topic {
	case kafka.TopicFor("tg"):
		err = p.sender.SendTg(attemptCtx, ev, ev.EventID)
	case kafka.TopicFor("email"):
		err = p.sender.SendEmail(attemptCtx, ev, ev.EventID)
	case kafka.TopicFor("sms"):
		err = p.sender.SendSMS(attemptCtx, ev, ev.EventID)
	default:
		return fmt.Errorf("%w: %s", notification.ErrUnsupportedTopic, topic)
	}
	// Only our own per-attempt deadline is retriable; shutdown cancellation is not.
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(attemptCtx), errAttemptTimeout) {
		return fmt.Errorf("%w: %w", errAttemptTimeout, err)
	}
	return err
}

func (p *Processor) publishInvalid(ctx context.Context, msg kafka.Message, reason string) error {
	key := msg.Key
	if key == "" {
		key = fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
	}
	err := p.dlq.Publish(ctx, kafka.InvalidDLQTopic, key, invalidEvent{
		EventID:     key,
		EventType:   "invalid_notification",
		SourceTopic: msg.Topic,
		Partition:   msg.Partition,
		Offset:      msg.Offset,
		Error:       reason,
		RawPayload:  msg.RawPayload,
	})
	if err != nil {
		return fmt.Errorf("publish invalid message %s to DLQ: %w", key, err)
	}
	return nil
}

func isRetriable(err error) bool {
	return errors.Is(err, notification.ErrTgUnavailable) ||
		errors.Is(err, notification.ErrSMSUnavailable) ||
		errors.Is(err, notification.ErrSMTPUnavailable) ||
		errors.Is(err, errAttemptTimeout)
}

// backoff mirrors tenacity's wait_exponential(multiplier=1, min, max).
func backoff(failedAttempt int, lo, hi time.Duration) time.Duration {
	return min(max(time.Second<<min(failedAttempt-1, 32), lo), hi)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
