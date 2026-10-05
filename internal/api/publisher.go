package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/SpaghettiRebel/Notification-Service/internal/kafka"
	"github.com/SpaghettiRebel/Notification-Service/internal/notification"
)

// EventPublisher delivers an event to a Kafka topic and waits for the acknowledgement.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, event any) error
}

// ChannelNotAllowedError means the greeting cannot go through the requested channel.
type ChannelNotAllowedError struct{ Detail string }

func (e *ChannelNotAllowedError) Error() string { return e.Detail }

// NotificationPublisher turns accepted requests into Kafka events.
type NotificationPublisher struct {
	events EventPublisher
}

func NewNotificationPublisher(events EventPublisher) *NotificationPublisher {
	return &NotificationPublisher{events: events}
}

// PublishBirthday sends a birthday greeting through Telegram or SMS only.
func (p *NotificationPublisher) PublishBirthday(ctx context.Context, msg SendingMessage, idempotencyKey string) (string, error) {
	if msg.MsgType == "email" {
		slog.WarnContext(ctx, "birthday_email_rejected", "channel", msg.MsgType)
		return "", &ChannelNotAllowedError{"You cannot send an Email with birthday message"}
	}
	return p.publish(ctx, "birthday", msg, idempotencyKey)
}

// PublishChristmas sends a Christmas greeting through Telegram or email only.
func (p *NotificationPublisher) PublishChristmas(ctx context.Context, msg SendingMessage, idempotencyKey string) (string, error) {
	if msg.MsgType == "sms" {
		slog.WarnContext(ctx, "christmas_sms_rejected", "channel", msg.MsgType)
		return "", &ChannelNotAllowedError{"You cannot send an SMS with Christmas message"}
	}
	return p.publish(ctx, "christmas", msg, idempotencyKey)
}

func (p *NotificationPublisher) publish(ctx context.Context, eventType string, msg SendingMessage, idempotencyKey string) (string, error) {
	event := notification.Event{
		EventID:   eventID(eventType, idempotencyKey),
		EventType: eventType,
		Channel:   msg.MsgType,
		MsgText:   msg.MsgText,
		// Microseconds, the precision of Python's datetime, so Python consumers can parse it too.
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	slog.InfoContext(ctx, "notification_event_created",
		"event_id", event.EventID, "event_type", event.EventType, "channel", event.Channel)

	topic := kafka.TopicFor(event.Channel)
	if err := p.events.Publish(ctx, topic, event.EventID, event); err != nil {
		return "", err
	}
	slog.InfoContext(ctx, "notification_event_published", "event_id", event.EventID, "topic", topic)
	return event.EventID, nil
}

// eventID is deterministic, so a retried request with the same Idempotency-Key
// produces the same event. It equals Python's uuid5(NAMESPACE_URL, name).
func eventID(eventType, idempotencyKey string) string {
	name := "notification:" + eventType + ":" + idempotencyKey
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String()
}
