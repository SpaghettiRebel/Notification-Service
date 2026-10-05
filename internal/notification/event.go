// Package notification defines the Kafka event shared by the public API and the worker.
package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"
)

// Channels lists the delivery channels; each one has its own Kafka topic.
var Channels = []string{"tg", "email", "sms"}

// EventTypes lists the supported greetings.
var EventTypes = []string{"birthday", "christmas"}

// Retriable provider failures, and the error for a topic the worker does not handle.
var (
	ErrTgUnavailable    = errors.New("telegram server unavailable")
	ErrSMSUnavailable   = errors.New("sms server unavailable")
	ErrSMTPUnavailable  = errors.New("smtp server unavailable")
	ErrUnsupportedTopic = errors.New("unsupported notification topic")
)

// Event is the message published to notification.<channel>.
type Event struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Channel   string    `json:"channel"`
	MsgText   string    `json:"msg_text"`
	CreatedAt time.Time `json:"created_at"`
}

var eventFields = []string{"event_id", "event_type", "channel", "msg_text", "created_at"}

// ParseEvent decodes an event and rejects anything the contract does not allow.
// Field names are compared exactly because encoding/json matches them
// case-insensitively, and absent fields would otherwise decode as zero values.
func ParseEvent(raw []byte) (Event, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Event{}, fmt.Errorf("decode notification event: %w", err)
	}
	for name := range fields {
		if !slices.Contains(eventFields, name) {
			return Event{}, fmt.Errorf("%s: extra fields are not permitted", name)
		}
	}
	var ev Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Event{}, fmt.Errorf("decode notification event: %w", err)
	}
	if err := ev.Validate(); err != nil {
		return Event{}, err
	}
	return ev, nil
}

// Validate checks every field; lengths are counted in characters, not bytes.
func (e Event) Validate() error {
	var errs []error
	if n := utf8.RuneCountInString(e.EventID); n < 1 || n > 128 {
		errs = append(errs, errors.New("event_id: length must be between 1 and 128 characters"))
	}
	if !slices.Contains(EventTypes, e.EventType) {
		errs = append(errs, fmt.Errorf("event_type: must be one of %v, got %q", EventTypes, e.EventType))
	}
	if !slices.Contains(Channels, e.Channel) {
		errs = append(errs, fmt.Errorf("channel: must be one of %v, got %q", Channels, e.Channel))
	}
	if n := utf8.RuneCountInString(e.MsgText); n < 10 || n > 400 {
		errs = append(errs, errors.New("msg_text: length must be between 10 and 400 characters"))
	}
	if e.CreatedAt.IsZero() {
		errs = append(errs, errors.New("created_at: field required"))
	}
	return errors.Join(errs...)
}
