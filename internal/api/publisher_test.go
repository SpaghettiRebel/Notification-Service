package api

import (
	"context"
	"testing"
)

type fakeEvents struct{ keys []string }

func (f *fakeEvents) Publish(_ context.Context, _, key string, _ any) error {
	f.keys = append(f.keys, key)
	return nil
}

func TestIdempotencyKeyProducesStableEventID(t *testing.T) {
	events := &fakeEvents{}
	publisher := NewNotificationPublisher(events)
	msg := SendingMessage{MsgType: "tg", MsgText: "A sufficiently long message"}
	ctx := context.Background()

	first, err := publisher.PublishBirthday(ctx, msg, "request-key-123")
	if err != nil {
		t.Fatal(err)
	}
	second, err := publisher.PublishBirthday(ctx, msg, "request-key-123")
	if err != nil {
		t.Fatal(err)
	}
	christmas, err := publisher.PublishChristmas(ctx, msg, "request-key-123")
	if err != nil {
		t.Fatal(err)
	}

	if first != second || events.keys[0] != events.keys[1] {
		t.Errorf("event IDs %q and %q differ for the same idempotency key", first, second)
	}
	if first == christmas {
		t.Errorf("birthday and christmas share event ID %q", first)
	}
	// uuid.uuid5(uuid.NAMESPACE_URL, "notification:birthday:request-key-123") in Python.
	if want := "3adbb0d2-8eb1-54e7-bbd4-a56448d28212"; first != want {
		t.Errorf("event ID = %q, want %q as produced by the Python version", first, want)
	}
}
