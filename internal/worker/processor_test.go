package worker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/SpaghettiRebel/Notification-Service/internal/kafka"
	"github.com/SpaghettiRebel/Notification-Service/internal/notification"
	"github.com/SpaghettiRebel/Notification-Service/internal/storage"
)

type dlqMessage struct {
	topic, key string
	event      any
}

type fakeDLQ struct{ messages []dlqMessage }

func (f *fakeDLQ) Publish(_ context.Context, topic, key string, event any) error {
	f.messages = append(f.messages, dlqMessage{topic, key, event})
	return nil
}

type fakeSender struct {
	failures int   // the first failures calls return ErrSMTPUnavailable
	crash    error // returned by every call when set
	hang     bool  // every call blocks until its context is done
	calls    int
}

func (f *fakeSender) send(ctx context.Context) error {
	f.calls++
	switch {
	case f.hang:
		<-ctx.Done()
		return ctx.Err()
	case f.crash != nil:
		return f.crash
	case f.calls <= f.failures:
		return notification.ErrSMTPUnavailable
	}
	return nil
}

func (f *fakeSender) SendTg(ctx context.Context, _ notification.Event, _ string) error {
	return f.send(ctx)
}

func (f *fakeSender) SendEmail(ctx context.Context, _ notification.Event, _ string) error {
	return f.send(ctx)
}

func (f *fakeSender) SendSMS(ctx context.Context, _ notification.Event, _ string) error {
	return f.send(ctx)
}

type statusUpdate struct {
	eventID   string
	status    storage.Status
	lastError string
}

type fakeRepository struct {
	acquisitions []storage.Acquisition
	acquireCalls int
	updates      []statusUpdate
}

func (f *fakeRepository) AcquireLock(context.Context, string, time.Duration) (storage.Acquisition, error) {
	f.acquireCalls++
	acq := f.acquisitions[0]
	f.acquisitions = f.acquisitions[1:]
	return acq, nil
}

func (f *fakeRepository) UpdateStatus(_ context.Context, eventID string, status storage.Status, lastError *string) error {
	update := statusUpdate{eventID: eventID, status: status}
	if lastError != nil {
		update.lastError = *lastError
	}
	f.updates = append(f.updates, update)
	return nil
}

var testSettings = Settings{ProcessingLease: time.Minute, MaxAttempts: 3, AttemptTimeout: 10 * time.Second}

func validEmailMessage(t *testing.T) kafka.Message {
	t.Helper()
	payload, err := json.Marshal(notification.Event{
		EventID:   "event-1",
		EventType: "christmas",
		Channel:   "email",
		MsgText:   "A sufficiently long message",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "notification.email", Offset: 1, Key: "event-1", Payload: payload}
}

func acquiredOnce() []storage.Acquisition {
	return []storage.Acquisition{{State: storage.Acquired}}
}

func TestRetryIsBoundedAndRoutesToDLQ(t *testing.T) {
	repo := &fakeRepository{acquisitions: acquiredOnce()}
	sender := &fakeSender{failures: 10}
	dlq := &fakeDLQ{}
	processor := NewProcessor(dlq, sender, repo, testSettings)

	if err := processor.Process(context.Background(), validEmailMessage(t)); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if sender.calls != 3 {
		t.Errorf("sender calls = %d, want 3", sender.calls)
	}
	if len(dlq.messages) != 1 || dlq.messages[0].topic != "notification.email.dlq" {
		t.Errorf("DLQ messages = %+v, want one on notification.email.dlq", dlq.messages)
	}
	want := []statusUpdate{{"event-1", storage.StatusFailed, notification.ErrSMTPUnavailable.Error()}}
	if !reflect.DeepEqual(repo.updates, want) {
		t.Errorf("status updates = %+v, want %+v", repo.updates, want)
	}
}

func TestBusyLeaseIsRetriedWithoutSkipping(t *testing.T) {
	now := time.Now()
	repo := &fakeRepository{acquisitions: []storage.Acquisition{
		{State: storage.Busy, LockedUntil: &now},
		{State: storage.Acquired},
	}}
	sender := &fakeSender{}
	processor := NewProcessor(&fakeDLQ{}, sender, repo, testSettings)

	if err := processor.Process(context.Background(), validEmailMessage(t)); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if repo.acquireCalls != 2 || sender.calls != 1 {
		t.Errorf("acquire calls = %d, sender calls = %d, want 2 and 1", repo.acquireCalls, sender.calls)
	}
	want := []statusUpdate{{"event-1", storage.StatusCompleted, ""}}
	if !reflect.DeepEqual(repo.updates, want) {
		t.Errorf("status updates = %+v, want %+v", repo.updates, want)
	}
}

func TestUnexpectedSenderErrorIsNotMarkedFailed(t *testing.T) {
	repo := &fakeRepository{acquisitions: acquiredOnce()}
	dlq := &fakeDLQ{}
	crash := errors.New("programming error")
	processor := NewProcessor(dlq, &fakeSender{crash: crash}, repo, testSettings)

	err := processor.Process(context.Background(), validEmailMessage(t))

	if !errors.Is(err, crash) {
		t.Fatalf("Process() error = %v, want %v", err, crash)
	}
	if len(dlq.messages) != 0 || len(repo.updates) != 0 {
		t.Errorf("DLQ messages = %v, status updates = %v, want none", dlq.messages, repo.updates)
	}
}

func TestInvalidMessageGoesToPoisonDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	processor := NewProcessor(dlq, &fakeSender{}, &fakeRepository{}, testSettings)
	raw := "{"

	err := processor.Process(context.Background(), kafka.Message{
		Topic:           "notification.email",
		Partition:       2,
		Offset:          10,
		ValidationError: "invalid JSON",
		RawPayload:      &raw,
	})

	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(dlq.messages))
	}
	if got := dlq.messages[0]; got.topic != kafka.InvalidDLQTopic || got.key != "notification.email:2:10" {
		t.Errorf("DLQ topic/key = %s/%s, want %s/notification.email:2:10", got.topic, got.key, kafka.InvalidDLQTopic)
	}
}

func TestPayloadForAnotherChannelGoesToPoisonDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	sender := &fakeSender{}
	processor := NewProcessor(dlq, sender, &fakeRepository{}, testSettings)
	msg := validEmailMessage(t)
	msg.Topic = "notification.sms"

	if err := processor.Process(context.Background(), msg); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if sender.calls != 0 || len(dlq.messages) != 1 || dlq.messages[0].topic != kafka.InvalidDLQTopic {
		t.Errorf("sender calls = %d, DLQ messages = %+v, want only a poison DLQ message", sender.calls, dlq.messages)
	}
}

func TestAttemptTimeoutIsRetriedAndRoutedToDLQ(t *testing.T) {
	repo := &fakeRepository{acquisitions: acquiredOnce()}
	sender := &fakeSender{hang: true}
	dlq := &fakeDLQ{}
	settings := testSettings
	settings.MaxAttempts = 2
	settings.AttemptTimeout = 10 * time.Millisecond
	processor := NewProcessor(dlq, sender, repo, settings)

	if err := processor.Process(context.Background(), validEmailMessage(t)); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if sender.calls != 2 || len(dlq.messages) != 1 {
		t.Errorf("sender calls = %d, DLQ messages = %d, want 2 and 1", sender.calls, len(dlq.messages))
	}
	if len(repo.updates) != 1 || repo.updates[0].status != storage.StatusFailed {
		t.Errorf("status updates = %+v, want one failed update", repo.updates)
	}
}

func TestShutdownDuringSendIsNotMarkedFailed(t *testing.T) {
	repo := &fakeRepository{acquisitions: acquiredOnce()}
	dlq := &fakeDLQ{}
	processor := NewProcessor(dlq, &fakeSender{hang: true}, repo, testSettings)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	err := processor.Process(ctx, validEmailMessage(t))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process() error = %v, want context.Canceled", err)
	}
	if len(dlq.messages) != 0 || len(repo.updates) != 0 {
		t.Errorf("DLQ messages = %v, status updates = %v, want none", dlq.messages, repo.updates)
	}
}

func TestBackoffMatchesTenacityWaitExponential(t *testing.T) {
	for _, tc := range []struct {
		failedAttempt int
		want          time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 5 * time.Second},
		{100, 5 * time.Second},
	} {
		if got := backoff(tc.failedAttempt, time.Second, 5*time.Second); got != tc.want {
			t.Errorf("backoff(%d) = %v, want %v", tc.failedAttempt, got, tc.want)
		}
	}
}
