package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeConsumerClient struct {
	records   []*kgo.Record
	commitErr error
	committed []*kgo.Record
	stop      context.CancelFunc
}

func (f *fakeConsumerClient) PollRecords(context.Context, int) kgo.Fetches {
	if len(f.records) == 0 {
		f.stop()
		return nil
	}
	record := f.records[0]
	f.records = f.records[1:]
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      record.Topic,
		Partitions: []kgo.FetchPartition{{Partition: record.Partition, Records: []*kgo.Record{record}}},
	}}}}
}

func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = append(f.committed, records...)
	return nil
}

func (f *fakeConsumerClient) AllowRebalance() {}
func (f *fakeConsumerClient) Close()          {}

type fakeProducerClient struct {
	records []*kgo.Record
	closed  bool
}

func (f *fakeProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, 0, len(records))
	for _, record := range records {
		results = append(results, kgo.ProduceResult{Record: record})
	}
	return results
}

func (f *fakeProducerClient) Ping(context.Context) error { return nil }
func (f *fakeProducerClient) Close()                     { f.closed = true }

func emailRecord() *kgo.Record {
	return &kgo.Record{
		Topic:  "notification.email",
		Offset: 1,
		Key:    []byte("event-1"),
		Value:  []byte(`{"event_id": "event-1", "msg_text": "hello"}`),
	}
}

func runConsumer(client *fakeConsumerClient, handle func(context.Context, Message) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.stop = cancel
	return (&Consumer{client: client}).Run(ctx, handle)
}

func TestConsumerCommitsAfterHandlerSucceeds(t *testing.T) {
	record := emailRecord()
	client := &fakeConsumerClient{records: []*kgo.Record{record}}
	var handled []Message

	err := runConsumer(client, func(_ context.Context, msg Message) error {
		handled = append(handled, msg)
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if len(handled) != 1 || handled[0].Topic != "notification.email" || handled[0].Key != "event-1" {
		t.Errorf("handled = %+v, want one notification.email message with key event-1", handled)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Errorf("committed = %v, want the handled record", client.committed)
	}
}

func TestConsumerDoesNotCommitWhenHandlerFails(t *testing.T) {
	client := &fakeConsumerClient{records: []*kgo.Record{emailRecord()}}
	handlerErr := errors.New("processing failed")

	err := runConsumer(client, func(context.Context, Message) error { return handlerErr })

	if !errors.Is(err, handlerErr) {
		t.Fatalf("Run() error = %v, want %v", err, handlerErr)
	}
	if len(client.committed) != 0 {
		t.Errorf("committed = %v, want nothing", client.committed)
	}
}

func TestCommitErrorIsNotIgnored(t *testing.T) {
	commitErr := errors.New("commit failed")
	client := &fakeConsumerClient{records: []*kgo.Record{emailRecord()}, commitErr: commitErr}

	err := runConsumer(client, func(context.Context, Message) error { return nil })

	if !errors.Is(err, commitErr) {
		t.Fatalf("Run() error = %v, want %v", err, commitErr)
	}
}

func TestDecodeRejectsInvalidRecords(t *testing.T) {
	for _, tc := range []struct {
		name      string
		record    *kgo.Record
		wantError string
		wantRaw   string
	}{
		{"non-object payload", &kgo.Record{Value: []byte("[]")}, "JSON object", "[]"},
		{"broken JSON", &kgo.Record{Value: []byte("{")}, "invalid JSON", "{"},
		{"no key and no event_id", &kgo.Record{Value: []byte(`{"msg_text": "hello"}`)}, "key/event_id", `{"msg_text": "hello"}`},
		{"invalid UTF-8", &kgo.Record{Value: []byte{'{', 0xff, '}'}}, "UTF-8", "{�}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := decode(tc.record)

			if !strings.Contains(msg.ValidationError, tc.wantError) {
				t.Errorf("ValidationError = %q, want it to mention %q", msg.ValidationError, tc.wantError)
			}
			if msg.RawPayload == nil || *msg.RawPayload != tc.wantRaw {
				t.Errorf("RawPayload = %v, want %q", msg.RawPayload, tc.wantRaw)
			}
		})
	}
}

func TestDecodeFallsBackToPayloadEventID(t *testing.T) {
	msg := decode(&kgo.Record{Value: []byte(`{"event_id": "event-1"}`)})

	if msg.ValidationError != "" || msg.Key != "event-1" {
		t.Errorf("decode() = %+v, want key event-1 without a validation error", msg)
	}
}

func TestProducerPublishesJSONAndCloses(t *testing.T) {
	client := &fakeProducerClient{}
	producer := &Producer{client: client, deliveryTimeout: time.Second}

	err := producer.Publish(context.Background(), "notification.email.dlq", "event-1",
		map[string]string{"event_id": "event-1"})
	producer.Close()

	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(client.records) != 1 || string(client.records[0].Value) != `{"event_id":"event-1"}` {
		t.Errorf("produced = %v, want one JSON record", client.records)
	}
	if !client.closed {
		t.Error("producer client was not closed")
	}
}
