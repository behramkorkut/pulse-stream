package collector

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// fakeWriter remplace *kafka.Writer dans les tests : aucun broker nécessaire.
type fakeWriter struct {
	msgs   []kafka.Message
	err    error
	block  bool // simule un broker qui ne répond plus
	closed bool
}

func (f *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

func sampleEvent() event.Event {
	return event.Event{
		ID:         "evt-1",
		Type:       event.TypePageview,
		SiteID:     "site-42",
		VisitorID:  "visitor-7",
		URL:        "https://example.com/produits",
		Timestamp:  time.Date(2026, 9, 30, 11, 59, 59, 0, time.UTC),
		ReceivedAt: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC),
		IP:         "192.0.2.1",
	}
}

func TestKafkaPublisherWritesKeyedJSON(t *testing.T) {
	fw := &fakeWriter{}
	pub := newKafkaPublisher(fw, time.Second)

	if err := pub.Publish(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(fw.msgs) != 1 {
		t.Fatalf("written %d messages, want 1", len(fw.msgs))
	}

	msg := fw.msgs[0]
	if string(msg.Key) != "site-42/visitor-7" {
		t.Errorf("key = %q, want site-42/visitor-7", msg.Key)
	}

	var got event.Event
	if err := json.Unmarshal(msg.Value, &got); err != nil {
		t.Fatalf("value is not valid JSON: %v", err)
	}
	if got.ID != "evt-1" || got.SiteID != "site-42" || got.IP != "192.0.2.1" {
		t.Errorf("decoded event = %+v", got)
	}
	if !got.Timestamp.Equal(sampleEvent().Timestamp) || !got.ReceivedAt.Equal(sampleEvent().ReceivedAt) {
		t.Errorf("timestamps not preserved: %+v", got)
	}
}

func TestKafkaPublisherWrapsWriteError(t *testing.T) {
	boom := errors.New("broker down")
	pub := newKafkaPublisher(&fakeWriter{err: boom}, time.Second)

	err := pub.Publish(context.Background(), sampleEvent())
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
}

func TestKafkaPublisherTimesOut(t *testing.T) {
	pub := newKafkaPublisher(&fakeWriter{block: true}, 50*time.Millisecond)

	start := time.Now()
	err := pub.Publish(context.Background(), sampleEvent())

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Publish took %v, the timeout was not applied", elapsed)
	}
}

func TestKafkaPublisherClose(t *testing.T) {
	fw := &fakeWriter{}
	if err := newKafkaPublisher(fw, time.Second).Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !fw.closed {
		t.Error("the underlying writer was not closed")
	}
}

func TestPartitionKey(t *testing.T) {
	base := sampleEvent()

	sameVisitor := base
	sameVisitor.ID = "evt-2"
	sameVisitor.URL = "https://example.com/panier"

	otherVisitor := base
	otherVisitor.VisitorID = "visitor-8"

	otherSite := base
	otherSite.SiteID = "site-43"

	if string(partitionKey(base)) != string(partitionKey(sameVisitor)) {
		t.Error("le même visiteur doit toujours produire la même clé, quel que soit l'événement")
	}
	if string(partitionKey(base)) == string(partitionKey(otherVisitor)) {
		t.Error("deux visiteurs différents doivent avoir des clés différentes")
	}
	if string(partitionKey(base)) == string(partitionKey(otherSite)) {
		t.Error("le même visiteur sur deux sites différents doit avoir des clés différentes")
	}
}
