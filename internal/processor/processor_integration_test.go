//go:build integration

// Test d'intégration du processor : nécessite l'infrastructure locale (make up).
// Lancement : make test-integration

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/batch"
	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
	"github.com/behramkorkut/pulse-stream/internal/sessions"
)

func integrationBrokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:19092"}
}

// readN lit exactement n messages d'un topic avec un groupe de lecture neuf.
func readN(t *testing.T, ctx context.Context, brokers []string, topic string, n int) [][]byte {
	t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     topic + "-test-reader",
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	var values [][]byte
	for len(values) < n {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("lecture de %s (%d/%d messages) : %v", topic, len(values), n, err)
		}
		values = append(values, msg.Value)
	}
	return values
}

// TestProcessorIntegration publie 4 messages bruts (1 humain, 1 robot, 1 JSON cassé, 1 événement
// incomplet) et vérifie que 2 arrivent enrichis et 2 en dead-letter, puis que l'arrêt est propre.
func TestProcessorIntegration(t *testing.T) {
	brokers := integrationBrokers()
	suffix := fmt.Sprint(time.Now().UnixNano())
	rawTopic, enrichedTopic, deadTopic := "it-raw-"+suffix, "it-enriched-"+suffix, "it-dead-"+suffix

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	for _, name := range []string{rawTopic, enrichedTopic, deadTopic} {
		if err := kafkautil.CreateTopic(ctx, brokers, name, 3); err != nil {
			t.Fatal(err)
		}
		name := name
		t.Cleanup(func() { _ = kafkautil.DeleteTopic(context.Background(), brokers, name) })
	}

	producer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        rawTopic,
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	t.Cleanup(func() { _ = producer.Close() })

	now := time.Now().UTC().Add(-time.Second).Format(time.RFC3339)
	raw := func(id, ua string) []byte {
		return []byte(fmt.Sprintf(`{"id":%q,"type":"pageview","site_id":"site-42","visitor_id":"v-1",`+
			`"url":"https://example.com/","user_agent":%q,"timestamp":%q}`, id, ua, now))
	}
	err := producer.WriteMessages(ctx,
		kafka.Message{Key: []byte("site-42/v-1"), Value: raw("evt-human", chromeUA)},
		kafka.Message{Key: []byte("site-42/v-2"), Value: raw("evt-bot", "Googlebot/2.1")},
		kafka.Message{Key: []byte("site-42/v-3"), Value: []byte(`{oops`)},
		kafka.Message{Key: []byte("site-42/v-4"), Value: []byte(`{"id":"evt-incomplete"}`)},
	)
	if err != nil {
		t.Fatalf("publication des messages bruts : %v", err)
	}

	reader := batch.NewReader(brokers, "it-group-"+suffix, rawTopic)
	writer := NewWriter(brokers)
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })

	// Redis : base numéro 15, réservée aux tests (elle est vidée).
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Redis injoignable sur %s (make up ?) : %v", redisAddr, err)
	}

	runner := NewRunner(reader, writer,
		Config{EnrichedTopic: enrichedTopic, DeadLetterTopic: deadTopic, Sessions: sessions.NewRedis(rdb, sessions.DefaultTimeout)},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	runCtx, stopRunner := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()

	// Enrichis : un humain Chrome, un robot.
	byID := map[string]event.Enriched{}
	for _, v := range readN(t, ctx, brokers, enrichedTopic, 2) {
		var e event.Enriched
		if err := json.Unmarshal(v, &e); err != nil {
			t.Fatalf("événement enrichi illisible : %v", err)
		}
		byID[e.ID] = e
	}
	if h := byID["evt-human"]; h.IsBot || h.Browser != "chrome" {
		t.Errorf("humain mal enrichi : %+v", h)
	}
	if h := byID["evt-human"]; h.SessionID == "" || !h.NewSession {
		t.Errorf("l'humain doit ouvrir une session : id=%q new=%v", h.SessionID, h.NewSession)
	}
	if b := byID["evt-bot"]; !b.IsBot || b.Device != DeviceBot {
		t.Errorf("robot mal enrichi : %+v", b)
	}
	if b := byID["evt-bot"]; b.SessionID != "" {
		t.Errorf("un robot ne doit pas avoir de session, got %q", b.SessionID)
	}

	// Rejetés : JSON cassé et événement incomplet.
	reasons := map[string]bool{}
	for _, v := range readN(t, ctx, brokers, deadTopic, 2) {
		var dl DeadLetter
		if err := json.Unmarshal(v, &dl); err != nil {
			t.Fatalf("dead-letter illisible : %v", err)
		}
		reasons[dl.Reason] = true
		if dl.Source.Topic != rawTopic {
			t.Errorf("source.topic = %q, want %q", dl.Source.Topic, rawTopic)
		}
	}
	if !reasons[ReasonInvalidJSON] || !reasons[ReasonInvalidEvent] {
		t.Errorf("raisons de rejet = %v, want invalid_json et invalid_event", reasons)
	}

	// Arrêt propre.
	stopRunner()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v après un arrêt demandé, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("le processor ne s'est pas arrêté")
	}
}
