//go:build integration

// Test d'intégration bout en bout de l'aggregator : Kafka -> dédoublonnage Redis -> MongoDB.
// Nécessite l'infrastructure locale (make up). Lancement : make test-integration

package aggregator

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
	"github.com/behramkorkut/pulse-stream/internal/dedupe"
	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
)

func integrationBrokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:19092"}
}

// publish envoie des événements enrichis avec la MÊME clé : ils atterrissent dans la même partition,
// donc dans l'ordre où on les envoie. Le test peut ainsi raisonner sur l'ordre de traitement.
func publish(t *testing.T, ctx context.Context, w *kafka.Writer, key string, events ...event.Enriched) {
	t.Helper()

	msgs := make([]kafka.Message, len(events))
	for i, e := range events {
		value, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = kafka.Message{Key: []byte(key), Value: value}
	}
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("publication : %v", err)
	}
}

func TestAggregatorIntegration(t *testing.T) {
	brokers := integrationBrokers()
	suffix := fmt.Sprint(time.Now().UnixNano())
	topic := "it-agg-enriched-" + suffix
	site := "site-" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if err := kafkautil.CreateTopic(ctx, brokers, topic, 3); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kafkautil.DeleteTopic(context.Background(), brokers, topic) })

	producer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	t.Cleanup(func() { _ = producer.Close() })

	// Redis : base numéro 12, réservée à CE paquet de tests (elle est vidée ; chaque paquet a la sienne : les paquets de test s'exécutent en parallèle).
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 12})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Redis injoignable sur %s (make up ?) : %v", redisAddr, err)
	}

	coll := testCollection(t)
	store := NewMongo(coll)
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}

	reader := batch.NewReader(brokers, "it-agg-group-"+suffix, topic)
	t.Cleanup(func() { _ = reader.Close() })

	runner := NewRunner(reader, store, dedupe.NewRedis(rdb, dedupe.DefaultTTL),
		Config{BatchSize: 50, BatchWait: 20 * time.Millisecond},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	runCtx, stopRunner := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()

	key := site + "/v-1"
	minuteID := site + "|2026-09-30T11:00Z"

	// Lot 1 : un affichage (livré deux fois), un robot, puis un clic "témoin".
	// Même clé = même partition = traitement dans cet ordre : quand le clic est compté,
	// tout ce qui le précède l'est aussi, et le test peut alors vérifier l'absence de doublon.
	view := human("evt-view", site, event.TypePageview, "mobile", "safari", 5*time.Second, true)
	publish(t, ctx, producer, key,
		view, view,
		bot("evt-bot", site, 6*time.Second),
		human("evt-click-1", site, event.TypeClick, "desktop", "chrome", 7*time.Second, false),
	)
	waitForDoc(t, coll, minuteID, func(d minuteDoc) bool { return d.Clicks == 1 })

	doc := readDoc(t, coll, minuteID)
	if doc.Pageviews != 1 || doc.BotEvents != 1 || doc.Sessions != 1 {
		t.Errorf("après le lot 1 : pv %d bots %d sessions %d, want 1 1 1 (le doublon est compté une fois)",
			doc.Pageviews, doc.BotEvents, doc.Sessions)
	}
	if doc.Devices["mobile"] != 1 || doc.Devices["desktop"] != 1 || doc.Browsers["safari"] != 1 || doc.Browsers["chrome"] != 1 {
		t.Errorf("répartitions = %v / %v", doc.Devices, doc.Browsers)
	}

	// Lot 2 : le même affichage revient (rejeu, autre lot), suivi d'un second clic témoin.
	// Le doublon est cette fois reconnu grâce à Redis, pas grâce à l'intérieur du lot.
	publish(t, ctx, producer, key,
		view,
		human("evt-click-2", site, event.TypeClick, "desktop", "chrome", 8*time.Second, false),
	)
	waitForDoc(t, coll, minuteID, func(d minuteDoc) bool { return d.Clicks == 2 })

	doc = readDoc(t, coll, minuteID)
	if doc.Pageviews != 1 {
		t.Errorf("pageviews = %d, want 1 : l'événement rejoué a été compté une seconde fois", doc.Pageviews)
	}

	stopRunner()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v après un arrêt demandé, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("l'aggregator ne s'est pas arrêté")
	}
}
