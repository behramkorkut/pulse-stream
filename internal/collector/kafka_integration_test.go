//go:build integration

// Test d'intégration : nécessite l'infrastructure locale (make up).
// Lancement : make test-integration

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
)

func integrationBrokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:19092"}
}

// TestKafkaPublisherIntegration publie des événements dans un vrai topic (créé pour l'occasion,
// supprimé à la fin), les relit et vérifie que tous les événements d'un même visiteur
// se trouvent sur la même partition.
func TestKafkaPublisherIntegration(t *testing.T) {
	brokers := integrationBrokers()
	topic := fmt.Sprintf("it-raw-events-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 6, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if terr := resp.Errors[topic]; terr != nil {
		t.Fatalf("create topic: %v", terr)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{Topics: []string{topic}})
	})

	// Le topic peut mettre un instant à être visible dans les métadonnées du broker.
	var checkErr error
	for i := 0; i < 20; i++ {
		if checkErr = kafkautil.CheckTopic(ctx, brokers, topic); checkErr == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if checkErr != nil {
		t.Fatalf("topic never became visible: %v", checkErr)
	}

	pub := NewKafkaPublisher(brokers, topic)
	t.Cleanup(func() { _ = pub.Close() })

	// 5 événements du visiteur A, 5 du visiteur B, sur le même site.
	sent := map[string]bool{}
	for i := 0; i < 5; i++ {
		for _, visitor := range []string{"visitor-A", "visitor-B"} {
			e := event.Event{
				ID:         fmt.Sprintf("%s-%d", visitor, i),
				Type:       event.TypePageview,
				SiteID:     "site-42",
				VisitorID:  visitor,
				URL:        "https://example.com/",
				Timestamp:  time.Now().UTC(),
				ReceivedAt: time.Now().UTC(),
			}
			if err := pub.Publish(ctx, e); err != nil {
				t.Fatalf("publish %s: %v", e.ID, err)
			}
			sent[e.ID] = true
		}
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     topic + "-reader",
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
	t.Cleanup(func() { _ = reader.Close() })

	partitionsByKey := map[string]map[int]bool{}
	for range sent {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read message: %v", err)
		}

		var e event.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			t.Fatalf("invalid JSON in topic: %v", err)
		}
		if !sent[e.ID] {
			t.Errorf("unexpected or duplicate event %q", e.ID)
		}
		delete(sent, e.ID)

		key := string(msg.Key)
		if partitionsByKey[key] == nil {
			partitionsByKey[key] = map[int]bool{}
		}
		partitionsByKey[key][msg.Partition] = true
	}

	for key, partitions := range partitionsByKey {
		if len(partitions) != 1 {
			t.Errorf("clé %q répartie sur %d partitions, want 1", key, len(partitions))
		}
	}
	if len(partitionsByKey) != 2 {
		t.Errorf("%d clés distinctes lues, want 2", len(partitionsByKey))
	}
}
