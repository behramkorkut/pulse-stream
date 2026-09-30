// Package kafkautil regroupe de petites opérations d'administration Kafka partagées
// par les différents programmes et tests de pulse-stream.
package kafkautil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// CheckTopic échoue vite (au démarrage) si le broker est injoignable ou si le topic n'existe pas.
// Mieux vaut un démarrage refusé avec un message clair que des erreurs mystérieuses en production.
func CheckTopic(ctx context.Context, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka broker configured")
	}

	conn, err := kafka.DefaultDialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("connect to kafka broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return fmt.Errorf("read partitions of topic %q (does it exist? run: make topics): %w", topic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("topic %q has no partitions (run: make topics)", topic)
	}
	return nil
}

// CreateTopic crée un topic puis attend qu'il soit visible dans les métadonnées du broker.
func CreateTopic(ctx context.Context, brokers []string, name string, partitions int) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: name, NumPartitions: partitions, ReplicationFactor: 1}},
	})
	if err != nil {
		return fmt.Errorf("create topic %q: %w", name, err)
	}
	if terr := resp.Errors[name]; terr != nil {
		return fmt.Errorf("create topic %q: %w", name, terr)
	}

	// Le topic peut mettre un instant à apparaître dans les métadonnées.
	var lastErr error
	for i := 0; i < 40; i++ {
		if lastErr = CheckTopic(ctx, brokers, name); lastErr == nil {
			return nil
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("topic %q never became visible: %w", name, lastErr)
}

// DeleteTopic supprime un topic.
func DeleteTopic(ctx context.Context, brokers []string, name string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	if _, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{name}}); err != nil {
		return fmt.Errorf("delete topic %q: %w", name, err)
	}
	return nil
}
