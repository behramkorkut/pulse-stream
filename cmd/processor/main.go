// Commande processor : consomme les événements bruts, les valide, les enrichit et les republie.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
	"github.com/behramkorkut/pulse-stream/internal/processor"
	"github.com/behramkorkut/pulse-stream/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("processor stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	brokers := splitCSV(getenv("KAFKA_BROKERS", "localhost:19092"))
	rawTopic := getenv("KAFKA_TOPIC_RAW", "raw-events")
	enrichedTopic := getenv("KAFKA_TOPIC_ENRICHED", "enriched-events")
	deadTopic := getenv("KAFKA_TOPIC_DLQ", "dead-letter")
	groupID := getenv("GROUP_ID", "pulse-processor")

	workers, err := envInt("WORKERS", 0) // 0 = nombre de cœurs
	if err != nil {
		return err
	}
	batchSize, err := envInt("BATCH_SIZE", 200)
	if err != nil {
		return err
	}
	batchWaitMs, err := envInt("BATCH_WAIT_MS", 50)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Échec rapide : on vérifie les trois topics avant de commencer.
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, topic := range []string{rawTopic, enrichedTopic, deadTopic} {
		if err := kafkautil.CheckTopic(checkCtx, brokers, topic); err != nil {
			return fmt.Errorf("kafka not ready: %w", err)
		}
	}

	reader := processor.NewReader(brokers, groupID, rawTopic)
	writer := processor.NewWriter(brokers)
	// Fermer le reader fait quitter le groupe immédiatement : les autres instances récupèrent
	// ses partitions tout de suite, sans attendre l'expiration de sa session.
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error("closing reader", slog.Any("error", err))
		}
		if err := writer.Close(); err != nil {
			log.Error("closing writer", slog.Any("error", err))
		}
	}()

	runner := processor.NewRunner(reader, writer, processor.Config{
		EnrichedTopic:   enrichedTopic,
		DeadLetterTopic: deadTopic,
		Workers:         workers,
		BatchSize:       batchSize,
		BatchWait:       time.Duration(batchWaitMs) * time.Millisecond,
	}, log)

	log.Info("processor started",
		slog.String("version", version.Version),
		slog.String("group", groupID),
		slog.String("from", rawTopic),
		slog.String("to", enrichedTopic),
		slog.String("dead_letter", deadTopic),
	)

	if err := runner.Run(ctx); err != nil {
		return err
	}
	log.Info("processor stopped cleanly")
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
