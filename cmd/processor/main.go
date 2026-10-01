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

	"github.com/redis/go-redis/v9"

	"github.com/behramkorkut/pulse-stream/internal/batch"
	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
	"github.com/behramkorkut/pulse-stream/internal/metrics"
	"github.com/behramkorkut/pulse-stream/internal/processor"
	"github.com/behramkorkut/pulse-stream/internal/sessions"
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

	sessionTimeoutMin, err := envInt("SESSION_TIMEOUT_MIN", 30)
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

	store, closeStore, err := newSessionStore(ctx, log, time.Duration(sessionTimeoutMin)*time.Minute)
	if err != nil {
		return err
	}
	defer closeStore()

	reader := batch.NewReader(brokers, groupID, rawTopic)
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

	reg := metrics.NewRegistry()
	batch.RegisterLag(reg, "processor", func() int64 { return reader.Stats().Lag })
	stopMetrics, err := metrics.Start(getenv("METRICS_ADDR", ":9102"), reg, log)
	if err != nil {
		return err
	}
	defer stopMetrics()

	runner := processor.NewRunner(reader, writer, processor.Config{
		Metrics:         processor.NewMetrics(reg),
		BatchMetrics:    batch.NewMetrics(reg, "processor"),
		EnrichedTopic:   enrichedTopic,
		DeadLetterTopic: deadTopic,
		Sessions:        store,
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

// newSessionStore choisit le magasin de sessions selon SESSIONS (redis par défaut, ou off pour
// travailler sans Redis). Il retourne aussi la fonction de fermeture à appeler à l'arrêt.
func newSessionStore(ctx context.Context, log *slog.Logger, timeout time.Duration) (sessions.Store, func(), error) {
	switch mode := getenv("SESSIONS", "redis"); mode {
	case "off":
		log.Warn("SESSIONS=off : les événements n'auront pas de session")
		return nil, func() {}, nil

	case "redis":
		addr := getenv("REDIS_ADDR", "localhost:6379")
		client := redis.NewClient(&redis.Options{Addr: addr})

		// Échec rapide : mieux vaut refuser de démarrer que découvrir Redis absent au premier événement.
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := client.Ping(pingCtx).Err(); err != nil {
			_ = client.Close()
			return nil, nil, fmt.Errorf("redis not ready at %s: %w", addr, err)
		}

		log.Info("sessions enabled", slog.String("redis", addr), slog.String("timeout", timeout.String()))
		return sessions.NewRedis(client, timeout), func() { _ = client.Close() }, nil

	default:
		return nil, nil, fmt.Errorf("unknown SESSIONS %q (expected redis or off)", mode)
	}
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
