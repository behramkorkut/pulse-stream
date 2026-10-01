// Commande aggregator : consomme les événements enrichis, écarte les doublons et cumule
// des compteurs par site et par minute dans MongoDB.
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
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/behramkorkut/pulse-stream/internal/aggregator"
	"github.com/behramkorkut/pulse-stream/internal/batch"
	"github.com/behramkorkut/pulse-stream/internal/dedupe"
	"github.com/behramkorkut/pulse-stream/internal/kafkautil"
	"github.com/behramkorkut/pulse-stream/internal/logging"
	"github.com/behramkorkut/pulse-stream/internal/metrics"
	"github.com/behramkorkut/pulse-stream/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("aggregator stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	log, err := logging.New(os.Stdout, os.Getenv("LOG_LEVEL"))
	if err != nil {
		return err
	}

	brokers := splitCSV(getenv("KAFKA_BROKERS", "localhost:19092"))
	topic := getenv("KAFKA_TOPIC_ENRICHED", "enriched-events")
	groupID := getenv("GROUP_ID", "pulse-aggregator")

	mongoURI := getenv("MONGO_URI", "mongodb://localhost:27017")
	mongoDB := getenv("MONGO_DB", "pulse")
	mongoColl := getenv("MONGO_COLLECTION", "minute_stats")
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")

	batchSize, err := envInt("BATCH_SIZE", 200)
	if err != nil {
		return err
	}
	batchWaitMs, err := envInt("BATCH_WAIT_MS", 50)
	if err != nil {
		return err
	}
	dedupeTTLMin, err := envInt("DEDUPE_TTL_MIN", 60)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Échec rapide : chaque dépendance est vérifiée avant de lire le moindre message.
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := kafkautil.CheckTopic(checkCtx, brokers, topic); err != nil {
		return fmt.Errorf("kafka not ready: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(checkCtx).Err(); err != nil {
		return fmt.Errorf("redis not ready at %s: %w", redisAddr, err)
	}

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(mongoURI).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			log.Error("closing mongo", slog.Any("error", err))
		}
	}()
	if err := mongoClient.Ping(checkCtx, readpref.Primary()); err != nil {
		return fmt.Errorf("mongo not ready at %s: %w", mongoURI, err)
	}

	store := aggregator.NewMongo(mongoClient.Database(mongoDB).Collection(mongoColl))
	if err := store.EnsureIndexes(checkCtx); err != nil {
		return err
	}

	reader := batch.NewReader(brokers, groupID, topic)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error("closing reader", slog.Any("error", err))
		}
	}()

	reg := metrics.NewRegistry()
	// Retard du groupe entier (somme des partitions), mesuré en arrière-plan : Reader.Stats().Lag ne voit
	// qu'une partition à la fois, et sous-estime donc le retard réel d'un facteur proche du nombre de partitions.
	lag := &kafkautil.LagTracker{}
	go lag.Run(ctx, kafkautil.NewGroupSource(brokers, topic, groupID), 5*time.Second, log)
	batch.RegisterLag(reg, "aggregator", lag.Value)
	stopMetrics, err := metrics.Start(getenv("METRICS_ADDR", ":9103"), reg, log)
	if err != nil {
		return err
	}
	defer stopMetrics()

	runner := aggregator.NewRunner(reader, store,
		dedupe.NewRedis(rdb, time.Duration(dedupeTTLMin)*time.Minute),
		aggregator.Config{
			BatchSize:    batchSize,
			BatchWait:    time.Duration(batchWaitMs) * time.Millisecond,
			Metrics:      aggregator.NewMetrics(reg),
			BatchMetrics: batch.NewMetrics(reg, "aggregator"),
		}, log)

	log.Info("aggregator started",
		slog.String("version", version.Version),
		slog.String("group", groupID),
		slog.String("from", topic),
		slog.String("mongo", mongoDB+"."+mongoColl),
		slog.String("dedupe_ttl", (time.Duration(dedupeTTLMin)*time.Minute).String()),
	)

	if err := runner.Run(ctx); err != nil {
		return err
	}
	log.Info("aggregator stopped cleanly")
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
