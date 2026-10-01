package processor

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/batch"
	"github.com/behramkorkut/pulse-stream/internal/sessions"
)

// sink est la partie de *kafka.Writer dont le runner a besoin.
type sink interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Config règle le comportement du runner.
type Config struct {
	EnrichedTopic   string
	DeadLetterTopic string

	// Sessions rattache les événements des humains à leur session. nil : pas de sessions.
	Sessions sessions.Store

	Workers      int           // goroutines de transformation (défaut : nombre de cœurs)
	BatchSize    int           // taille maximale d'un lot (défaut : 200)
	BatchWait    time.Duration // attente maximale pour remplir un lot (défaut : 50 ms)
	MaxAttempts  int           // essais (transformation, écriture) avant d'abandonner (défaut : 5)
	RetryBackoff time.Duration // pause avant le 2e essai, doublée à chaque essai (défaut : 200 ms)
}

func (c Config) withDefaults() Config {
	if c.Workers <= 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.BatchWait <= 0 {
		c.BatchWait = 50 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	return c
}

// Runner transforme par lots les messages bruts (en parallèle) et publie les résultats.
type Runner struct {
	src batch.Source
	dst sink
	cfg Config
	log *slog.Logger
	now func() time.Time // injectable pour les tests
}

// NewRunner crée un Runner.
func NewRunner(src batch.Source, dst sink, cfg Config, log *slog.Logger) *Runner {
	return newRunner(src, dst, cfg, log, time.Now)
}

func newRunner(src batch.Source, dst sink, cfg Config, log *slog.Logger, now func() time.Time) *Runner {
	return &Runner{src: src, dst: dst, cfg: cfg.withDefaults(), log: log, now: now}
}

// Run traite les messages jusqu'à l'annulation de ctx (arrêt propre, retourne nil)
// ou jusqu'à une erreur irrécupérable (retourne l'erreur). La boucle de lecture, l'assemblage des
// lots et la validation des offsets sont dans le paquet batch ; ici, seulement le métier.
func (r *Runner) Run(ctx context.Context) error {
	return batch.Run(ctx, r.src, batch.Config{Size: r.cfg.BatchSize, Wait: r.cfg.BatchWait}, r.handle)
}

// handle traite un lot. Il ne retourne nil que si TOUS les résultats sont écrits dans Kafka : la
// boucle valide alors les offsets d'entrée. Écrire d'abord, valider ensuite : en cas de crash entre
// les deux, le lot est relu (doublons possibles, mais aucune perte). C'est le "au moins une fois".
//
// Retenter la transformation est sûr parce que le rattachement à une session est idempotent.
func (r *Runner) handle(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	retry := batch.RetryPolicy{MaxAttempts: r.cfg.MaxAttempts, Backoff: r.cfg.RetryBackoff}

	var outs []Output
	err := retry.Do(ctx, r.log, "transform batch", func() (err error) {
		outs, err = r.transformBatch(ctx, msgs)
		return err
	})
	if err != nil {
		return fmt.Errorf("transform batch: %w", err)
	}

	out := make([]kafka.Message, 0, len(msgs))
	dead := 0
	for i, o := range outs {
		topic := r.cfg.EnrichedTopic
		if o.Dead {
			topic = r.cfg.DeadLetterTopic
			dead++
		}
		out = append(out, kafka.Message{Topic: topic, Key: msgs[i].Key, Value: o.Value})
	}

	err = retry.Do(ctx, r.log, "write batch", func() error { return r.dst.WriteMessages(ctx, out...) })
	if err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	r.log.Info("batch processed",
		slog.Int("messages", len(msgs)),
		slog.Int("dead_letter", dead),
		slog.Float64("took_ms", float64(time.Since(start).Microseconds())/1000),
	)
	return nil
}

// transformBatch transforme le lot en parallèle tout en préservant l'ordre par clé.
//
// Chaque message est affecté à un "shard" selon le hachage de sa clé : tous les messages d'un même
// visiteur passent par la même goroutine, dans l'ordre. Les visiteurs différents avancent en parallèle.
// C'est indispensable aux sessions : deux événements du même visiteur traités simultanément,
// ou dans le désordre, fausseraient la détection des nouvelles sessions.
func (r *Runner) transformBatch(ctx context.Context, batch []kafka.Message) ([]Output, error) {
	now := r.now()
	outs := make([]Output, len(batch))

	shards := make([][]int, r.cfg.Workers)
	for i, m := range batch {
		s := shardOf(m.Key, r.cfg.Workers)
		shards[s] = append(shards[s], i)
	}

	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	for shard, indexes := range shards {
		if len(indexes) == 0 {
			continue
		}
		wg.Add(1)
		go func(shard int, indexes []int) {
			defer wg.Done()
			// Chaque goroutine n'écrit que dans ses propres cases (outs[i], errs[shard]) : pas de data race.
			for _, i := range indexes {
				m := batch[i]
				src := Source{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset}

				res := Transform(m.Value, src, now)
				if err := r.attachSession(ctx, res); err != nil {
					errs[shard] = err
					return
				}
				outs[i] = res.encode(m.Value, src, now)
			}
		}(shard, indexes)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return outs, nil
}

// attachSession rattache un événement valide à sa session. Les robots n'en ont pas : ils
// gonfleraient inutilement Redis et fausseraient les statistiques de sessions.
func (r *Runner) attachSession(ctx context.Context, res Result) error {
	if r.cfg.Sessions == nil || res.Event == nil || res.Event.IsBot {
		return nil
	}
	s, err := r.cfg.Sessions.Touch(ctx, res.Event.Event)
	if err != nil {
		return fmt.Errorf("session for event %s: %w", res.Event.ID, err)
	}
	res.Event.SessionID = s.ID
	res.Event.NewSession = s.New
	return nil
}

// shardOf associe une clé à un numéro de shard, de façon stable.
func shardOf(key []byte, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write(key) // l'écriture dans un hash ne retourne jamais d'erreur
	return int(h.Sum32() % uint32(shards))
}
