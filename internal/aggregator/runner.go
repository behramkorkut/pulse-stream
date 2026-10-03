package aggregator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/batch"
	"github.com/behramkorkut/pulse-stream/internal/dedupe"
	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Config règle le comportement du runner.
type Config struct {
	BatchSize    int           // défaut : 200
	BatchWait    time.Duration // défaut : 50 ms
	MaxAttempts  int           // essais avant d'abandonner (défaut : 5)
	RetryBackoff time.Duration // pause avant le 2e essai, doublée à chaque essai (défaut : 200 ms)

	Metrics      *Metrics       // optionnel : mesures de l'aggregator
	BatchMetrics *batch.Metrics // optionnel : mesures de la boucle de consommation
}

// Runner lit les événements enrichis par lots, écarte les doublons et cumule les compteurs.
type Runner struct {
	src   batch.Source
	store Store
	seen  dedupe.Store
	cfg   Config
	log   *slog.Logger
}

// NewRunner crée un Runner.
func NewRunner(src batch.Source, store Store, seen dedupe.Store, cfg Config, log *slog.Logger) *Runner {
	return &Runner{src: src, store: store, seen: seen, cfg: cfg, log: log}
}

// Run traite les messages jusqu'à l'annulation de ctx (arrêt propre, retourne nil)
// ou jusqu'à une erreur irrécupérable (retourne l'erreur).
func (r *Runner) Run(ctx context.Context) error {
	return batch.Run(ctx, r.src, batch.Config{Size: r.cfg.BatchSize, Wait: r.cfg.BatchWait, Metrics: r.cfg.BatchMetrics}, r.handle)
}

// handle traite un lot dans cet ordre précis :
//
//  1. décoder, en écartant les messages inexploitables et les doublons du lot ;
//  2. interroger la mémoire des identifiants déjà comptés (Seen) et écarter ceux-là ;
//  3. cumuler les compteurs dans MongoDB (Apply) ;
//  4. SEULEMENT ALORS mémoriser les identifiants comptés (Mark) ;
//  5. puis, dans la boucle, valider les offsets.
//
// Un crash entre 3 et 4 fait recompter ce lot (doublon) ; un crash entre 4 et 5 ne fait rien recompter,
// car les identifiants sont déjà mémorisés. Inverser 3 et 4 ferait l'inverse : des événements perdus.
func (r *Runner) handle(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	retry := batch.RetryPolicy{MaxAttempts: r.cfg.MaxAttempts, Backoff: r.cfg.RetryBackoff}

	events, skipped, batchDuplicates := r.decode(msgs)

	keys := make([]dedupe.Key, len(events))
	for i, e := range events {
		keys[i] = dedupeKey(e)
	}

	var seen []bool
	if len(keys) > 0 {
		err := retry.Do(ctx, r.log, "check seen ids", func() (err error) {
			seen, err = r.seen.Seen(ctx, keys)
			return err
		})
		if err != nil {
			return fmt.Errorf("check seen ids: %w", err)
		}
	}

	var fresh []event.Enriched
	var freshKeys []dedupe.Key
	for i, e := range events {
		if !seen[i] {
			fresh = append(fresh, e)
			freshKeys = append(freshKeys, keys[i])
		}
	}
	duplicates := batchDuplicates + len(events) - len(fresh)

	buckets := Aggregate(fresh)
	if len(buckets) > 0 {
		applyOnce := func() error {
			start := time.Now()
			defer func() { r.cfg.Metrics.observeApply(time.Since(start)) }()
			return r.store.Apply(ctx, buckets)
		}
		if err := retry.Do(ctx, r.log, "apply counters", applyOnce); err != nil {
			return fmt.Errorf("apply counters: %w", err)
		}
		if err := retry.Do(ctx, r.log, "mark counted ids", func() error { return r.seen.Mark(ctx, freshKeys) }); err != nil {
			return fmt.Errorf("mark counted ids: %w", err)
		}
	}

	r.cfg.Metrics.observeBatch(len(fresh), duplicates, skipped, len(buckets))

	r.log.Debug("batch aggregated",
		slog.Int("messages", len(msgs)),
		slog.Int("counted", len(fresh)),
		slog.Int("duplicates", duplicates),
		slog.Int("skipped", skipped),
		slog.Int("buckets", len(buckets)),
		slog.Float64("took_ms", float64(time.Since(start).Microseconds())/1000),
	)
	return nil
}

// decode transforme les messages en événements. Un message inexploitable est écarté et journalisé, mais
// n'arrête jamais le flux : il sera validé avec le reste du lot. Ce serait sinon un "message poison" qui
// bloquerait l'aggregator pour toujours. Les doublons à l'intérieur du lot (même site, même identifiant) sont
// écartés (le premier gagne).
func (r *Runner) decode(msgs []kafka.Message) (events []event.Enriched, skipped, duplicates int) {
	inBatch := make(map[dedupe.Key]bool, len(msgs))

	for _, m := range msgs {
		e, err := Decode(m.Value)
		if err != nil {
			skipped++
			r.log.Warn("skipping undecodable message",
				slog.String("topic", m.Topic), slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset), slog.Any("error", err))
			continue
		}
		k := dedupeKey(e)
		if inBatch[k] {
			duplicates++
			continue
		}
		inBatch[k] = true
		events = append(events, e)
	}
	return events, skipped, duplicates
}

// dedupeKey identifie un événement pour le dédoublonnage. L'identifiant vient du client et n'est unique qu'au sein
// d'un site : le site fait partie de la clé, sinon le même identifiant envoyé par deux sites ne serait compté qu'une fois.
func dedupeKey(e event.Enriched) dedupe.Key {
	return dedupe.Key{SiteID: e.SiteID, EventID: e.ID}
}
