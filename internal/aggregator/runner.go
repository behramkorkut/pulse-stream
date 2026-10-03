package aggregator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
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

// Runner lit les événements enrichis par lots, écarte les doublons et cumule les compteurs, exactement une fois.
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
//  1. décoder (un message inexploitable est écarté et journalisé, jamais bloquant), en notant la position
//     (partition, offset) de chaque événement et le dernier offset du lot dans chaque partition ;
//  2. réserver chaque événement pour le message qui le porte (dedupe.Claim) : un événement déjà réservé par un
//     AUTRE message est un doublon, écarté ;
//  3. écrire dans MongoDB, en UNE transaction, les compteurs des événements que la position de leur partition ne
//     couvre pas encore, et les nouvelles positions (Store.Apply) ;
//  4. puis, dans la boucle, valider les offsets dans Kafka.
//
// Un crash à n'importe quel moment ne fait ni perdre ni compter deux fois : avant 3, le lot relu retrouve ses propres
// réservations et compte ses événements ; après 3, le lot relu est entièrement couvert par les positions et
// n'ajoute rien. Deux instances qui traitent le même lot pendant un rééquilibrage se heurtent sur le document de
// position : MongoDB n'en laisse passer qu'une.
func (r *Runner) handle(ctx context.Context, msgs []kafka.Message) error {
	start := time.Now()
	retry := batch.RetryPolicy{MaxAttempts: r.cfg.MaxAttempts, Backoff: r.cfg.RetryBackoff}

	candidates, upTo, skipped := r.decode(msgs)

	keys := make([]dedupe.Key, len(candidates))
	owners := make([]string, len(candidates))
	for i, c := range candidates {
		keys[i] = dedupeKey(c.Event)
		owners[i] = ownerOf(c)
	}

	var claimed []string
	if len(keys) > 0 {
		err := retry.Do(ctx, r.log, "claim events", func() (err error) {
			claimed, err = r.seen.Claim(ctx, keys, owners)
			return err
		})
		if err != nil {
			return fmt.Errorf("claim events: %w", err)
		}
	}

	mine := make([]Counted, 0, len(candidates))
	for i, c := range candidates {
		if claimed[i] == owners[i] {
			mine = append(mine, c)
		}
	}
	duplicates := len(candidates) - len(mine)

	// Retenter est sûr : l'écriture est idempotente, même si un essai précédent a abouti sans qu'on le sache.
	var applied Applied
	err := retry.Do(ctx, r.log, "apply counters", func() (err error) {
		start := time.Now()
		defer func() { r.cfg.Metrics.observeApply(time.Since(start)) }()
		applied, err = r.store.Apply(ctx, mine, upTo)
		return err
	})
	if err != nil {
		return fmt.Errorf("apply counters: %w", err)
	}
	writtenAt := time.Now() // les compteurs sont visibles dans MongoDB à partir de cet instant
	replayed := len(mine) - len(applied.Events)

	r.cfg.Metrics.observeLatency(applied.Events, writtenAt)
	r.cfg.Metrics.observeBatch(len(applied.Events), duplicates, skipped, replayed, applied.Buckets)

	r.log.Debug("batch aggregated",
		slog.Int("messages", len(msgs)),
		slog.Int("counted", len(applied.Events)),
		slog.Int("duplicates", duplicates),
		slog.Int("replayed", replayed),
		slog.Int("skipped", skipped),
		slog.Int("buckets", applied.Buckets),
		slog.Float64("took_ms", float64(time.Since(start).Microseconds())/1000),
	)
	return nil
}

// decode transforme les messages en événements positionnés. Un message inexploitable est écarté et journalisé, mais
// n'arrête jamais le flux : il sera validé avec le reste du lot (ce serait sinon un "message poison" qui bloquerait
// l'aggregator pour toujours). upTo donne, pour chaque partition, l'offset du dernier message du lot.
func (r *Runner) decode(msgs []kafka.Message) (events []Counted, upTo map[Partition]int64, skipped int) {
	upTo = make(map[Partition]int64)
	for _, m := range msgs {
		p := Partition{Topic: m.Topic, ID: m.Partition}
		if last, known := upTo[p]; !known || m.Offset > last {
			upTo[p] = m.Offset
		}

		e, err := Decode(m.Value)
		if err != nil {
			skipped++
			r.log.Warn("skipping undecodable message",
				slog.String("topic", m.Topic), slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset), slog.Any("error", err))
			continue
		}
		events = append(events, Counted{Event: e, From: p, Offset: m.Offset})
	}
	return events, upTo, skipped
}

// dedupeKey identifie un événement pour le dédoublonnage. L'identifiant vient du client et n'est unique qu'au sein
// d'un site : le site fait partie de la clé, sinon le même identifiant envoyé par deux sites ne serait compté qu'une fois.
func dedupeKey(e event.Enriched) dedupe.Key {
	return dedupe.Key{SiteID: e.SiteID, EventID: e.ID}
}

// ownerOf identifie le message qui porte un événement : c'est lui qui le comptera s'il le réserve le premier.
func ownerOf(c Counted) string {
	return c.From.Topic + "/" + strconv.Itoa(c.From.ID) + ":" + strconv.FormatInt(c.Offset, 10)
}
