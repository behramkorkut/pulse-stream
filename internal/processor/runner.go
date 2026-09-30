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

	"github.com/behramkorkut/pulse-stream/internal/sessions"
)

// source est la partie de *kafka.Reader dont le runner a besoin.
type source interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

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

// batchDeadline borne le temps accordé au traitement d'un lot, y compris à l'arrêt.
const batchDeadline = 15 * time.Second

// Runner lit les messages bruts par lots, les transforme en parallèle, publie les résultats,
// puis valide (commit) les offsets. L'ordre est essentiel : voir processBatch.
type Runner struct {
	src source
	dst sink
	cfg Config
	log *slog.Logger
	now func() time.Time // injectable pour les tests
}

// NewRunner crée un Runner.
func NewRunner(src source, dst sink, cfg Config, log *slog.Logger) *Runner {
	return newRunner(src, dst, cfg, log, time.Now)
}

func newRunner(src source, dst sink, cfg Config, log *slog.Logger, now func() time.Time) *Runner {
	return &Runner{src: src, dst: dst, cfg: cfg.withDefaults(), log: log, now: now}
}

// Run traite les messages jusqu'à l'annulation de ctx (arrêt propre, retourne nil)
// ou jusqu'à une erreur irrécupérable (retourne l'erreur).
func (r *Runner) Run(ctx context.Context) error {
	// Ce contexte dérivé garantit que la goroutine de lecture s'arrête quand Run se termine,
	// même sur erreur : sinon elle resterait bloquée indéfiniment (fuite de goroutine).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgs := make(chan kafka.Message, r.cfg.BatchSize)
	fetchErr := make(chan error, 1)

	// Étage 1 : une goroutine lit Kafka en continu et alimente le canal. Pendant que le lot courant
	// est traité, la lecture du suivant avance déjà (pipeline).
	go func() {
		defer close(msgs)
		for {
			m, err := r.src.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() == nil { // erreur réelle, pas un arrêt demandé
					fetchErr <- fmt.Errorf("fetch: %w", err)
				}
				return
			}
			select {
			case msgs <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Étage 2 : la boucle principale assemble des lots et les traite.
	for {
		batch, more := collect(msgs, r.cfg.BatchSize, r.cfg.BatchWait)
		if len(batch) > 0 {
			if err := r.processBatch(ctx, batch); err != nil {
				return err
			}
		}
		if !more {
			break
		}
	}

	// Le canal est fermé : soit arrêt demandé (nil), soit la lecture a échoué (l'erreur est déjà
	// dans fetchErr, écrite avant la fermeture du canal).
	select {
	case err := <-fetchErr:
		return err
	default:
		return nil
	}
}

// collect assemble un lot : attend le premier message (sans limite de temps), puis complète
// jusqu'à max messages ou jusqu'à l'expiration de wait, selon ce qui arrive en premier.
// more vaut false quand le canal est fermé : le lot retourné est alors le dernier.
func collect(ch <-chan kafka.Message, max int, wait time.Duration) (batch []kafka.Message, more bool) {
	first, ok := <-ch
	if !ok {
		return nil, false
	}
	batch = append(batch, first)

	timer := time.NewTimer(wait)
	defer timer.Stop()

	for len(batch) < max {
		select {
		case m, ok := <-ch:
			if !ok {
				return batch, false
			}
			batch = append(batch, m)
		case <-timer.C:
			return batch, true
		}
	}
	return batch, true
}

// processBatch est le cœur de la garantie "au moins une fois" :
//
//  1. transformer le lot (validation, enrichissement, sessions) ;
//  2. écrire TOUS les résultats dans Kafka (avec nouvelles tentatives) ;
//  3. SEULEMENT ALORS valider les offsets d'entrée.
//
// Si le processus meurt entre 2 et 3, le lot sera relu et retraité : des doublons possibles,
// mais aucune perte. Valider avant d'écrire ferait l'inverse : des pertes possibles.
//
// Retenter la transformation est sûr parce que le rattachement à une session est idempotent.
func (r *Runner) processBatch(ctx context.Context, batch []kafka.Message) error {
	// Le lot en cours doit aller au bout même si un arrêt a été demandé : on détache son contexte
	// de l'annulation, en lui imposant tout de même une échéance.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchDeadline)
	defer cancel()

	start := time.Now()

	var outs []Output
	err := r.retry(ctx, "transform batch", func() (err error) {
		outs, err = r.transformBatch(ctx, batch)
		return err
	})
	if err != nil {
		return fmt.Errorf("transform batch: %w", err)
	}

	msgs := make([]kafka.Message, 0, len(batch))
	dead := 0
	for i, out := range outs {
		topic := r.cfg.EnrichedTopic
		if out.Dead {
			topic = r.cfg.DeadLetterTopic
			dead++
		}
		msgs = append(msgs, kafka.Message{Topic: topic, Key: batch[i].Key, Value: out.Value})
	}

	err = r.retry(ctx, "write batch", func() error { return r.dst.WriteMessages(ctx, msgs...) })
	if err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	if err := r.src.CommitMessages(ctx, batch...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	r.log.Info("batch processed",
		slog.Int("messages", len(batch)),
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

// retry exécute fn jusqu'à MaxAttempts fois, avec un délai croissant entre les essais.
// Un échec partiel suivi d'un nouvel essai peut dupliquer des messages : acceptable en
// "au moins une fois", à condition que les consommateurs en aval dédoublonnent grâce à l'identifiant
// de l'événement.
func (r *Runner) retry(ctx context.Context, what string, fn func() error) error {
	backoff := r.cfg.RetryBackoff

	var err error
	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == r.cfg.MaxAttempts {
			break
		}
		r.log.Warn("attempt failed, retrying",
			slog.String("step", what), slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff), slog.Any("error", err))

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %v)", ctx.Err(), err)
		}
	}
	return fmt.Errorf("after %d attempts: %w", r.cfg.MaxAttempts, err)
}
