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

	Workers          int           // goroutines de transformation (défaut : nombre de cœurs)
	BatchSize        int           // taille maximale d'un lot (défaut : 200)
	BatchWait        time.Duration // attente maximale pour remplir un lot (défaut : 50 ms)
	MaxWriteAttempts int           // essais d'écriture avant d'abandonner (défaut : 5)
	RetryBackoff     time.Duration // pause avant le 2e essai, doublée à chaque essai (défaut : 200 ms)
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
	if c.MaxWriteAttempts <= 0 {
		c.MaxWriteAttempts = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	return c
}

// batchDeadline borne le temps accordé à l'écriture et au commit d'un lot, y compris à l'arrêt.
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
//  1. transformer le lot ;
//  2. écrire TOUS les résultats dans Kafka (avec nouvelles tentatives) ;
//  3. SEULEMENT ALORS valider les offsets d'entrée.
//
// Si le processus meurt entre 2 et 3, le lot sera relu et retraité : des doublons possibles,
// mais aucune perte. Valider avant d'écrire ferait l'inverse : des pertes possibles.
func (r *Runner) processBatch(ctx context.Context, batch []kafka.Message) error {
	// Le lot en cours doit aller au bout même si un arrêt a été demandé : on détache son contexte
	// de l'annulation, en lui imposant tout de même une échéance.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchDeadline)
	defer cancel()

	start := time.Now()
	outs := r.transformBatch(batch)

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

	if err := r.writeWithRetry(ctx, msgs); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	if err := r.src.CommitMessages(ctx, batch...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	r.log.Info("batch processed",
		slog.Int("messages", len(batch)),
		slog.Int("dead_letter", dead),
		slog.Duration("took", time.Since(start)),
	)
	return nil
}

// transformBatch transforme le lot en parallèle tout en préservant l'ordre par clé.
//
// Chaque message est affecté à un "shard" selon le hachage de sa clé : tous les messages d'un même
// visiteur passent par la même goroutine, dans l'ordre. Les visiteurs différents avancent en parallèle.
// Sans cela, deux événements du même visiteur pourraient être traités simultanément dans le
// désordre, ce qui casserait le calcul des sessions (palier suivant).
func (r *Runner) transformBatch(batch []kafka.Message) []Output {
	now := r.now()
	outs := make([]Output, len(batch))

	shards := make([][]int, r.cfg.Workers)
	for i, m := range batch {
		s := shardOf(m.Key, r.cfg.Workers)
		shards[s] = append(shards[s], i)
	}

	var wg sync.WaitGroup
	for _, indexes := range shards {
		if len(indexes) == 0 {
			continue
		}
		wg.Add(1)
		go func(indexes []int) {
			defer wg.Done()
			for _, i := range indexes {
				m := batch[i]
				// Chaque goroutine écrit dans des cases distinctes de outs : pas de data race.
				outs[i] = Transform(m.Value, Source{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset}, now)
			}
		}(indexes)
	}
	wg.Wait()
	return outs
}

// shardOf associe une clé à un numéro de shard, de façon stable.
func shardOf(key []byte, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write(key) // l'écriture dans un hash ne retourne jamais d'erreur
	return int(h.Sum32() % uint32(shards))
}

// writeWithRetry réessaie l'écriture avec un délai croissant. Un échec partiel suivi d'un
// nouvel essai peut dupliquer des messages : acceptable en "au moins une fois", à condition que
// les consommateurs en aval dédoublonnent grâce à l'identifiant de l'événement.
func (r *Runner) writeWithRetry(ctx context.Context, msgs []kafka.Message) error {
	backoff := r.cfg.RetryBackoff

	var err error
	for attempt := 1; attempt <= r.cfg.MaxWriteAttempts; attempt++ {
		if err = r.dst.WriteMessages(ctx, msgs...); err == nil {
			return nil
		}
		if attempt == r.cfg.MaxWriteAttempts {
			break
		}
		r.log.Warn("write failed, retrying",
			slog.Int("attempt", attempt), slog.Duration("backoff", backoff), slog.Any("error", err))

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return fmt.Errorf("%w (last write error: %v)", ctx.Err(), err)
		}
	}
	return fmt.Errorf("after %d attempts: %w", r.cfg.MaxWriteAttempts, err)
}
