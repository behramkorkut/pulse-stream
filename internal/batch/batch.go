// Package batch contient la boucle commune aux consommateurs Kafka de pulse-stream :
// lire en continu, assembler des lots, laisser un Handler les traiter, puis seulement valider les offsets.
//
// Le Handler ne s'occupe que du métier. La garantie "au moins une fois" (ne valider qu'après un
// traitement réussi) et l'arrêt propre sont écrits une seule fois, ici.
package batch

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Source est la partie de *kafka.Reader dont la boucle a besoin.
type Source interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Handler traite un lot de messages. Il doit retourner nil seulement si tout le lot est traité :
// c'est alors, et seulement alors, que les offsets sont validés. En cas d'erreur, le lot sera relu.
type Handler func(ctx context.Context, msgs []kafka.Message) error

// Config règle la boucle.
type Config struct {
	Size     int           // taille maximale d'un lot (défaut : 200)
	Wait     time.Duration // attente maximale pour remplir un lot (défaut : 50 ms)
	Deadline time.Duration // temps accordé au traitement d'un lot et à son commit (défaut : 15 s)

	Metrics *Metrics // optionnel : nil = aucune mesure
}

func (c Config) withDefaults() Config {
	if c.Size <= 0 {
		c.Size = 200
	}
	if c.Wait <= 0 {
		c.Wait = 50 * time.Millisecond
	}
	if c.Deadline <= 0 {
		c.Deadline = 15 * time.Second
	}
	return c
}

// Run lit, assemble et traite les lots jusqu'à l'annulation de ctx (arrêt propre, retourne nil)
// ou jusqu'à une erreur irrécupérable (retourne l'erreur).
func Run(ctx context.Context, src Source, cfg Config, handle Handler) error {
	cfg = cfg.withDefaults()

	// Ce contexte dérivé garantit que la goroutine de lecture s'arrête quand Run se termine,
	// même sur erreur : sinon elle resterait bloquée indéfiniment (fuite de goroutine).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgs := make(chan kafka.Message, cfg.Size)
	fetchErr := make(chan error, 1)

	// Étage 1 : une goroutine lit Kafka en continu et alimente le canal. Pendant que le lot courant
	// est traité, la lecture du suivant avance déjà (pipeline).
	go func() {
		defer close(msgs)
		for {
			m, err := src.FetchMessage(ctx)
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
		lot, more := collect(msgs, cfg.Size, cfg.Wait)
		if len(lot) > 0 {
			if err := process(ctx, src, cfg, handle, lot); err != nil {
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

// process traite un lot puis valide ses offsets. Le lot en cours doit aller au bout même si un arrêt
// a été demandé : son contexte est détaché de l'annulation, avec tout de même une échéance.
func process(ctx context.Context, src Source, cfg Config, handle Handler, lot []kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Deadline)
	defer cancel()

	start := time.Now()
	err := handleAndCommit(ctx, src, handle, lot)
	cfg.Metrics.observe(len(lot), time.Since(start), err)
	return err
}

func handleAndCommit(ctx context.Context, src Source, handle Handler, lot []kafka.Message) error {
	if err := handle(ctx, lot); err != nil {
		return err
	}
	// Valider AVANT de traiter perdrait des messages en cas de crash ; valider APRÈS, au pire,
	// en relit (doublons). C'est la garantie "au moins une fois".
	if err := src.CommitMessages(ctx, lot...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// collect assemble un lot : attend le premier message (sans limite de temps), puis complète
// jusqu'à max messages ou jusqu'à l'expiration de wait, selon ce qui arrive en premier.
// more vaut false quand le canal est fermé : le lot retourné est alors le dernier.
func collect(ch <-chan kafka.Message, max int, wait time.Duration) (lot []kafka.Message, more bool) {
	first, ok := <-ch
	if !ok {
		return nil, false
	}
	lot = append(lot, first)

	timer := time.NewTimer(wait)
	defer timer.Stop()

	for len(lot) < max {
		select {
		case m, ok := <-ch:
			if !ok {
				return lot, false
			}
			lot = append(lot, m)
		case <-timer.C:
			return lot, true
		}
	}
	return lot, true
}

// RetryPolicy décrit combien de fois, et avec quel délai croissant, retenter une opération.
type RetryPolicy struct {
	MaxAttempts int           // défaut : 5
	Backoff     time.Duration // pause avant le 2e essai, doublée à chaque essai (défaut : 200 ms)
}

// Do exécute fn jusqu'à MaxAttempts fois. Retenter n'est sûr que si fn est idempotente ou si les
// doublons sont acceptables : c'est à l'appelant d'y veiller.
func (p RetryPolicy) Do(ctx context.Context, log *slog.Logger, what string, fn func() error) error {
	attempts, backoff := p.MaxAttempts, p.Backoff
	if attempts <= 0 {
		attempts = 5
	}
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == attempts {
			break
		}
		log.Warn("attempt failed, retrying",
			slog.String("step", what), slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff), slog.Any("error", err))

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %v)", ctx.Err(), err)
		}
	}
	return fmt.Errorf("after %d attempts: %w", attempts, err)
}

// NewReader crée un consommateur Kafka membre du groupe donné.
//
// Les partitions du topic sont réparties entre tous les membres du groupe : lancer une seconde
// instance avec le même groupID partage le travail, en arrêter une le redistribue.
func NewReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,

		MinBytes: 1,
		MaxBytes: 10 << 20,
		MaxWait:  250 * time.Millisecond,

		// Pour un NOUVEAU groupe (aucun offset enregistré) : partir du début du topic plutôt
		// que de la fin, afin de ne perdre aucun message déjà publié.
		StartOffset: kafka.FirstOffset,

		// Réaction à la disparition d'un membre. Si une instance meurt sans prévenir (crash, coupure), le courtier
		// ne le sait qu'à l'expiration de sa session. Jusque-là ses partitions ne sont lues par personne et les
		// autres membres ne peuvent plus valider d'offsets. Le défaut de kafka-go (30 s) a été mesuré trop long :
		// retard jusqu'à 175 000 messages et arrêts en cascade des survivants (commit au-delà du délai d'un lot,
		// 15 s), voir docs/resilience.md. 10 s ; Redpanda refuse moins de 6 s.
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  15 * time.Second,

		// On valide les offsets nous-mêmes (CommitMessages) après le traitement :
		// FetchMessage ne valide rien automatiquement.
	})
}
