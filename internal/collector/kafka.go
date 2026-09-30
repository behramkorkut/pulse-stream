package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

const (
	// publishTimeout borne le temps qu'une requête HTTP peut attendre Kafka.
	// Sans borne, un broker en panne bloquerait chaque requête jusqu'au WriteTimeout du serveur HTTP
	// (10 s) et le client verrait une connexion coupée au lieu d'un 503 propre.
	publishTimeout = 3 * time.Second
)

// messageWriter est la partie de *kafka.Writer dont on a besoin. Déclarer une petite interface
// côté consommateur permet de tester KafkaPublisher sans broker, avec un faux écrivain.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaPublisher publie chaque événement accepté dans un topic Kafka.
type KafkaPublisher struct {
	w       messageWriter
	timeout time.Duration
}

// NewKafkaPublisher crée un publisher vers le topic donné.
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	w := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,

		// Murmur2 est l'algorithme de partitionnement du client Java officiel : une même clé
		// atterrit sur la même partition quel que soit le langage du producteur.
		Balancer: &kafka.Murmur2Balancer{},

		// Écriture synchrone : WriteMessages ne rend la main qu'une fois Kafka a acquitté.
		// Si Kafka est indisponible, on le sait et on répond 503 (le client réessaiera),
		// au lieu de répondre 202 pour un événement perdu.
		// RequireAll : toutes les répliques synchronisées doivent avoir écrit (ici une seule,
		// mais c'est le bon réglage à garder en production).
		RequiredAcks: kafka.RequireAll,

		// PIÈGE : la valeur par défaut de BatchTimeout est 1 s. En mode synchrone, chaque requête
		// attendrait jusqu'à 1 s que le lot soit "plein". À 10 ms, les requêtes HTTP concurrentes
		// sont regroupées en un seul envoi réseau sans ralentir sensiblement chacune d'elles.
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    200,

		WriteTimeout: 5 * time.Second,

		// On ne laisse pas un producteur créer des topics par accident (faute de frappe = topic fantôme).
		AllowAutoTopicCreation: false,
	}
	return newKafkaPublisher(w, publishTimeout)
}

func newKafkaPublisher(w messageWriter, timeout time.Duration) *KafkaPublisher {
	return &KafkaPublisher{w: w, timeout: timeout}
}

// Publish sérialise l'événement en JSON et l'écrit dans Kafka.
//
// Une erreur ne signifie pas toujours que le message n'est pas parti : un délai dépassé peut
// survenir après l'écriture. Le client réessaiera donc parfois un événement déjà stocké,
// c'est pourquoi chaque événement porte un identifiant unique (dédoublonnage en aval).
func (p *KafkaPublisher) Publish(ctx context.Context, e event.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	msg := kafka.Message{Key: partitionKey(e), Value: payload}
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to kafka: %w", err)
	}
	return nil
}

// Close vide les lots en attente puis ferme les connexions. À appeler après l'arrêt du serveur HTTP.
func (p *KafkaPublisher) Close() error {
	return p.w.Close()
}

// partitionKey choisit la clé de partitionnement : site + visiteur.
//
// Pourquoi le visiteur ? Kafka garantit l'ordre uniquement à l'intérieur d'une partition.
// Tous les événements d'un même visiteur sur la même partition, c'est ce qui permettra au
// processor de reconstituer ses sessions dans l'ordre, sans coordination entre workers.
//
// Pourquoi pas le site seul ? Un gros site enverrait tout son trafic sur une seule partition
// (partition "chaude") pendant que les autres restent presque vides.
func partitionKey(e event.Event) []byte {
	return []byte(e.SiteID + "/" + e.VisitorID)
}

// CheckTopic échoue vite (au démarrage) si le broker est injoignable ou si le topic n'existe pas.
// Mieux vaut un démarrage refusé avec un message clair que des 503 mystérieux en production.
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
