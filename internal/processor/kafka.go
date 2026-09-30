package processor

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// NewReader crée un consommateur Kafka membre du groupe donné.
//
// Les partitions du topic sont réparties entre tous les membres du groupe : lancer une seconde
// instance avec le même groupId partage le travail, en arrêter une le redistribue.
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

		// On valide les offsets nous-mêmes (CommitMessages) après avoir écrit les résultats :
		// FetchMessage ne valide rien automatiquement.
	})
}

// NewWriter crée un producteur sans topic fixe : chaque message précise son topic de destination
// (enriched-events ou dead-letter), ce qui permet d'écrire un lot entier en un seul appel.
func NewWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Murmur2Balancer{},
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           10 * time.Millisecond,
		BatchSize:              500,
		WriteTimeout:           5 * time.Second,
		AllowAutoTopicCreation: false,
	}
}
