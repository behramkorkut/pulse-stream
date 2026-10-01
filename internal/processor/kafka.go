package processor

import (
	"time"

	"github.com/segmentio/kafka-go"
)

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
