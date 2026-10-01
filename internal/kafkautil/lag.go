package kafkautil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// PartitionOffsets décrit où en est un groupe de consommateurs sur UNE partition.
type PartitionOffsets struct {
	Partition int
	First     int64 // premier offset encore disponible dans la partition
	End       int64 // prochain offset qui sera écrit (le "bout" de la partition)
	Committed int64 // prochain offset que le groupe lira ; -1 si le groupe n'a encore rien validé
}

// Lag est le nombre de messages publiés dans cette partition mais pas encore validés par le groupe.
func (p PartitionOffsets) Lag() int64 {
	from := p.Committed
	if from < p.First { // rien de validé (-1), ou début de partition déjà purgé : on compte depuis le début disponible
		from = p.First
	}
	if lag := p.End - from; lag > 0 {
		return lag
	}
	return 0
}

// TotalLag additionne le retard de toutes les partitions : c'est le retard du groupe entier.
func TotalLag(parts []PartitionOffsets) int64 {
	var total int64
	for _, p := range parts {
		total += p.Lag()
	}
	return total
}

// OffsetSource fournit les offsets d'un groupe sur toutes les partitions d'un topic.
// Une interface, pour tester le calcul sans broker.
type OffsetSource interface {
	Offsets(ctx context.Context) ([]PartitionOffsets, error)
}

// LagTracker mesure le retard du groupe en arrière-plan et le garde en mémoire : lire Value() ne coûte rien,
// donc une lecture par Prometheus n'appelle jamais le broker (et un broker lent ne bloque pas /metrics).
type LagTracker struct {
	lag atomic.Int64
}

// Value est le dernier retard mesuré (0 tant qu'aucune mesure n'a réussi).
func (t *LagTracker) Value() int64 { return t.lag.Load() }

// Refresh fait une mesure et la mémorise.
func (t *LagTracker) Refresh(ctx context.Context, src OffsetSource) error {
	parts, err := src.Offsets(ctx)
	if err != nil {
		return err
	}
	t.lag.Store(TotalLag(parts))
	return nil
}

// Run mesure toutes les `every` jusqu'à l'annulation de ctx. En cas d'échec, la dernière valeur reste affichée ;
// on le signale une fois, pas à chaque tentative, pour ne pas inonder les journaux.
func (t *LagTracker) Run(ctx context.Context, src OffsetSource, every time.Duration, log *slog.Logger) {
	healthy := true
	measure := func() {
		callCtx, cancel := context.WithTimeout(ctx, every)
		defer cancel()
		err := t.Refresh(callCtx, src)
		switch {
		case err != nil && ctx.Err() == nil && healthy:
			healthy = false
			log.Warn("lag measurement failed, keeping last value", slog.Any("error", err))
		case err == nil && !healthy:
			healthy = true
			log.Info("lag measurement recovered")
		}
	}

	measure()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			measure()
		}
	}
}

// groupSource lit les offsets d'un groupe dans Kafka avec le client d'administration de kafka-go.
type groupSource struct {
	client *kafka.Client
	topic  string
	group  string
}

// NewGroupSource retourne la source d'offsets du groupe `group` sur le topic `topic`.
func NewGroupSource(brokers []string, topic, group string) OffsetSource {
	return &groupSource{
		client: &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second},
		topic:  topic,
		group:  group,
	}
}

func (s *groupSource) Offsets(ctx context.Context) ([]PartitionOffsets, error) {
	meta, err := s.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{s.topic}})
	if err != nil {
		return nil, fmt.Errorf("metadata of %q: %w", s.topic, err)
	}
	if len(meta.Topics) != 1 {
		return nil, fmt.Errorf("metadata of %q: unexpected answer", s.topic)
	}
	if err := meta.Topics[0].Error; err != nil {
		return nil, fmt.Errorf("metadata of %q: %w", s.topic, err)
	}
	ids := make([]int, 0, len(meta.Topics[0].Partitions))
	for _, p := range meta.Topics[0].Partitions {
		ids = append(ids, p.ID)
	}
	if len(ids) == 0 {
		return nil, errors.New("topic has no partitions")
	}

	committedResp, err := s.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: s.group, Topics: map[string][]int{s.topic: ids}})
	if err != nil {
		return nil, fmt.Errorf("committed offsets of group %q: %w", s.group, err)
	}
	committed := make(map[int]int64, len(ids))
	for _, p := range committedResp.Topics[s.topic] {
		if p.Error != nil {
			return nil, fmt.Errorf("committed offset of partition %d: %w", p.Partition, p.Error)
		}
		committed[p.Partition] = p.CommittedOffset
	}

	reqs := make([]kafka.OffsetRequest, 0, 2*len(ids))
	for _, id := range ids {
		reqs = append(reqs, kafka.FirstOffsetOf(id), kafka.LastOffsetOf(id))
	}
	listed, err := s.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{s.topic: reqs}})
	if err != nil {
		return nil, fmt.Errorf("end offsets of %q: %w", s.topic, err)
	}

	out := make([]PartitionOffsets, 0, len(ids))
	for _, p := range listed.Topics[s.topic] {
		if p.Error != nil {
			return nil, fmt.Errorf("end offset of partition %d: %w", p.Partition, p.Error)
		}
		c, ok := committed[p.Partition]
		if !ok {
			c = -1
		}
		out = append(out, PartitionOffsets{Partition: p.Partition, First: p.FirstOffset, End: p.LastOffset, Committed: c})
	}
	return out, nil
}
