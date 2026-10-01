package dedupe

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "pulse:seen:"

// DefaultTTL est la durée pendant laquelle un identifiant reste mémorisé.
//
// Limite assumée : la mémoire nécessaire croît avec le débit x la durée. À des centaines de milliers
// d'événements par seconde, une fenêtre d'une heure ne tiendrait pas dans un Redis : on utiliserait
// alors une fenêtre plus courte, un filtre de Bloom, ou un état local par partition. Ici, l'objectif est
// de montrer le mécanisme.
const DefaultTTL = time.Hour

// Redis est un Store adossé à Redis : une clé par identifiant, qui expire seule.
type Redis struct {
	client redis.Cmdable
	ttl    time.Duration
}

// NewRedis crée un Store Redis.
func NewRedis(client redis.Cmdable, ttl time.Duration) *Redis {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Redis{client: client, ttl: ttl}
}

// Seen implémente Store avec un seul aller-retour (MGET) pour tout le lot.
func (r *Redis) Seen(ctx context.Context, ids []string) ([]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = keyPrefix + id
	}

	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget: %w", err)
	}
	if len(values) != len(ids) {
		return nil, fmt.Errorf("redis mget: %d values for %d keys", len(values), len(ids))
	}

	seen := make([]bool, len(ids))
	for i, v := range values {
		seen[i] = v != nil // une clé absente est retournée comme nil
	}
	return seen, nil
}

// Mark implémente Store avec un seul aller-retour (pipeline) pour tout le lot.
func (r *Redis) Mark(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, id := range ids {
		pipe.Set(ctx, keyPrefix+id, 1, r.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline set: %w", err)
	}
	return nil
}
