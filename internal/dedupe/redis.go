package dedupe

import (
	"context"
	"fmt"
	"strconv"
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

// redisKey construit la clé Redis d'un événement : pulse:seen:<longueur du site>:<site>:<identifiant>.
// La longueur rend la clé sans ambiguïté quel que soit le contenu du site : avec un simple séparateur,
// ("a:b", "c") et ("a", "b:c") donneraient la même clé. Elle reste lisible dans redis-cli.
func redisKey(k Key) string {
	return keyPrefix + strconv.Itoa(len(k.SiteID)) + ":" + k.SiteID + ":" + k.EventID
}

// Redis est un Store adossé à Redis : une clé par événement, qui expire seule.
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
func (r *Redis) Seen(ctx context.Context, keys []Key) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = redisKey(k)
	}

	values, err := r.client.MGet(ctx, names...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget: %w", err)
	}
	if len(values) != len(keys) {
		return nil, fmt.Errorf("redis mget: %d values for %d keys", len(values), len(keys))
	}

	seen := make([]bool, len(keys))
	for i, v := range values {
		seen[i] = v != nil // une clé absente est retournée comme nil
	}
	return seen, nil
}

// Mark implémente Store avec un seul aller-retour (pipeline) pour tout le lot.
func (r *Redis) Mark(ctx context.Context, keys []Key) error {
	if len(keys) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, k := range keys {
		pipe.Set(ctx, redisKey(k), 1, r.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline set: %w", err)
	}
	return nil
}
