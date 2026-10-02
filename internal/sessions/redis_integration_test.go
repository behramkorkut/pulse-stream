//go:build integration

// Test d'intégration : nécessite Redis (make up). Il utilise la base numéro 14, réservée à ce paquet, et la vide :
// ne jamais pointer ce test vers un Redis contenant des données à conserver.

package sessions

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testClient se connecte à la base 14, réservée à ce paquet.
func testClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: 14})
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis injoignable sur %s (make up ?) : %v", addr, err)
	}
	return client
}

func flushDB(t *testing.T, client *redis.Client) {
	t.Helper()
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB : %v", err)
	}
}

func TestRedisStoreContract(t *testing.T) {
	client := testClient(t)
	runStoreContract(t, func(t *testing.T) Store {
		flushDB(t, client)
		return NewRedis(client, DefaultTimeout)
	})
}

// Chaque clé écrite (état du visiteur, résultat mémorisé de chaque événement) doit expirer : sinon la mémoire de
// Redis croîtrait sans fin avec le nombre d'événements.
func TestRedisStoreKeysExpire(t *testing.T) {
	client := testClient(t)
	flushDB(t, client)
	ctx := context.Background()

	s := NewRedis(client, DefaultTimeout)
	touch(t, s, ev("s", "v", "e1", t0))
	touch(t, s, ev("s", "v", "e2", t0.Add(time.Minute)))

	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("KEYS : %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("%d clés %v, want 3 : l'état du visiteur et un résultat par événement", len(keys), keys)
	}
	for _, k := range keys {
		ttl, err := client.TTL(ctx, k).Result()
		if err != nil {
			t.Fatalf("TTL %s : %v", k, err)
		}
		if ttl <= 0 || ttl > 2*DefaultTimeout {
			t.Errorf("clé %s : durée de vie %v, want entre 0 et %v", k, ttl, 2*DefaultTimeout)
		}
	}
}
