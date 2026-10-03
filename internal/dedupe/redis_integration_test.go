//go:build integration

// Test d'intégration : nécessite Redis (make up). Il utilise la base numéro 15, réservée à ce paquet, et la vide :
// ne jamais pointer ce test vers un Redis contenant des données à conserver.

package dedupe

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis injoignable sur %s (make up ?) : %v", addr, err)
	}
	return client
}

func TestRedisStoreContract(t *testing.T) {
	client := testClient(t)

	runStoreContract(t, func(t *testing.T) Store {
		if err := client.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("FlushDB : %v", err)
		}
		return NewRedis(client, time.Hour)
	})
}

// Les clés doivent expirer d'elles-mêmes, sinon la mémoire de Redis grossit sans limite.
func TestRedisStoreKeysExpire(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB : %v", err)
	}

	store := NewRedis(client, 90*time.Second)
	key := Key{SiteID: "site-ttl", EventID: "evt-ttl"}
	if err := store.Mark(ctx, []Key{key}); err != nil {
		t.Fatalf("Mark() error = %v", err)
	}

	ttl, err := client.TTL(ctx, redisKey(key)).Result()
	if err != nil {
		t.Fatalf("TTL : %v", err)
	}
	if ttl <= 0 || ttl > 90*time.Second {
		t.Errorf("TTL = %v, want une valeur dans ]0 ; 90 s]", ttl)
	}
}
