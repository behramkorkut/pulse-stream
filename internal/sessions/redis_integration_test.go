//go:build integration

// Test d'intégration : nécessite Redis (make up). Il utilise la base numéro 15 et la vide :
// ne jamais pointer ce test vers un Redis contenant des données à conserver.

package sessions

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreContract(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis injoignable sur %s (make up ?) : %v", addr, err)
	}

	runStoreContract(t, func(t *testing.T) Store {
		if err := client.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("FlushDB : %v", err)
		}
		return NewRedis(client, DefaultTimeout)
	})
}
