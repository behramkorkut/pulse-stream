package dedupe

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "pulse:seen:"

// redisKey construit la clé Redis d'un événement : pulse:seen:<longueur du site>:<site>:<identifiant>.
// La longueur rend la clé sans ambiguïté quel que soit le contenu du site : avec un simple séparateur,
// ("a:b", "c") et ("a", "b:c") donneraient la même clé. Elle reste lisible dans redis-cli.
func redisKey(k Key) string {
	return keyPrefix + strconv.Itoa(len(k.SiteID)) + ":" + k.SiteID + ":" + k.EventID
}

// claimScript réserve, d'un bloc, toutes les clés d'un lot : une clé libre prend le propriétaire proposé, une clé
// déjà réservée garde le sien. Un script Lua s'exécute sans qu'aucune autre commande s'intercale : deux instances
// ne peuvent pas réserver la même clé chacune de leur côté.
//
//	KEYS[i] clé de l'événement    ARGV[1] durée de vie (secondes)    ARGV[i+1] propriétaire proposé pour KEYS[i]
//
// Retourne le propriétaire retenu pour chaque clé, dans l'ordre.
const claimScript = `
local ttl = tonumber(ARGV[1])
local out = {}
for i, key in ipairs(KEYS) do
  local owner = redis.call('GET', key)
  if not owner then
    owner = ARGV[i + 1]
    redis.call('SET', key, owner, 'EX', ttl)
  end
  out[i] = owner
end
return out
`

// Redis est un Store adossé à Redis : une clé par événement, qui expire seule.
type Redis struct {
	client redis.Scripter
	script *redis.Script
	ttl    time.Duration
}

// NewRedis crée un Store Redis.
func NewRedis(client redis.Scripter, ttl time.Duration) *Redis {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Redis{client: client, script: redis.NewScript(claimScript), ttl: ttl}
}

// Claim implémente Store avec un seul aller-retour (un script) pour tout le lot.
func (r *Redis) Claim(ctx context.Context, keys []Key, owners []string) ([]string, error) {
	if err := checkLengths(keys, owners); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	names := make([]string, len(keys))
	args := make([]any, 0, len(keys)+1)
	args = append(args, int64(r.ttl.Seconds()))
	for i, k := range keys {
		names[i] = redisKey(k)
		args = append(args, owners[i])
	}

	got, err := r.script.Run(ctx, r.client, names, args...).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("redis claim script: %w", err)
	}
	if len(got) != len(keys) {
		return nil, fmt.Errorf("redis claim script: %d réponses pour %d clés", len(got), len(keys))
	}
	return got, nil
}
