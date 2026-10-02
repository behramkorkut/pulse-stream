package sessions

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

const (
	keyPrefix    = "pulse:session:"        // état d'un visiteur (session en cours)
	resultPrefix = "pulse:session-result:" // résultat mémorisé du rattachement d'un événement
)

// touchScript lit puis met à jour l'état d'un visiteur EN UNE SEULE opération atomique.
//
// Pourquoi un script Lua et pas GET puis SET côté Go ? Entre le GET et le SET, une autre instance
// du processor pourrait modifier la même clé (course entre deux lectures-écritures). Redis exécute
// un script d'un bloc, sans intercaler aucune autre commande : la lecture et l'écriture sont atomiques.
//
//	KEYS[1] état du visiteur              KEYS[2] résultat mémorisé de cet événement
//	ARGV[1] instant de l'événement (ms)   ARGV[2] inactivité maximale (ms)
//	ARGV[3] identifiant proposé si nouvelle session
//	ARGV[4] identifiant de l'événement    ARGV[5] durée de vie des deux clés (secondes)
//
// Retourne {identifiant de session, 1 si cet événement est celui qui a ouvert la session}.
//
// Le résultat de chaque événement est mémorisé ("<0|1>:<identifiant de session>") et renvoyé tel quel au rejeu.
// Le recalculer serait faux quand le visiteur a ouvert une nouvelle session entre-temps : dans un lot rejoué qui
// franchit une coupure, les premiers événements seraient rattachés à la session suivante.
const touchScript = `
local memo = redis.call('GET', KEYS[2])
if memo then
  return {string.sub(memo, 3), tonumber(string.sub(memo, 1, 1))}
end

local ts      = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])

local state = redis.call('HMGET', KEYS[1], 'id', 'last', 'first')
local id    = state[1]
local last  = tonumber(state[2])
local first = state[3]

if (not id) or (ts - last > timeout) then
  id = ARGV[3]
  first = ARGV[4]
  redis.call('HSET', KEYS[1], 'id', id, 'first', first, 'last', ts)
elseif ts > last then
  redis.call('HSET', KEYS[1], 'last', ts)
end

-- La durée de vie ne sert qu'à libérer la mémoire des visiteurs partis : la règle de session,
-- elle, repose sur les instants des événements.
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))

local is_new = 0
if first == ARGV[4] then is_new = 1 end

redis.call('SET', KEYS[2], is_new .. ':' .. id, 'EX', tonumber(ARGV[5]))
return {id, is_new}
`

// Redis est un Store adossé à Redis.
type Redis struct {
	client  redis.Scripter
	script  *redis.Script
	timeout time.Duration
	ttl     time.Duration
}

// NewRedis crée un Store Redis. La clé d'un visiteur expire après deux fois le délai d'inactivité :
// passé ce délai, la clé ne pourrait de toute façon plus rattacher aucun événement en temps réel.
// Le résultat mémorisé de chaque événement a la même durée de vie : c'est la fenêtre dans laquelle un
// rejeu redonne exactement le même résultat. Coût : une clé par événement humain pendant cette durée.
func NewRedis(client redis.Scripter, timeout time.Duration) *Redis {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Redis{
		client:  client,
		script:  redis.NewScript(touchScript),
		timeout: timeout,
		ttl:     2 * timeout,
	}
}

// Touch applique la règle de session dans Redis.
func (r *Redis) Touch(ctx context.Context, e event.Event) (Session, error) {
	res, err := r.script.Run(ctx, r.client,
		[]string{keyPrefix + sessionKey(e), resultPrefix + eventKey(e)},
		e.Timestamp.UnixMilli(),
		r.timeout.Milliseconds(),
		NewSessionID(e.SiteID, e.VisitorID, e.ID),
		e.ID,
		int64(r.ttl.Seconds()),
	).Slice()
	if err != nil {
		return Session{}, fmt.Errorf("redis session script: %w", err)
	}

	if len(res) != 2 {
		return Session{}, fmt.Errorf("redis session script: unexpected reply %v", res)
	}
	id, ok1 := res[0].(string)
	isNew, ok2 := res[1].(int64)
	if !ok1 || !ok2 {
		return Session{}, fmt.Errorf("redis session script: unexpected reply types %T, %T", res[0], res[1])
	}
	return Session{ID: id, New: isNew == 1}, nil
}
