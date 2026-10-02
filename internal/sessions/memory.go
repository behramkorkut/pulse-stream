package sessions

import (
	"context"
	"sync"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Memory est une implémentation en mémoire de Store. Elle sert de référence exécutable de la règle
// (les tests de contrat la vérifient, puis vérifient que Redis se comporte pareil) et de double
// dans les tests. Elle n'expire jamais rien : ne pas l'utiliser en production.
type Memory struct {
	timeout time.Duration

	mu      sync.Mutex
	state   map[string]memState
	results map[string]Session // résultat de chaque événement déjà rattaché, par eventKey
}

type memState struct {
	id    string
	first string // identifiant du premier événement de la session
	last  int64  // dernier instant d'événement vu, en millisecondes
}

// NewMemory crée un Store en mémoire.
func NewMemory(timeout time.Duration) *Memory {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Memory{timeout: timeout, state: map[string]memState{}, results: map[string]Session{}}
}

// Touch applique la règle de session. Elle reproduit à l'identique le script Lua de Redis.
func (m *Memory) Touch(_ context.Context, e event.Event) (Session, error) {
	ts := e.Timestamp.UnixMilli()
	key := sessionKey(e)
	evKey := eventKey(e)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Rejeu : l'événement a déjà été rattaché, on redonne le même résultat sans toucher à l'état. Le recalculer
	// serait faux si le visiteur a ouvert une nouvelle session depuis : l'événement y serait rattaché à tort.
	if res, ok := m.results[evKey]; ok {
		return res, nil
	}

	st, ok := m.state[key]
	if !ok || ts-st.last > m.timeout.Milliseconds() {
		// Nouvelle session : premier événement du visiteur, ou inactivité dépassée.
		st = memState{id: NewSessionID(e.SiteID, e.VisitorID, e.ID), first: e.ID, last: ts}
	} else if ts > st.last {
		// Un événement en retard (ts plus ancien) est rattaché à la session courante,
		// mais ne fait jamais reculer le dernier instant vu.
		st.last = ts
	}
	m.state[key] = st

	// "New" compare avec le premier événement de la session plutôt que de dire "j'ai créé la session à cet
	// appel" : même sans résultat mémorisé (expiré dans Redis), retraiter le premier événement redonne New = true.
	res := Session{ID: st.id, New: st.first == e.ID}
	m.results[evKey] = res
	return res, nil
}
