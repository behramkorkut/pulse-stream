package dedupe

import (
	"context"
	"sync"
)

// Memory est une implémentation en mémoire de Store, pour les tests. Elle n'expire jamais rien.
type Memory struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemory crée un Store en mémoire.
func NewMemory() *Memory {
	return &Memory{seen: map[string]struct{}{}}
}

// Seen implémente Store.
func (m *Memory) Seen(_ context.Context, ids []string) ([]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]bool, len(ids))
	for i, id := range ids {
		_, out[i] = m.seen[id]
	}
	return out, nil
}

// Mark implémente Store.
func (m *Memory) Mark(_ context.Context, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range ids {
		m.seen[id] = struct{}{}
	}
	return nil
}
