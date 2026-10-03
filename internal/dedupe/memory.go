package dedupe

import (
	"context"
	"sync"
)

// Memory est une implémentation en mémoire de Store, pour les tests. Elle n'expire jamais rien.
type Memory struct {
	mu   sync.Mutex
	seen map[Key]struct{}
}

// NewMemory crée un Store en mémoire.
func NewMemory() *Memory {
	return &Memory{seen: map[Key]struct{}{}}
}

// Seen implémente Store.
func (m *Memory) Seen(_ context.Context, keys []Key) ([]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]bool, len(keys))
	for i, k := range keys {
		_, out[i] = m.seen[k]
	}
	return out, nil
}

// Mark implémente Store.
func (m *Memory) Mark(_ context.Context, keys []Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, k := range keys {
		m.seen[k] = struct{}{}
	}
	return nil
}
