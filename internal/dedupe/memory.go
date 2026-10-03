package dedupe

import (
	"context"
	"sync"
)

// Memory est une implémentation en mémoire de Store, pour les tests. Elle n'expire jamais rien.
type Memory struct {
	mu     sync.Mutex
	owners map[Key]string
}

// NewMemory crée un Store en mémoire.
func NewMemory() *Memory {
	return &Memory{owners: map[Key]string{}}
}

// Claim implémente Store.
func (m *Memory) Claim(_ context.Context, keys []Key, owners []string) ([]string, error) {
	if err := checkLengths(keys, owners); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, len(keys))
	for i, k := range keys {
		owner, taken := m.owners[k]
		if !taken {
			owner = owners[i]
			m.owners[k] = owner
		}
		out[i] = owner
	}
	return out, nil
}
