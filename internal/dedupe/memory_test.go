package dedupe

import "testing"

func TestMemoryStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store { return NewMemory() })
}
