package sessions

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store { return NewMemory(DefaultTimeout) })
}

func TestNewSessionIDIsStableAndDistinct(t *testing.T) {
	a := NewSessionID("site", "visitor", "evt-1")
	if a != NewSessionID("site", "visitor", "evt-1") {
		t.Error("l'identifiant doit être stable")
	}
	if a == NewSessionID("site", "visitor", "evt-2") || a == NewSessionID("site", "autre", "evt-1") {
		t.Error("des entrées différentes doivent donner des identifiants différents")
	}
	// Le séparateur évite qu'("ab","c") et ("a","bc") produisent la même empreinte.
	if NewSessionID("ab", "c", "e") == NewSessionID("a", "bc", "e") {
		t.Error("collision entre des concaténations ambiguës")
	}
}

// Le Store est appelé depuis plusieurs goroutines : ce test échoue sous `go test -race` s'il manque un verrou.
func TestMemoryStoreIsSafeForConcurrentUse(t *testing.T) {
	s := NewMemory(DefaultTimeout)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e := ev("s", "v", "e", t0.Add(time.Duration(i)*time.Second))
				e.VisitorID = string(rune('a' + g%4)) // des visiteurs partagés entre goroutines
				touch(t, s, e)
			}
		}(g)
	}
	wg.Wait()
}
