package dedupe

import (
	"context"
	"reflect"
	"testing"
)

// runStoreContract vérifie qu'une implémentation de Store respecte le contrat.
// La même série de tests s'applique à la version mémoire et à Redis. newStore doit retourner un Store vide.
func runStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	ctx := context.Background()

	t.Run("un identifiant jamais marqué n'est pas vu", func(t *testing.T) {
		got, err := newStore(t).Seen(ctx, []string{"a", "b"})
		if err != nil || !reflect.DeepEqual(got, []bool{false, false}) {
			t.Errorf("Seen() = %v, %v, want [false false]", got, err)
		}
	})

	t.Run("un identifiant marqué est vu, les autres non, dans l'ordre demandé", func(t *testing.T) {
		s := newStore(t)
		if err := s.Mark(ctx, []string{"b", "d"}); err != nil {
			t.Fatalf("Mark() error = %v", err)
		}
		got, err := s.Seen(ctx, []string{"a", "b", "c", "d"})
		if err != nil || !reflect.DeepEqual(got, []bool{false, true, false, true}) {
			t.Errorf("Seen() = %v, %v, want [false true false true]", got, err)
		}
	})

	t.Run("marquer deux fois est sans effet", func(t *testing.T) {
		s := newStore(t)
		for i := 0; i < 2; i++ {
			if err := s.Mark(ctx, []string{"a"}); err != nil {
				t.Fatalf("Mark() #%d error = %v", i+1, err)
			}
		}
		got, _ := s.Seen(ctx, []string{"a"})
		if !reflect.DeepEqual(got, []bool{true}) {
			t.Errorf("Seen() = %v, want [true]", got)
		}
	})

	t.Run("les listes vides sont acceptées", func(t *testing.T) {
		s := newStore(t)
		if got, err := s.Seen(ctx, nil); err != nil || len(got) != 0 {
			t.Errorf("Seen(nil) = %v, %v, want une liste vide sans erreur", got, err)
		}
		if err := s.Mark(ctx, nil); err != nil {
			t.Errorf("Mark(nil) error = %v", err)
		}
	})

	t.Run("des identifiants différents sont indépendants", func(t *testing.T) {
		s := newStore(t)
		_ = s.Mark(ctx, []string{"evt-1"})
		got, _ := s.Seen(ctx, []string{"evt-10", "evt-1", "evt-"})
		if !reflect.DeepEqual(got, []bool{false, true, false}) {
			t.Errorf("Seen() = %v, want [false true false]", got)
		}
	})
}
