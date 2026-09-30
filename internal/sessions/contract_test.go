package sessions

import (
	"context"
	"testing"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

var t0 = time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

func ev(site, visitor, id string, ts time.Time) event.Event {
	return event.Event{ID: id, SiteID: site, VisitorID: visitor, Timestamp: ts}
}

func touch(t *testing.T, s Store, e event.Event) Session {
	t.Helper()
	got, err := s.Touch(context.Background(), e)
	if err != nil {
		t.Fatalf("Touch(%s) error = %v", e.ID, err)
	}
	return got
}

// runStoreContract vérifie qu'une implémentation de Store respecte la règle de session.
// La même série de tests est appliquée à l'implémentation mémoire et à Redis : c'est ce qui garantit
// que les deux se comportent à l'identique. newStore doit retourner un Store vide.
func runStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("le premier événement ouvre une session", func(t *testing.T) {
		got := touch(t, newStore(t), ev("s", "v", "e1", t0))
		if got.ID == "" || !got.New {
			t.Errorf("got %+v, want une session neuve avec un identifiant", got)
		}
	})

	t.Run("dans le délai : même session", func(t *testing.T) {
		s := newStore(t)
		first := touch(t, s, ev("s", "v", "e1", t0))
		second := touch(t, s, ev("s", "v", "e2", t0.Add(10*time.Minute)))
		if second.ID != first.ID || second.New {
			t.Errorf("second = %+v, want la session %s, non neuve", second, first.ID)
		}
	})

	t.Run("exactement au délai : même session", func(t *testing.T) {
		s := newStore(t)
		first := touch(t, s, ev("s", "v", "e1", t0))
		second := touch(t, s, ev("s", "v", "e2", t0.Add(DefaultTimeout)))
		if second.ID != first.ID || second.New {
			t.Errorf("second = %+v, la limite est stricte : 30 min pile restent dans la session", second)
		}
	})

	t.Run("au-delà du délai : nouvelle session", func(t *testing.T) {
		s := newStore(t)
		first := touch(t, s, ev("s", "v", "e1", t0))
		second := touch(t, s, ev("s", "v", "e2", t0.Add(DefaultTimeout+time.Millisecond)))
		if second.ID == first.ID || !second.New {
			t.Errorf("second = %+v, want une nouvelle session (première : %s)", second, first.ID)
		}
	})

	t.Run("le délai est glissant", func(t *testing.T) {
		s := newStore(t)
		first := touch(t, s, ev("s", "v", "e0", t0))
		// Un événement toutes les 20 min pendant 80 min : jamais plus de 30 min d'écart.
		for i := 1; i <= 4; i++ {
			got := touch(t, s, ev("s", "v", "e"+string(rune('0'+i)), t0.Add(time.Duration(i)*20*time.Minute)))
			if got.ID != first.ID || got.New {
				t.Fatalf("événement %d : %+v, want la session initiale %s", i, got, first.ID)
			}
		}
	})

	t.Run("visiteurs et sites sont isolés", func(t *testing.T) {
		s := newStore(t)
		a := touch(t, s, ev("site-1", "v-1", "e1", t0))
		b := touch(t, s, ev("site-1", "v-2", "e2", t0))
		c := touch(t, s, ev("site-2", "v-1", "e3", t0))
		if a.ID == b.ID || a.ID == c.ID || !b.New || !c.New {
			t.Errorf("sessions confondues : %+v %+v %+v", a, b, c)
		}
	})

	t.Run("un événement en retard ne fait pas reculer la session", func(t *testing.T) {
		s := newStore(t)
		first := touch(t, s, ev("s", "v", "e1", t0))
		touch(t, s, ev("s", "v", "e2", t0.Add(20*time.Minute)))

		late := touch(t, s, ev("s", "v", "e3", t0.Add(5*time.Minute))) // arrive après e2
		if late.ID != first.ID || late.New {
			t.Fatalf("événement en retard : %+v, want la session courante %s", late, first.ID)
		}

		// 25 min après e2 : encore dans la session. Si l'événement en retard avait ramené
		// "le dernier instant vu" à +5 min, l'écart serait de 40 min et une session s'ouvrirait à tort.
		next := touch(t, s, ev("s", "v", "e4", t0.Add(45*time.Minute)))
		if next.ID != first.ID || next.New {
			t.Errorf("next = %+v, la session ne devait pas être coupée", next)
		}
	})

	t.Run("retraiter un événement donne le même résultat", func(t *testing.T) {
		s := newStore(t)
		a := ev("s", "v", "e1", t0)
		b := ev("s", "v", "e2", t0.Add(10*time.Minute))

		first := touch(t, s, a)
		if again := touch(t, s, a); again != first {
			t.Errorf("rejeu de e1 = %+v, want %+v", again, first)
		}
		touch(t, s, b)
		// Rejeu après un événement plus récent (redélivrance d'un lot après un crash).
		if again := touch(t, s, a); again != first {
			t.Errorf("rejeu tardif de e1 = %+v, want %+v (New doit rester vrai)", again, first)
		}
		if again := touch(t, s, b); again.New || again.ID != first.ID {
			t.Errorf("rejeu de e2 = %+v, want la session %s, non neuve", again, first.ID)
		}
	})

	t.Run("l'identifiant est déterministe", func(t *testing.T) {
		e := ev("s", "v", "e1", t0)
		one := touch(t, newStore(t), e)
		two := touch(t, newStore(t), e) // magasin neuf, comme après une perte de l'état
		if one.ID != two.ID {
			t.Errorf("identifiants différents pour le même premier événement : %s et %s", one.ID, two.ID)
		}
	})
}
