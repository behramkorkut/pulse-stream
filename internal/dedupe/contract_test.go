package dedupe

import (
	"context"
	"reflect"
	"testing"
)

// keysOf fabrique des clés du site "s" : la plupart des cas ne portent que sur l'identifiant.
func keysOf(ids ...string) []Key {
	keys := make([]Key, len(ids))
	for i, id := range ids {
		keys[i] = Key{SiteID: "s", EventID: id}
	}
	return keys
}

// runStoreContract vérifie qu'une implémentation de Store respecte le contrat.
// La même série de tests s'applique à la version mémoire et à Redis. newStore doit retourner un Store vide.
func runStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	ctx := context.Background()

	claim := func(t *testing.T, s Store, keys []Key, owners ...string) []string {
		t.Helper()
		got, err := s.Claim(ctx, keys, owners)
		if err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return got
	}

	t.Run("une clé libre est réservée par le message qui la demande", func(t *testing.T) {
		got := claim(t, newStore(t), keysOf("a", "b"), "m1", "m2")
		if !reflect.DeepEqual(got, []string{"m1", "m2"}) {
			t.Errorf("Claim() = %v, want [m1 m2]", got)
		}
	})

	t.Run("une clé réservée garde son propriétaire", func(t *testing.T) {
		s := newStore(t)
		claim(t, s, keysOf("a"), "m1")
		if got := claim(t, s, keysOf("a", "b"), "m2", "m2"); !reflect.DeepEqual(got, []string{"m1", "m2"}) {
			t.Errorf("Claim() = %v, want [m1 m2] : a appartient déjà à m1", got)
		}
	})

	// C'est ce qui rend la réservation sûre AVANT l'écriture : un message relu après un échec se retrouve propriétaire.
	t.Run("un message relu retrouve sa propre réservation", func(t *testing.T) {
		s := newStore(t)
		claim(t, s, keysOf("a"), "m1")
		if got := claim(t, s, keysOf("a"), "m1"); !reflect.DeepEqual(got, []string{"m1"}) {
			t.Errorf("Claim() = %v, want [m1]", got)
		}
	})

	t.Run("dans un même appel, la première occurrence l'emporte", func(t *testing.T) {
		got := claim(t, newStore(t), keysOf("a", "a"), "m1", "m2")
		if !reflect.DeepEqual(got, []string{"m1", "m1"}) {
			t.Errorf("Claim() = %v, want [m1 m1]", got)
		}
	})

	t.Run("les listes vides sont acceptées", func(t *testing.T) {
		if got, err := newStore(t).Claim(ctx, nil, nil); err != nil || len(got) != 0 {
			t.Errorf("Claim(nil) = %v, %v, want une liste vide sans erreur", got, err)
		}
	})

	t.Run("un propriétaire par clé est exigé", func(t *testing.T) {
		if _, err := newStore(t).Claim(ctx, keysOf("a", "b"), []string{"m1"}); err == nil {
			t.Error("Claim() avec 2 clés et 1 propriétaire : want une erreur")
		}
	})

	t.Run("des identifiants différents sont indépendants", func(t *testing.T) {
		s := newStore(t)
		claim(t, s, keysOf("evt-1"), "m1")
		got := claim(t, s, keysOf("evt-10", "evt-1", "evt-"), "m2", "m2", "m2")
		if !reflect.DeepEqual(got, []string{"m2", "m1", "m2"}) {
			t.Errorf("Claim() = %v, want [m2 m1 m2]", got)
		}
	})

	// Régression : l'identifiant vient du client, il n'est unique qu'au sein d'un site. Avec l'identifiant seul
	// comme clé, l'événement evt-1 de site-2 était écarté comme doublon de celui de site-1, sans bruit.
	t.Run("le même identifiant sur deux sites reste deux événements", func(t *testing.T) {
		s := newStore(t)
		claim(t, s, []Key{{SiteID: "site-1", EventID: "evt-1"}}, "m1")
		got := claim(t, s, []Key{{SiteID: "site-1", EventID: "evt-1"}, {SiteID: "site-2", EventID: "evt-1"}}, "m2", "m2")
		if !reflect.DeepEqual(got, []string{"m1", "m2"}) {
			t.Errorf("Claim() = %v, want [m1 m2] : evt-1 de site-2 est un autre événement", got)
		}
	})

	t.Run("la frontière entre site et identifiant est sans ambiguïté", func(t *testing.T) {
		s := newStore(t)
		claim(t, s, []Key{{SiteID: "a:b", EventID: "c"}}, "m1")
		got := claim(t, s, []Key{{SiteID: "a", EventID: "b:c"}, {SiteID: "a:b", EventID: "c"}}, "m2", "m2")
		if !reflect.DeepEqual(got, []string{"m2", "m1"}) {
			t.Errorf("Claim() = %v, want [m2 m1]", got)
		}
	})
}
