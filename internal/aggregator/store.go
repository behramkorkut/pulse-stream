package aggregator

import (
	"context"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Applied est le résultat d'une écriture.
type Applied struct {
	Events  []event.Enriched // événements comptés ; les autres étaient déjà couverts par la position enregistrée
	Buckets int              // documents (site, minute) mis à jour
}

// Store écrit des compteurs de façon idempotente.
type Store interface {
	// Apply cumule les compteurs des événements et enregistre, pour chaque partition de upTo, l'offset du dernier
	// message traité, le tout de façon ATOMIQUE. Les événements que la position enregistrée couvre déjà sont ignorés :
	// un lot rejoué (crash avant la validation des offsets, rééquilibrage, nouvel essai) n'a aucun effet.
	//
	// Chaque position (partition, offset) apparaît au plus une fois dans events : la position enregistrée protège
	// d'un lot à l'autre, pas à l'intérieur d'un lot. Le Runner écarte les messages livrés deux fois (decode).
	Apply(ctx context.Context, events []Counted, upTo map[Partition]int64) (Applied, error)
}
