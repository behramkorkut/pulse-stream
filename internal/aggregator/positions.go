package aggregator

import "github.com/behramkorkut/pulse-stream/internal/event"

// Partition identifie une partition Kafka.
type Partition struct {
	Topic string
	ID    int
}

// Counted est un événement à compter, avec la position (partition, offset) du message qui le porte.
type Counted struct {
	Event  event.Enriched
	From   Partition
	Offset int64
}

// NotYetApplied garde les événements que la position enregistrée ne couvre pas encore : ceux dont l'offset dépasse
// le dernier offset écrit pour leur partition. Une partition absente de positions n'a encore rien écrit.
//
// C'est ce qui rend l'écriture idempotente : un lot rejoué (crash avant la validation des offsets dans Kafka,
// rééquilibrage, nouvel essai après une erreur ambiguë) ne contient que des offsets déjà couverts, et n'ajoute rien.
// Les messages d'une partition sont lus dans l'ordre : tout offset inférieur à la position a déjà été traité.
func NotYetApplied(events []Counted, positions map[Partition]int64) []event.Enriched {
	out := make([]event.Enriched, 0, len(events))
	for _, c := range events {
		if pos, known := positions[c.From]; !known || c.Offset > pos {
			out = append(out, c.Event)
		}
	}
	return out
}

// Advanced retourne les partitions dont la position doit avancer, avec leur nouvel offset : celles où le lot va plus
// loin que la position enregistrée. Une position ne recule jamais.
func Advanced(upTo, positions map[Partition]int64) map[Partition]int64 {
	out := make(map[Partition]int64, len(upTo))
	for p, offset := range upTo {
		if pos, known := positions[p]; !known || offset > pos {
			out[p] = offset
		}
	}
	return out
}
