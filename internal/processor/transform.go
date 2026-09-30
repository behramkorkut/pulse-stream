package processor

import (
	"encoding/json"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Raisons de rejet.
const (
	ReasonInvalidJSON  = "invalid_json"
	ReasonInvalidEvent = "invalid_event"
	ReasonEncodeError  = "encode_error"
)

// maxRawBytes borne la taille du message d'origine recopié dans un rejet.
const maxRawBytes = 8 << 10

// Source identifie le message Kafka d'origine, pour retrouver un rejet dans le topic brut.
type Source struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// DeadLetter est l'enregistrement publié dans le topic dead-letter pour un message inexploitable.
type DeadLetter struct {
	Reason   string    `json:"reason"`
	Problems []string  `json:"problems,omitempty"`
	Raw      string    `json:"raw"`
	Source   Source    `json:"source"`
	FailedAt time.Time `json:"failed_at"`
}

// Output est le résultat du traitement d'un message : le JSON à publier, et le topic visé.
type Output struct {
	Value []byte
	Dead  bool // true : à publier dans dead-letter ; false : dans enriched-events
}

// Transform décode, valide et enrichit un message brut. C'est une fonction pure : aucune entrée-sortie,
// l'heure est passée en paramètre. Elle ne rejette jamais silencieusement : tout message qu'elle
// ne peut pas transformer devient un enregistrement dead-letter.
//
// Pourquoi valider à nouveau alors que le collector l'a déjà fait ? Parce que le topic est une
// frontière : rien ne garantit que seul le collector y écrit (autre producteur, ancienne version, bug).
// À l'inverse, on ne rejette PAS les champs inconnus (contrairement au collector) : un consommateur
// tolérant continue de fonctionner quand un producteur plus récent ajoute un champ.
func Transform(raw []byte, src Source, now time.Time) Output {
	var e event.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return deadLetter(ReasonInvalidJSON, []string{err.Error()}, raw, src, now)
	}

	if problems := e.Validate(now); len(problems) > 0 {
		return deadLetter(ReasonInvalidEvent, problems, raw, src, now)
	}

	info := ParseUserAgent(e.UserAgent)
	value, err := json.Marshal(event.Enriched{
		Event:       e,
		Browser:     info.Browser,
		Device:      info.Device,
		IsBot:       info.Bot,
		ProcessedAt: now.UTC(),
	})
	if err != nil {
		return deadLetter(ReasonEncodeError, []string{err.Error()}, raw, src, now)
	}
	return Output{Value: value}
}

func deadLetter(reason string, problems []string, raw []byte, src Source, now time.Time) Output {
	rawText := string(raw)
	if len(rawText) > maxRawBytes {
		rawText = rawText[:maxRawBytes] + "...(truncated)"
	}

	value, err := json.Marshal(DeadLetter{
		Reason:   reason,
		Problems: problems,
		Raw:      rawText,
		Source:   src,
		FailedAt: now.UTC(),
	})
	if err != nil {
		// Ne peut pas arriver avec ces types simples, mais un rejet ne doit jamais disparaître.
		value = []byte(`{"reason":"dead_letter_encode_error"}`)
	}
	return Output{Value: value, Dead: true}
}
