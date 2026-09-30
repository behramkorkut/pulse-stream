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

// Result est le résultat de la transformation d'un message : exactement un des deux champs est renseigné.
type Result struct {
	Event *event.Enriched // message valide, prêt à être enrichi davantage (session) puis publié
	Dead  *DeadLetter     // message rejeté
}

// Output est la forme finale à publier : le JSON, et le topic visé.
type Output struct {
	Value []byte
	Dead  bool // true : à publier dans dead-letter ; false : dans enriched-events
}

// Transform décode, valide et enrichit un message brut. C'est une fonction pure : aucune entrée-sortie,
// l'heure est passée en paramètre. Elle ne rejette jamais silencieusement : tout message qu'elle
// ne peut pas transformer devient un rejet (Result.Dead).
//
// Pourquoi valider à nouveau alors que le collector l'a déjà fait ? Parce que le topic est une
// frontière : rien ne garantit que seul le collector y écrit (autre producteur, ancienne version, bug).
// À l'inverse, on ne rejette PAS les champs inconnus (contrairement au collector) : un consommateur
// tolérant continue de fonctionner quand un producteur plus récent ajoute un champ.
func Transform(raw []byte, src Source, now time.Time) Result {
	var e event.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Result{Dead: newDeadLetter(ReasonInvalidJSON, []string{err.Error()}, raw, src, now)}
	}

	if problems := e.Validate(now); len(problems) > 0 {
		return Result{Dead: newDeadLetter(ReasonInvalidEvent, problems, raw, src, now)}
	}

	info := ParseUserAgent(e.UserAgent)
	return Result{Event: &event.Enriched{
		Event:       e,
		Browser:     info.Browser,
		Device:      info.Device,
		IsBot:       info.Bot,
		ProcessedAt: now.UTC(),
	}}
}

// encode sérialise le résultat en JSON. Si un événement valide ne pouvait pas être sérialisé
// (ne devrait jamais arriver), il devient un rejet : rien ne disparaît.
func (r Result) encode(raw []byte, src Source, now time.Time) Output {
	if r.Event != nil {
		if value, err := json.Marshal(r.Event); err == nil {
			return Output{Value: value}
		} else {
			r = Result{Dead: newDeadLetter(ReasonEncodeError, []string{err.Error()}, raw, src, now)}
		}
	}

	value, err := json.Marshal(r.Dead)
	if err != nil {
		// Ne peut pas arriver avec ces types simples, mais un rejet ne doit jamais disparaître.
		value = []byte(`{"reason":"dead_letter_encode_error"}`)
	}
	return Output{Value: value, Dead: true}
}

func newDeadLetter(reason string, problems []string, raw []byte, src Source, now time.Time) *DeadLetter {
	rawText := string(raw)
	if len(rawText) > maxRawBytes {
		rawText = rawText[:maxRawBytes] + "...(truncated)"
	}
	return &DeadLetter{
		Reason:   reason,
		Problems: problems,
		Raw:      rawText,
		Source:   src,
		FailedAt: now.UTC(),
	}
}
