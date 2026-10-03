package loadgen

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
	"github.com/behramkorkut/pulse-stream/internal/processor"
)

var testNow = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func newGen(mix Mix) *Generator {
	return NewGenerator(GenConfig{RunID: "t1", Sites: 3, Visitors: 100, Mix: mix, Seed: 42})
}

// decodeStrict applique les mêmes règles que le collector : champs inconnus refusés, puis validation.
func decodeStrict(body []byte) (event.Event, []string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var e event.Event
	if err := dec.Decode(&e); err != nil {
		return e, nil, err
	}
	return e, e.Validate(testNow), nil
}

func TestGeneratedEventsPassTheCollectorValidation(t *testing.T) {
	g := newGen(Mix{Bot: 0.3, Click: 0.4})
	for i := 0; i < 2000; i++ {
		r := g.Next(testNow)
		e, problems, err := decodeStrict(r.Body)
		if err != nil || len(problems) > 0 {
			t.Fatalf("événement %d refusé : err=%v problèmes=%v corps=%s", i, err, problems, r.Body)
		}
		if e.ID != r.ID || e.Type != r.Type {
			t.Fatalf("la requête annonce id=%q type=%q mais le corps contient id=%q type=%q", r.ID, r.Type, e.ID, e.Type)
		}
		if !strings.HasPrefix(e.SiteID, g.SitePrefix()) {
			t.Fatalf("site %q sans le préfixe %q : la vérification ne retrouverait pas cet événement", e.SiteID, g.SitePrefix())
		}
	}
}

// Les robots annoncés par le générateur doivent être reconnus comme tels par le processor, et les humains
// ne doivent jamais l'être : sinon les totaux attendus ne correspondraient pas à ce que MongoDB compte.
func TestBotsAreClassifiedLikeTheProcessorDoes(t *testing.T) {
	g := newGen(Mix{Bot: 0.5})
	var bots, humans int
	for i := 0; i < 2000; i++ {
		r := g.Next(testNow)
		e, _, err := decodeStrict(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if got := processor.ParseUserAgent(e.UserAgent).Bot; got != r.Bot {
			t.Fatalf("événement %s : le générateur dit bot=%v, le processor dit bot=%v (user-agent %q)", r.ID, r.Bot, got, e.UserAgent)
		}
		if r.Bot {
			bots++
		} else {
			humans++
		}
	}
	if bots == 0 || humans == 0 {
		t.Errorf("bots=%d humains=%d : le mélange devrait contenir les deux", bots, humans)
	}
}

func TestSameSeedGivesTheSameEvents(t *testing.T) {
	mix := Mix{Duplicate: 0.1, Invalid: 0.1, Bot: 0.2, Click: 0.3, Late: 0.1, TooLate: 0.05}
	a, b := newGen(mix), newGen(mix)
	for i := 0; i < 500; i++ {
		if ra, rb := a.Next(testNow), b.Next(testNow); !bytes.Equal(ra.Body, rb.Body) || ra.Kind != rb.Kind {
			t.Fatalf("requête %d différente avec la même graine :\n%s\n%s", i, ra.Body, rb.Body)
		}
	}
}

func TestMixProportions(t *testing.T) {
	const n = 40_000
	g := newGen(Mix{Duplicate: 0.10, Invalid: 0.05, Bot: 0.20, Click: 0.30})

	var dup, invalid, valid, bots, clicks int
	for i := 0; i < n; i++ {
		switch r := g.Next(testNow); r.Kind {
		case KindDuplicate:
			dup++
		case KindInvalid:
			invalid++
		default:
			valid++
			if r.Bot {
				bots++
			}
			if r.Type == "click" {
				clicks++
			}
		}
	}

	near := func(name string, got, total int, want float64) {
		t.Helper()
		if frac := float64(got) / float64(total); frac < want-0.02 || frac > want+0.02 {
			t.Errorf("%s : %.3f, want %.2f ± 0,02", name, frac, want)
		}
	}
	near("renvois", dup, n, 0.10)
	near("invalides", invalid, n, 0.05)
	near("robots", bots, valid, 0.20)
	near("clics", clicks, valid, 0.30)
}

func TestDuplicatesReplayAnEarlierEventUnchanged(t *testing.T) {
	g := newGen(Mix{Duplicate: 0.5})
	seen := map[string][]byte{}
	dups := 0
	for i := 0; i < 2000; i++ {
		r := g.Next(testNow)
		switch r.Kind {
		case KindValid:
			if _, exists := seen[r.ID]; exists {
				t.Fatalf("identifiant %s attribué deux fois à des événements « nouveaux »", r.ID)
			}
			seen[r.ID] = r.Body
		case KindDuplicate:
			dups++
			if want, ok := seen[r.ID]; !ok || !bytes.Equal(want, r.Body) {
				t.Fatalf("le renvoi de %s ne reprend pas l'événement d'origine à l'identique", r.ID)
			}
		}
	}
	if dups == 0 {
		t.Error("aucun renvoi généré")
	}
}

func TestInvalidRequestsAreRefusedByTheCollectorRules(t *testing.T) {
	g := newGen(Mix{Invalid: 1})
	var broken, incomplete int
	for i := 0; i < 200; i++ {
		r := g.Next(testNow)
		if r.Kind != KindInvalid || r.ID != "" {
			t.Fatalf("requête %d : kind=%v id=%q, want invalide sans identifiant", i, r.Kind, r.ID)
		}
		_, problems, err := decodeStrict(r.Body)
		switch {
		case err != nil:
			broken++ // le collector répondrait 400
		case len(problems) > 0:
			incomplete++ // le collector répondrait 422
		default:
			t.Fatalf("le corps %s serait ACCEPTÉ par le collector", r.Body)
		}
	}
	if broken == 0 || incomplete == 0 {
		t.Errorf("400 : %d, 422 : %d, want les deux variantes", broken, incomplete)
	}
}

// Chaque événement doit tomber nettement dans sa catégorie, et le processor doit en juger comme le générateur :
// sinon l'attendu de la vérification de bout en bout serait faux.
func TestLateEventsAreJudgedLikeTheProcessorDoes(t *testing.T) {
	const n = 4000
	g := newGen(Mix{Late: 0.30, TooLate: 0.20})

	var onTime, late, tooLate int
	for i := 0; i < n; i++ {
		r := g.Next(testNow)
		e, problems, err := decodeStrict(r.Body)
		if err != nil || len(problems) > 0 {
			t.Fatalf("événement %d refusé par le collector : err=%v problèmes=%v", i, err, problems)
		}

		lateness := testNow.Sub(e.Timestamp)
		switch {
		case r.TooLate:
			tooLate++
			if lateness < event.MaxLateness+lateMargin {
				t.Fatalf("événement %s « trop en retard » de %v seulement : trop près de la limite", r.ID, lateness)
			}
		case lateness == 0:
			onTime++
		default:
			late++
			if lateness < time.Minute || lateness > event.MaxLateness-lateMargin+time.Second {
				t.Fatalf("événement %s en retard de %v : hors de la bande tolérée", r.ID, lateness)
			}
		}

		// Ce que décide le processor, avec l'heure de réception que poserait le collector.
		e.ReceivedAt = testNow
		raw, _ := json.Marshal(e)
		res := processor.Transform(raw, processor.Source{}, testNow)
		if dead := res.Dead != nil && res.Dead.Reason == processor.ReasonTooLate; dead != r.TooLate {
			t.Fatalf("événement %s : générateur TooLate=%v, processor dead-letter too_late=%v", r.ID, r.TooLate, dead)
		}
	}

	near := func(name string, got int, want float64) {
		t.Helper()
		if frac := float64(got) / n; frac < want-0.03 || frac > want+0.03 {
			t.Errorf("%s : %.3f, want %.2f ± 0,03", name, frac, want)
		}
	}
	near("à l'heure", onTime, 0.50)
	near("en retard", late, 0.30)
	near("trop en retard", tooLate, 0.20)
}
