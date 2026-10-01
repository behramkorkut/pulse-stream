package loadgen

import "testing"

func TestTallyCountsEachAcceptedEventOnce(t *testing.T) {
	tally := Tally{}
	view := Request{ID: "e1", Type: "pageview", Kind: KindValid}
	click := Request{ID: "e2", Type: "click", Kind: KindValid}
	bot := Request{ID: "e3", Type: "pageview", Bot: true, Kind: KindValid}

	tally.Add(view, 202)
	tally.Add(Request{ID: "e1", Type: "pageview", Kind: KindDuplicate}, 202) // renvoi accepté : même identifiant
	tally.Add(click, 202)
	tally.Add(bot, 202)

	if got, want := tally.Totals(), (Totals{Pageviews: 1, Clicks: 1, Bots: 1}); got != want {
		t.Errorf("Totals() = %+v, want %+v", got, want)
	}
}

func TestTallyIgnoresWhatWasNotAccepted(t *testing.T) {
	tally := Tally{}
	tally.Add(Request{ID: "e1", Type: "pageview", Kind: KindValid}, 503) // Kafka indisponible
	tally.Add(Request{ID: "e2", Type: "pageview", Kind: KindValid}, 0)   // erreur de transport
	tally.Add(Request{Kind: KindInvalid, Body: []byte(`{oops`)}, 400)    // refusé à juste titre
	tally.Add(Request{ID: "e3", Type: "click", Kind: KindValid}, 202)    // seul accepté

	if got, want := tally.Totals(), (Totals{Clicks: 1}); got != want {
		t.Errorf("Totals() = %+v, want %+v", got, want)
	}
}

// Un événement refusé au premier envoi puis accepté à son renvoi doit compter une fois.
func TestTallyCountsAnEventAcceptedOnRetry(t *testing.T) {
	tally := Tally{}
	r := Request{ID: "e1", Type: "pageview", Kind: KindValid}
	tally.Add(r, 503)
	r.Kind = KindDuplicate
	tally.Add(r, 202)

	if got := tally.Totals().Pageviews; got != 1 {
		t.Errorf("pageviews = %d, want 1", got)
	}
}

func TestTallyMerge(t *testing.T) {
	a, b := Tally{}, Tally{}
	a.Add(Request{ID: "e1", Type: "pageview", Kind: KindValid}, 202)
	b.Add(Request{ID: "e1", Type: "pageview", Kind: KindDuplicate}, 202) // même événement vu par un autre worker
	b.Add(Request{ID: "e2", Type: "click", Kind: KindValid}, 202)
	a.Merge(b)

	if got, want := a.Totals(), (Totals{Pageviews: 1, Clicks: 1}); got != want {
		t.Errorf("Totals() après fusion = %+v, want %+v", got, want)
	}
}
