package aggregator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

var base = time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)

// human et bot fabriquent des événements enrichis. offset est le décalage par rapport à 11:00:00.
func human(id, site, typ, device, browser string, offset time.Duration, newSession bool) event.Enriched {
	return event.Enriched{
		Event:      event.Event{ID: id, SiteID: site, VisitorID: "v-" + id, Type: typ, Timestamp: base.Add(offset)},
		Device:     device,
		Browser:    browser,
		NewSession: newSession,
	}
}

func bot(id, site string, offset time.Duration) event.Enriched {
	return event.Enriched{
		Event:  event.Event{ID: id, SiteID: site, VisitorID: "v-" + id, Type: event.TypePageview, Timestamp: base.Add(offset)},
		Device: "bot", Browser: "other", IsBot: true,
	}
}

func findBucket(t *testing.T, buckets []Bucket, id string) Bucket {
	t.Helper()
	for _, b := range buckets {
		if b.ID() == id {
			return b
		}
	}
	t.Fatalf("bucket %q introuvable parmi %d buckets", id, len(buckets))
	return Bucket{}
}

func TestAggregateCountsPerSiteAndMinute(t *testing.T) {
	events := []event.Enriched{
		human("e1", "site-42", event.TypePageview, "mobile", "safari", 10*time.Second, true),
		human("e2", "site-42", event.TypeClick, "desktop", "chrome", 50*time.Second, false),
		bot("e3", "site-42", 59*time.Second),
		human("e4", "site-42", event.TypePageview, "desktop", "chrome", 65*time.Second, true), // minute suivante
		human("e5", "site-43", event.TypePageview, "tablet", "firefox", 20*time.Second, true), // autre site
	}

	buckets := Aggregate(events)

	if len(buckets) != 3 {
		t.Fatalf("%d buckets, want 3 (site-42 à 11:00, site-42 à 11:01, site-43 à 11:00)", len(buckets))
	}

	first := findBucket(t, buckets, "site-42|2026-09-30T11:00Z")
	if first.Pageviews != 1 || first.Clicks != 1 || first.BotEvents != 1 || first.Sessions != 1 {
		t.Errorf("site-42 11:00 = pv %d clicks %d bots %d sessions %d, want 1 1 1 1",
			first.Pageviews, first.Clicks, first.BotEvents, first.Sessions)
	}
	if first.Devices["mobile"] != 1 || first.Devices["desktop"] != 1 || len(first.Devices) != 2 {
		t.Errorf("appareils = %v, want mobile et desktop à 1 (le robot n'y figure pas)", first.Devices)
	}
	if first.Browsers["safari"] != 1 || first.Browsers["chrome"] != 1 || len(first.Browsers) != 2 {
		t.Errorf("navigateurs = %v, want safari et chrome à 1", first.Browsers)
	}

	next := findBucket(t, buckets, "site-42|2026-09-30T11:01Z")
	if next.Pageviews != 1 || next.Sessions != 1 {
		t.Errorf("site-42 11:01 = %+v", next)
	}

	other := findBucket(t, buckets, "site-43|2026-09-30T11:00Z")
	if other.Pageviews != 1 || other.Devices["tablet"] != 1 {
		t.Errorf("site-43 11:00 = %+v", other)
	}
}

func TestAggregateBotsAreCountedApart(t *testing.T) {
	buckets := Aggregate([]event.Enriched{bot("e1", "site-42", 0), bot("e2", "site-42", time.Second)})

	b := findBucket(t, buckets, "site-42|2026-09-30T11:00Z")
	if b.BotEvents != 2 || b.Pageviews != 0 || b.Sessions != 0 || len(b.Devices) != 0 {
		t.Errorf("bucket = %+v, les robots ne doivent alimenter que bot_events", b)
	}
}

func TestAggregateUsesEventTimeNotArrivalTime(t *testing.T) {
	late := human("late", "site-42", event.TypePageview, "desktop", "chrome", -3*time.Hour, false) // 08:00
	buckets := Aggregate([]event.Enriched{late})

	if len(buckets) != 1 || buckets[0].ID() != "site-42|2026-09-30T08:00Z" {
		t.Errorf("buckets = %v, want un seul bucket à 08:00 (l'heure de l'événement)", buckets)
	}
}

// Les valeurs inconnues (ou hostiles) ne doivent jamais devenir des noms de champs MongoDB.
func TestAggregateSanitizesBreakdownKeys(t *testing.T) {
	weird := human("e1", "site-42", event.TypePageview, "$where", "evil.browser", 0, false)
	b := Aggregate([]event.Enriched{weird})[0]

	if b.Devices["other"] != 1 || len(b.Devices) != 1 {
		t.Errorf("appareils = %v, want {other: 1}", b.Devices)
	}
	if b.Browsers["other"] != 1 || len(b.Browsers) != 1 {
		t.Errorf("navigateurs = %v, want {other: 1}", b.Browsers)
	}
}

func TestAggregateIsDeterministicAndSorted(t *testing.T) {
	events := []event.Enriched{
		human("e1", "site-b", event.TypePageview, "desktop", "chrome", 0, false),
		human("e2", "site-a", event.TypePageview, "desktop", "chrome", 0, false),
		human("e3", "site-a", event.TypePageview, "desktop", "chrome", -time.Minute, false),
	}
	got := Aggregate(events)

	var ids []string
	for _, b := range got {
		ids = append(ids, b.ID())
	}
	want := "site-a|2026-09-30T10:59Z,site-a|2026-09-30T11:00Z,site-b|2026-09-30T11:00Z"
	if strings.Join(ids, ",") != want {
		t.Errorf("ids = %v, want %s", ids, want)
	}
}

func TestAggregateEmptyInput(t *testing.T) {
	if got := Aggregate(nil); len(got) != 0 {
		t.Errorf("Aggregate(nil) = %v, want aucun bucket", got)
	}
}

func TestDecode(t *testing.T) {
	valid := human("e1", "site-42", event.TypePageview, "desktop", "chrome", 0, true)
	raw, _ := json.Marshal(valid)

	got, err := Decode(raw)
	if err != nil || got.ID != "e1" || !got.NewSession {
		t.Fatalf("Decode() = %+v, %v", got, err)
	}

	for name, bad := range map[string]string{
		"JSON cassé":           `{oops`,
		"objet vide":           `{}`,
		"sans identifiant":     `{"site_id":"s","timestamp":"2026-09-30T11:00:00Z"}`,
		"sans site":            `{"id":"e","timestamp":"2026-09-30T11:00:00Z"}`,
		"sans horodatage":      `{"id":"e","site_id":"s"}`,
		"tableau plutôt objet": `[1]`,
	} {
		if _, err := Decode([]byte(bad)); err == nil {
			t.Errorf("Decode(%s) a réussi, want une erreur", name)
		}
	}
}
