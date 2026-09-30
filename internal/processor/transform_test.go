package processor

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

var testNow = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

const chromeUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

func rawEvent(id, userAgent string) []byte {
	return []byte(`{"id":"` + id + `","type":"pageview","site_id":"site-42","visitor_id":"v-1",` +
		`"url":"https://example.com/","user_agent":"` + userAgent + `","timestamp":"2026-09-30T11:59:59Z"}`)
}

func TestTransformEnrichesValidEvent(t *testing.T) {
	out := Transform(rawEvent("evt-1", chromeUA), Source{}, testNow)

	if out.Dead {
		t.Fatalf("événement valide envoyé en dead-letter : %s", out.Value)
	}
	var got event.Enriched
	if err := json.Unmarshal(out.Value, &got); err != nil {
		t.Fatalf("sortie illisible : %v", err)
	}
	if got.ID != "evt-1" || got.SiteID != "site-42" {
		t.Errorf("champs d'origine perdus : %+v", got)
	}
	if got.Browser != "chrome" || got.Device != DeviceDesktop || got.IsBot {
		t.Errorf("enrichissement = browser %q device %q bot %v", got.Browser, got.Device, got.IsBot)
	}
	if !got.ProcessedAt.Equal(testNow) {
		t.Errorf("ProcessedAt = %v, want %v", got.ProcessedAt, testNow)
	}
}

func TestTransformFlagsBotsButKeepsThem(t *testing.T) {
	out := Transform(rawEvent("evt-bot", "Googlebot/2.1"), Source{}, testNow)

	var got event.Enriched
	if out.Dead || json.Unmarshal(out.Value, &got) != nil {
		t.Fatalf("un robot doit rester dans le flux enrichi : dead=%v value=%s", out.Dead, out.Value)
	}
	if !got.IsBot || got.Device != DeviceBot {
		t.Errorf("robot non détecté : %+v", got)
	}
}

func TestTransformToleratesUnknownFields(t *testing.T) {
	raw := []byte(`{"id":"evt-1","type":"pageview","site_id":"s","visitor_id":"v","url":"https://e.com/",` +
		`"timestamp":"2026-09-30T11:59:59Z","champ_du_futur":42}`)

	if out := Transform(raw, Source{}, testNow); out.Dead {
		t.Fatalf("un champ inconnu ne doit pas provoquer de rejet : %s", out.Value)
	}
}

func TestTransformRejections(t *testing.T) {
	src := Source{Topic: "raw-events", Partition: 3, Offset: 42}

	cases := []struct {
		name       string
		raw        []byte
		wantReason string
		wantDetail string
	}{
		{name: "JSON cassé", raw: []byte(`{oops`), wantReason: ReasonInvalidJSON},
		{name: "pas un objet", raw: []byte(`[1,2,3]`), wantReason: ReasonInvalidJSON},
		{name: "message vide", raw: []byte(``), wantReason: ReasonInvalidJSON},
		{name: "objet vide", raw: []byte(`{}`), wantReason: ReasonInvalidEvent, wantDetail: "id is required"},
		{name: "null", raw: []byte(`null`), wantReason: ReasonInvalidEvent, wantDetail: "id is required"},
		{
			name: "timestamp dans le futur",
			raw: []byte(`{"id":"a","type":"pageview","site_id":"s","visitor_id":"v","url":"https://e.com/",` +
				`"timestamp":"2027-01-01T00:00:00Z"}`),
			wantReason: ReasonInvalidEvent, wantDetail: "in the future",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Transform(tc.raw, src, testNow)
			if !out.Dead {
				t.Fatalf("message invalide accepté : %s", out.Value)
			}

			var dl DeadLetter
			if err := json.Unmarshal(out.Value, &dl); err != nil {
				t.Fatalf("dead-letter illisible : %v", err)
			}
			if dl.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", dl.Reason, tc.wantReason)
			}
			if dl.Raw != string(tc.raw) {
				t.Errorf("le message d'origine n'est pas conservé : %q", dl.Raw)
			}
			if dl.Source != src {
				t.Errorf("source = %+v, want %+v", dl.Source, src)
			}
			if tc.wantDetail != "" && !strings.Contains(strings.Join(dl.Problems, ";"), tc.wantDetail) {
				t.Errorf("problems = %v, want une mention de %q", dl.Problems, tc.wantDetail)
			}
			if len(dl.Problems) == 0 {
				t.Error("un rejet doit toujours expliquer pourquoi")
			}
		})
	}
}

func TestTransformTruncatesHugeRawInDeadLetter(t *testing.T) {
	huge := []byte("{" + strings.Repeat("x", 100_000))

	out := Transform(huge, Source{}, testNow)

	var dl DeadLetter
	if err := json.Unmarshal(out.Value, &dl); err != nil {
		t.Fatalf("dead-letter illisible : %v", err)
	}
	if len(dl.Raw) > maxRawBytes+50 {
		t.Errorf("len(raw) = %d, la troncature n'a pas été appliquée", len(dl.Raw))
	}
	if !strings.HasSuffix(dl.Raw, "(truncated)") {
		t.Error("le message tronqué doit le signaler")
	}
}
