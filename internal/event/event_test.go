package event

import (
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func validEvent() Event {
	return Event{
		ID:        "evt-1",
		Type:      TypePageview,
		SiteID:    "site-42",
		VisitorID: "visitor-7",
		URL:       "https://example.com/produits?id=3",
		Timestamp: now.Add(-time.Second),
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Event)
		wantErr string // sous-chaîne attendue dans les problèmes ; vide = événement valide
	}{
		{name: "événement valide", mutate: func(e *Event) {}},
		{name: "click valide", mutate: func(e *Event) { e.Type = TypeClick }},
		{name: "ancien événement accepté", mutate: func(e *Event) { e.Timestamp = now.Add(-48 * time.Hour) }},
		{name: "id manquant", mutate: func(e *Event) { e.ID = "" }, wantErr: "id is required"},
		{name: "site manquant", mutate: func(e *Event) { e.SiteID = "" }, wantErr: "site_id is required"},
		{name: "visiteur manquant", mutate: func(e *Event) { e.VisitorID = "" }, wantErr: "visitor_id is required"},
		{name: "id trop long", mutate: func(e *Event) { e.ID = strings.Repeat("x", 129) }, wantErr: "at most 128"},
		{name: "type inconnu", mutate: func(e *Event) { e.Type = "scroll" }, wantErr: "type must be"},
		{name: "url manquante", mutate: func(e *Event) { e.URL = "" }, wantErr: "url is required"},
		{name: "url relative", mutate: func(e *Event) { e.URL = "/produits" }, wantErr: "absolute http(s)"},
		{name: "url avec un autre schéma", mutate: func(e *Event) { e.URL = "javascript:alert(1)" }, wantErr: "absolute http(s)"},
		{name: "timestamp manquant", mutate: func(e *Event) { e.Timestamp = time.Time{} }, wantErr: "timestamp is required"},
		{name: "timestamp dans le futur", mutate: func(e *Event) { e.Timestamp = now.Add(time.Hour) }, wantErr: "in the future"},
		{name: "léger décalage d'horloge toléré", mutate: func(e *Event) { e.Timestamp = now.Add(time.Minute) }},
		{name: "referrer absent accepté", mutate: func(e *Event) { e.Referrer = "" }},
		{name: "referrer valide", mutate: func(e *Event) { e.Referrer = "https://www.google.com/" }},
		//{name: "referrer relatif rejeté", mutate: func(e *Event) { e.Referrer = "google" }, wantErr: "referrer must be an absolute http(s) URL"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(&e)

			problems := strings.Join(e.Validate(now), "; ")

			if tc.wantErr == "" && problems != "" {
				t.Fatalf("événement valide rejeté : %s", problems)
			}
			if tc.wantErr != "" && !strings.Contains(problems, tc.wantErr) {
				t.Fatalf("problèmes = %q, on attendait une mention de %q", problems, tc.wantErr)
			}

		})
	}

}

func TestValidateReportsAllProblems(t *testing.T) {
	problems := Event{}.Validate(now)
	// id, site_id, visitor_id, type, url, timestamp
	if len(problems) != 6 {
		t.Fatalf("len(problems) = %d (%v), want 6", len(problems), problems)
	}
}
