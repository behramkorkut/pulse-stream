package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

var fixedNow = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

// fakePublisher enregistre les événements reçus. Le mutex montre le réflexe à avoir :
// un vrai serveur HTTP appelle les handlers depuis plusieurs goroutines en parallèle.
type fakePublisher struct {
	mu     sync.Mutex
	events []event.Event
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, e event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func newTestHandler(pub Publisher) http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newHandler(pub, log, func() time.Time { return fixedNow }, nil)
}

const validBody = `{
  "id": "evt-1",
  "type": "pageview",
  "site_id": "site-42",
  "visitor_id": "visitor-7",
  "url": "https://example.com/produits",
  "timestamp": "2026-09-30T11:59:59Z"
}`

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("User-Agent", "test-agent/1.0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCollectAcceptsValidEvent(t *testing.T) {
	pub := &fakePublisher{}
	rec := do(newTestHandler(pub), http.MethodPost, "/collect", validBody)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body)
	}
	if pub.count() != 1 {
		t.Fatalf("published %d events, want 1", pub.count())
	}

	got := pub.events[0]
	if !got.ReceivedAt.Equal(fixedNow) {
		t.Errorf("ReceivedAt = %v, want %v", got.ReceivedAt, fixedNow)
	}
	if got.IP != "192.0.2.0" { // 192.0.2.1 (adresse par défaut de httptest.NewRequest), tronquée en /24
		t.Errorf("IP = %q, want 192.0.2.0", got.IP)
	}
	if got.UserAgent != "test-agent/1.0" {
		t.Errorf("UserAgent = %q, want test-agent/1.0", got.UserAgent)
	}
}

func TestCollectRejections(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{name: "JSON invalide", method: http.MethodPost, path: "/collect", body: `{oops`, wantStatus: http.StatusBadRequest},
		{name: "champ inconnu", method: http.MethodPost, path: "/collect", body: `{"id":"a","bogus":1}`, wantStatus: http.StatusBadRequest},
		{name: "événement invalide", method: http.MethodPost, path: "/collect", body: `{"id":"a"}`, wantStatus: http.StatusUnprocessableEntity},
		{
			name: "corps trop gros", method: http.MethodPost, path: "/collect",
			body:       `{"id":"a","url":"https://example.com/` + strings.Repeat("a", 70_000) + `"}`,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{name: "mauvaise méthode", method: http.MethodGet, path: "/collect", body: "", wantStatus: http.StatusMethodNotAllowed},
		{name: "route inconnue", method: http.MethodPost, path: "/nope", body: validBody, wantStatus: http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			rec := do(newTestHandler(pub), tc.method, tc.path, tc.body)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body)
			}
			if pub.count() != 0 {
				t.Errorf("published %d events, want 0", pub.count())
			}
		})
	}
}

func TestCollectReturns503WhenPublishFails(t *testing.T) {
	pub := &fakePublisher{err: errors.New("kafka down")}
	rec := do(newTestHandler(pub), http.MethodPost, "/collect", validBody)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	rec := do(newTestHandler(&fakePublisher{}), http.MethodGet, "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Errorf("body = %s, want status ok", rec.Body)
	}
}

func TestMetricsCountRequestsByStatusCode(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pub := &fakePublisher{}
	h := newHandler(pub, log, func() time.Time { return fixedNow }, m)

	do(h, http.MethodPost, "/collect", validBody)                  // 202
	do(h, http.MethodPost, "/collect", validBody)                  // 202
	do(h, http.MethodPost, "/collect", `{oops`)                    // 400
	do(h, http.MethodPost, "/collect", `{"id":"x","type":"nope"}`) // champs inconnus ou invalides
	do(h, http.MethodGet, "/healthz", "")                          // hors /collect : non compté

	pub.err = errors.New("kafka down")
	do(h, http.MethodPost, "/collect", validBody) // 503

	for code, want := range map[string]float64{"202": 2, "400": 1, "503": 1} {
		if got := testutil.ToFloat64(m.requests.WithLabelValues(code)); got != want {
			t.Errorf("requêtes avec le code %s = %v, want %v", code, got, want)
		}
	}
	if got := testutil.CollectAndCount(m.duration); got != 1 {
		t.Errorf("l'histogramme des durées n'est pas alimenté (séries = %d)", got)
	}
}

func TestNilMetricsLeaveTheHandlerUntouched(t *testing.T) {
	h := newTestHandler(&fakePublisher{}) // métriques nil
	if rec := do(h, http.MethodPost, "/collect", validBody); rec.Code != http.StatusAccepted {
		t.Errorf("code = %d, want 202 : l'absence de métriques ne doit rien changer", rec.Code)
	}
}

func TestMetricsSeriesExistAtZeroFromTheStart(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	if got := testutil.CollectAndCount(m.requests); got != 5 {
		t.Errorf("séries pulse_collector_requests_total = %d, want 5 (un code par réponse possible)", got)
	}
}
