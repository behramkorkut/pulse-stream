package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s : %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestMetricsEndpointServesTheRegistry(t *testing.T) {
	reg := NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "pulse_test_events_total", Help: "pour le test"})
	reg.MustRegister(c)
	c.Add(3)

	s, err := NewServer("127.0.0.1:0", reg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Run() }()
	defer func() { _ = s.Shutdown(context.Background()) }()

	status, body := get(t, "http://"+s.Addr()+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !strings.Contains(body, "pulse_test_events_total 3") {
		t.Errorf("la métrique de test est absente de la réponse :\n%s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("les métriques du runtime Go sont absentes de la réponse")
	}

	if status, _ := get(t, "http://"+s.Addr()+"/autre"); status != http.StatusNotFound {
		t.Errorf("GET /autre = %d, want 404 : seul /metrics est exposé", status)
	}
}

func TestNewServerFailsWhenThePortIsTaken(t *testing.T) {
	first, err := NewServer("127.0.0.1:0", NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Shutdown(context.Background()) }()

	if _, err := NewServer(first.Addr(), NewRegistry()); err == nil {
		t.Error("NewServer() a réussi sur un port déjà pris, want une erreur (échec rapide au démarrage)")
	}
}

func TestStartAndStop(t *testing.T) {
	stop, err := Start("127.0.0.1:0", NewRegistry(), quiet())
	if err != nil {
		t.Fatal(err)
	}
	stop() // ne doit ni bloquer ni paniquer
}
