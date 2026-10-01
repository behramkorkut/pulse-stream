package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func quickGen() *Generator { return newGen(Mix{Duplicate: 0.1, Invalid: 0.1, Bot: 0.2, Click: 0.3}) }

func TestPercentile(t *testing.T) {
	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Millisecond // 1 ms ... 100 ms
	}
	tests := []struct {
		p    float64
		want time.Duration
	}{
		{0.50, 50 * time.Millisecond},
		{0.95, 95 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1.00, 100 * time.Millisecond},
		{0.00, 1 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := percentile(sorted, tt.p); got != tt.want {
			t.Errorf("percentile(%.2f) = %v, want %v", tt.p, got, tt.want)
		}
	}
	if got := percentile(nil, 0.99); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
}

func TestRunStepSendsAtTheRequestedRate(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := RunStep(context.Background(), Options{URL: srv.URL, Rate: 200, Duration: time.Second, Workers: 16, Gen: quickGen()})
	if err != nil {
		t.Fatal(err)
	}

	// 200 requêtes/s pendant 1 s : on tolère large (machines d'intégration continue lentes).
	if n := hits.Load(); n < 150 || n > 230 {
		t.Errorf("%d requêtes reçues, want environ 200", n)
	}
	if res.Dropped != 0 || res.Errors != 0 {
		t.Errorf("perdues=%d erreurs=%d, want 0 contre un serveur instantané", res.Dropped, res.Errors)
	}
	if res.Codes[202] != int(hits.Load()) {
		t.Errorf("codes 202 = %d, requêtes reçues = %d", res.Codes[202], hits.Load())
	}
	if res.Accepted < 140 {
		t.Errorf("acceptés/s = %.0f, want proche de 200", res.Accepted)
	}
	if res.P50 <= 0 || res.P99 < res.P50 || res.Max < res.P99 {
		t.Errorf("latences incohérentes : p50=%v p99=%v max=%v", res.P50, res.P99, res.Max)
	}
}

// La latence se mesure depuis l'instant PLANIFIÉ : un serveur lent doit apparaître lent dans les mesures.
func TestRunStepMeasuresLatencyFromTheIntendedSendTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := RunStep(context.Background(), Options{URL: srv.URL, Rate: 50, Duration: 500 * time.Millisecond, Workers: 32, Gen: quickGen()})
	if err != nil {
		t.Fatal(err)
	}
	if res.P50 < 60*time.Millisecond {
		t.Errorf("p50 = %v, want au moins 60 ms (le serveur met 60 ms à répondre)", res.P50)
	}
}

// Omission coordonnée : quand les requêtes font la queue derrière un seul worker, l'attente DOIT apparaître
// dans la latence. Mesurée depuis la prise en charge par le worker, elle resterait à ~20 ms et masquerait le
// problème ; mesurée depuis l'instant planifié, elle grimpe avec la file.
func TestRunStepLatencyIncludesTimeSpentWaitingInTheQueue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	// 100 req/s pendant 400 ms, mais un seul worker à 20 ms par requête : il n'en traite que 50/s.
	res, err := RunStep(context.Background(), Options{URL: srv.URL, Rate: 100, Duration: 400 * time.Millisecond, Workers: 1, Queue: 100, Gen: quickGen()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Max < 100*time.Millisecond {
		t.Errorf("max = %v, want >= 100 ms : l'attente en file doit compter dans la latence", res.Max)
	}
}

// Si le client n'a plus de worker libre ni de place en file, la requête est comptée "perdue" au lieu de
// ralentir la planification : c'est le principe du débit imposé, et la mesure indique que le CLIENT sature.
func TestRunStepCountsDroppedRequestsWhenTheClientSaturates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := RunStep(context.Background(), Options{URL: srv.URL, Rate: 400, Duration: 500 * time.Millisecond, Workers: 2, Queue: 2, Gen: quickGen()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Dropped == 0 {
		t.Errorf("perdues = 0, want > 0 : 2 workers à 200 ms ne peuvent pas tenir 400 requêtes/s")
	}
	if res.Scheduled+res.Dropped < 150 {
		t.Errorf("planifiées+perdues = %d, want environ 200 : la planification ne doit pas ralentir avec le serveur", res.Scheduled+res.Dropped)
	}
}

func TestRunStepRecordsErrorCodesAndAcksOnlyAcceptedEvents(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := RunStep(context.Background(), Options{URL: srv.URL, Rate: 100, Duration: 500 * time.Millisecond, Workers: 8, Gen: newGen(Mix{})})
	if err != nil {
		t.Fatal(err)
	}
	if res.Codes[503] == 0 || res.Codes[202] == 0 {
		t.Fatalf("codes = %v, want des 202 et des 503", res.Codes)
	}
	acked := res.Acked.Totals()
	if got := int(acked.Pageviews + acked.Clicks + acked.Bots); got != res.Codes[202] {
		t.Errorf("%d événements acquittés, want %d (un par réponse 202, sans doublons ici)", got, res.Codes[202])
	}
}

func TestRunStepCountsTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // plus personne n'écoute

	res, err := RunStep(context.Background(), Options{URL: url, Rate: 50, Duration: 300 * time.Millisecond, Workers: 4, Gen: quickGen()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors == 0 || len(res.Codes) != 0 {
		t.Errorf("erreurs=%d codes=%v, want des erreurs de transport et aucun code", res.Errors, res.Codes)
	}
}

func TestRunStepStopsWhenTheContextIsCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)

	start := time.Now()
	if _, err := RunStep(ctx, Options{URL: srv.URL, Rate: 100, Duration: 30 * time.Second, Workers: 4, Gen: quickGen()}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("RunStep a duré %v après l'annulation, want un arrêt rapide", elapsed)
	}
}

func TestRunStepRejectsBadOptions(t *testing.T) {
	for name, o := range map[string]Options{
		"sans URL":    {Rate: 1, Duration: time.Second, Gen: quickGen()},
		"débit nul":   {URL: "http://x", Duration: time.Second, Gen: quickGen()},
		"sans durée":  {URL: "http://x", Rate: 1, Gen: quickGen()},
		"sans source": {URL: "http://x", Rate: 1, Duration: time.Second},
	} {
		if _, err := RunStep(context.Background(), o); err == nil {
			t.Errorf("%s : RunStep a réussi, want une erreur", name)
		}
	}
}
