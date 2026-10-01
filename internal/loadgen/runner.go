package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Options règle un palier de charge.
type Options struct {
	URL      string
	Rate     int           // requêtes par seconde visées
	Duration time.Duration // durée du palier
	Workers  int           // requêtes simultanées au maximum (défaut : 128)
	Queue    int           // requêtes qui peuvent attendre un worker libre avant d'être comptées "perdues" (défaut : Workers)
	Client   *http.Client
	Gen      *Generator
}

// StepResult est le bilan d'un palier.
type StepResult struct {
	Rate               int           // débit visé
	Duration           time.Duration // durée du palier
	Scheduled          int           // requêtes planifiées (et mises en file)
	Dropped            int           // requêtes que le client n'a pas pu mettre en file : saturation du CLIENT
	Errors             int           // erreurs de transport (connexion refusée, délai dépassé...)
	Codes              map[int]int   // réponses par code HTTP
	Accepted           float64       // réponses 202 par seconde
	P50, P95, P99, Max time.Duration
	Acked              Tally // événements acceptés (pour la vérification)
}

type job struct {
	req      Request
	intended time.Time // instant où la requête DEVAIT partir
}

type workerStats struct {
	lat    []time.Duration
	codes  map[int]int
	errors int
	acked  Tally
}

// RunStep envoie du trafic à débit imposé pendant Duration, puis attend la fin des requêtes en cours.
//
// Débit IMPOSÉ ("open-loop") : les requêtes sont planifiées sur une grille régulière, quoi que fasse le
// serveur. La latence se mesure depuis l'instant PLANIFIÉ. Si le serveur ralentit, les requêtes attendent
// en file et leur latence augmente, comme pour de vrais utilisateurs qui n'attendent pas leur tour.
// À l'inverse, une boucle "envoie, attends la réponse, recommence" ralentit avec le serveur et ne mesure
// jamais l'attente qu'elle évite de subir ("coordinated omission") : les percentiles seraient faux.
func RunStep(ctx context.Context, o Options) (StepResult, error) {
	if o.URL == "" || o.Rate <= 0 || o.Duration <= 0 || o.Gen == nil {
		return StepResult{}, errors.New("loadgen: URL, Rate, Duration et Gen sont obligatoires")
	}
	if o.Workers <= 0 {
		o.Workers = 128
	}
	if o.Queue <= 0 {
		o.Queue = o.Workers
	}
	if o.Client == nil {
		o.Client = newClient(o.Workers)
	}

	jobs := make(chan job, o.Queue)
	stats := make([]*workerStats, o.Workers)
	var wg sync.WaitGroup
	for i := range stats {
		stats[i] = &workerStats{codes: map[int]int{}, acked: Tally{}}
		wg.Add(1)
		go func(ws *workerStats) {
			defer wg.Done()
			for j := range jobs {
				status, err := post(ctx, o.Client, o.URL, j.req.Body)
				ws.lat = append(ws.lat, time.Since(j.intended))
				if err != nil {
					ws.errors++
					continue
				}
				ws.codes[status]++
				ws.acked.Add(j.req, status)
			}
		}(stats[i])
	}

	var scheduled, dropped int
	interval := time.Second / time.Duration(o.Rate)
	start := time.Now()
schedule:
	for i := 0; ; i++ {
		target := start.Add(time.Duration(i) * interval)
		if target.Sub(start) >= o.Duration {
			break
		}
		// Sous 1 ms, dormir est imprécis (le système dort plus longtemps) : on laisse partir en légère avance,
		// et la requête retombe sur la grille grâce au rattrapage de la boucle (si on est en retard, on ne dort pas).
		if d := time.Until(target); d >= time.Millisecond {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				break schedule
			}
		} else if ctx.Err() != nil {
			break
		}

		intended := target
		if now := time.Now(); now.Before(target) {
			intended = now // parti en avance : la latence se mesure depuis le départ réel
		}
		select {
		case jobs <- job{req: o.Gen.Next(intended), intended: intended}:
			scheduled++
		default:
			dropped++
		}
	}
	close(jobs)
	wg.Wait()

	return summarize(o, stats, scheduled, dropped), nil
}

func summarize(o Options, stats []*workerStats, scheduled, dropped int) StepResult {
	res := StepResult{Rate: o.Rate, Duration: o.Duration, Scheduled: scheduled, Dropped: dropped, Codes: map[int]int{}, Acked: Tally{}}

	var lat []time.Duration
	for _, ws := range stats {
		lat = append(lat, ws.lat...)
		res.Errors += ws.errors
		for code, n := range ws.codes {
			res.Codes[code] += n
		}
		res.Acked.Merge(ws.acked)
	}

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	res.P50, res.P95, res.P99 = percentile(lat, 0.50), percentile(lat, 0.95), percentile(lat, 0.99)
	if n := len(lat); n > 0 {
		res.Max = lat[n-1]
	}
	res.Accepted = float64(res.Codes[202]) / o.Duration.Seconds()
	return res
}

// percentile retourne le p-ième percentile d'un échantillon TRIÉ (méthode du rang supérieur).
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p*float64(len(sorted))+0.999999) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// newClient crée un client HTTP qui réutilise ses connexions : ouvrir une connexion TCP par requête
// mesurerait le coût du système d'exploitation plutôt que celui du collector.
func newClient(workers int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = workers
	t.MaxIdleConnsPerHost = workers
	t.MaxConnsPerHost = workers
	return &http.Client{Transport: t, Timeout: 5 * time.Second}
}

func post(ctx context.Context, c *http.Client, url string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // vider le corps permet de réutiliser la connexion
	return resp.StatusCode, nil
}
