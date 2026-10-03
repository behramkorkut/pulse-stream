package aggregator

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/behramkorkut/pulse-stream/internal/event"
)

// Metrics mesure ce que l'aggregator fait des messages. Un *Metrics nil est valide (no-op).
type Metrics struct {
	events  *prometheus.CounterVec // messages lus, par issue (counted | duplicate | skipped)
	buckets prometheus.Counter     // documents (site, minute) mis à jour dans MongoDB
	apply   prometheus.Histogram   // durée de chaque appel à MongoDB
	latency prometheus.Histogram   // fraîcheur : de la réception par le collector à l'écriture dans MongoDB
}

// NewMetrics déclare les métriques de l'aggregator dans reg (nil : déclarées mais non publiées).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		events: f.NewCounterVec(prometheus.CounterOpts{
			Name: "pulse_aggregator_events_total",
			Help: "Messages lus par l'aggregator, par issue : comptés, doublons écartés, inexploitables.",
		}, []string{"outcome"}),
		buckets: f.NewCounter(prometheus.CounterOpts{
			Name: "pulse_aggregator_buckets_written_total",
			Help: "Documents (site, minute) mis à jour dans MongoDB.",
		}),
		apply: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulse_aggregator_store_duration_seconds",
			Help:    "Durée d'un appel d'écriture vers MongoDB (échecs compris).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1 ms ... 8 s
		}),
		latency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulse_end_to_end_latency_seconds",
			Help:    "Fraîcheur : temps entre la réception d'un événement par le collector et l'écriture de ses compteurs dans MongoDB.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 17), // 5 ms ... 5,5 min (un arriéré se voit)
		}),
	}

	// Séries créées à zéro dès le départ (voir batch.NewMetrics : "No data" et première incrémentation perdue).
	for _, outcome := range []string{"counted", "duplicate", "skipped"} {
		m.events.WithLabelValues(outcome)
	}
	return m
}

// observeBatch est appelée une fois le lot compté ET mémorisé : on ne compte que ce qui est acquis.
func (m *Metrics) observeBatch(counted, duplicates, skipped, buckets int) {
	if m == nil {
		return
	}
	m.events.WithLabelValues("counted").Add(float64(counted))
	m.events.WithLabelValues("duplicate").Add(float64(duplicates))
	m.events.WithLabelValues("skipped").Add(float64(skipped))
	m.buckets.Add(float64(buckets))
}

func (m *Metrics) observeApply(took time.Duration) {
	if m == nil {
		return
	}
	m.apply.Observe(took.Seconds())
}

// observeLatency mesure la fraîcheur des événements comptés : de leur réception par le collector à l'instant où
// leurs compteurs sont écrits, donc visibles. C'est l'indicateur principal d'un pipeline temps réel ; le retard en
// nombre de messages (lag) ne dit pas combien de secondes on a de retard. Les événements sans heure de réception
// (publiés par un autre producteur) ne sont pas mesurés. Suppose des horloges synchronisées (NTP) entre machines.
func (m *Metrics) observeLatency(events []event.Enriched, writtenAt time.Time) {
	if m == nil {
		return
	}
	for _, e := range events {
		if !e.ReceivedAt.IsZero() {
			m.latency.Observe(writtenAt.Sub(e.ReceivedAt).Seconds())
		}
	}
}
