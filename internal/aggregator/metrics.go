package aggregator

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics mesure ce que l'aggregator fait des messages. Un *Metrics nil est valide (no-op).
type Metrics struct {
	events  *prometheus.CounterVec // messages lus, par issue (counted | duplicate | skipped)
	buckets prometheus.Counter     // documents (site, minute) mis à jour dans MongoDB
	apply   prometheus.Histogram   // durée de chaque appel à MongoDB
}

// NewMetrics déclare les métriques de l'aggregator dans reg (nil : déclarées mais non publiées).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
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
	}
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
