package processor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics mesure ce que le processor fait des messages. Un *Metrics nil est valide (no-op).
type Metrics struct {
	events      *prometheus.CounterVec // messages écrits, par destination (enriched | dead_letter)
	deadLetters *prometheus.CounterVec // rejets, par raison (invalid_json, invalid_event, encode_error)
}

// NewMetrics déclare les métriques du processor dans reg (nil : déclarées mais non publiées).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		events: f.NewCounterVec(prometheus.CounterOpts{
			Name: "pulse_processor_events_total",
			Help: "Messages écrits par le processor, par destination.",
		}, []string{"outcome"}),
		deadLetters: f.NewCounterVec(prometheus.CounterOpts{
			Name: "pulse_processor_dead_letters_total",
			Help: "Messages rejetés vers dead-letter, par raison.",
		}, []string{"reason"}),
	}
}

// observe est appelée une fois le lot écrit dans Kafka : on ne compte que ce qui est réellement publié.
func (m *Metrics) observe(outs []Output) {
	if m == nil {
		return
	}
	for _, o := range outs {
		if o.Dead {
			m.events.WithLabelValues("dead_letter").Inc()
			m.deadLetters.WithLabelValues(o.Reason).Inc()
			continue
		}
		m.events.WithLabelValues("enriched").Inc()
	}
}
