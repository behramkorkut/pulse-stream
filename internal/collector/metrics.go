package collector

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics mesure le collector. Un *Metrics nil est valide : toutes les méthodes deviennent des no-ops.
type Metrics struct {
	requests    *prometheus.CounterVec // requêtes sur /collect, par code HTTP
	duration    prometheus.Histogram   // durée totale d'une requête /collect
	publishTime prometheus.Histogram   // part de cette durée passée à publier dans Kafka
}

// NewMetrics déclare les métriques du collector dans reg (nil : déclarées mais non publiées).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	buckets := prometheus.ExponentialBuckets(0.001, 2, 14) // 1 ms ... 8 s

	return &Metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "pulse_collector_requests_total",
			Help: "Requêtes reçues sur /collect, par code HTTP.",
		}, []string{"code"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_collector_request_duration_seconds", Help: "Durée de traitement d'une requête /collect.",
			Buckets: buckets,
		}),
		publishTime: f.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_collector_publish_duration_seconds", Help: "Durée de publication d'un événement dans Kafka.",
			Buckets: buckets,
		}),
	}
}

func (m *Metrics) observePublish(took time.Duration) {
	if m == nil {
		return
	}
	m.publishTime.Observe(took.Seconds())
}

// instrument entoure un handler : il compte la requête par code de réponse et mesure sa durée.
// Le label "code" n'a que quelques valeurs possibles (202, 400, 413, 422, 503...) : pas d'explosion
// du nombre de séries, ce qui arriverait avec un label contenant une URL ou un identifiant.
func (m *Metrics) instrument(next http.HandlerFunc) http.HandlerFunc {
	if m == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)

		m.requests.WithLabelValues(strconv.Itoa(rec.status)).Inc()
		m.duration.Observe(time.Since(start).Seconds())
	}
}

// statusRecorder retient le code de statut écrit par le handler (http.ResponseWriter ne permet pas de le relire).
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
