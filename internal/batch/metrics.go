package batch

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics mesure la boucle de consommation : ce qui est commun au processor et à l'aggregator.
// Le label "consumer" les distingue quand Prometheus lit les deux.
//
// Un *Metrics nil est valide : toutes les méthodes deviennent des no-ops, ce qui permet aux tests
// (et aux programmes sans métriques) de ne rien déclarer.
type Metrics struct {
	batches  *prometheus.CounterVec // lots traités, par résultat (ok | error)
	messages prometheus.Counter     // messages dont le lot a été traité ET validé
	size     prometheus.Histogram   // nombre de messages par lot
	duration prometheus.Histogram   // durée traitement + validation des offsets, par lot
}

// NewMetrics déclare les métriques de la boucle dans reg (nil : déclarées mais non publiées).
func NewMetrics(reg prometheus.Registerer, consumer string) *Metrics {
	f := promauto.With(reg)
	labels := prometheus.Labels{"consumer": consumer}

	m := &Metrics{
		batches: f.NewCounterVec(prometheus.CounterOpts{
			Name: "pulse_batches_total", Help: "Lots traités, par résultat.", ConstLabels: labels,
		}, []string{"result"}),
		messages: f.NewCounter(prometheus.CounterOpts{
			Name: "pulse_batch_messages_total", Help: "Messages dont le lot est traité et validé.", ConstLabels: labels,
		}),
		size: f.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_batch_size", Help: "Nombre de messages par lot.", ConstLabels: labels,
			Buckets: prometheus.ExponentialBuckets(1, 2, 9), // 1, 2, 4 ... 256
		}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "pulse_batch_duration_seconds", Help: "Durée de traitement d'un lot, validation des offsets comprise.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 14), // 1 ms ... 8 s
		}),
	}

	// Les séries étiquetées n'existent qu'une fois touchées. On les crée à zéro dès le départ : sans cela,
	// un dashboard affiche "No data" au lieu de 0, et surtout rate() ignore la toute première incrémentation
	// (il lui faut deux points pour calculer une variation, or le premier point serait déjà 1).
	m.batches.WithLabelValues("ok")
	m.batches.WithLabelValues("error")
	return m
}

// RegisterLag publie le retard du consommateur : le nombre de messages publiés mais pas encore lus.
// lag est appelée à chaque lecture par Prometheus (une jauge "à la demande", rien à tenir à jour).
func RegisterLag(reg prometheus.Registerer, consumer string, lag func() int64) {
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "pulse_consumer_lag", Help: "Messages en attente de lecture pour ce consommateur.",
		ConstLabels: prometheus.Labels{"consumer": consumer},
	}, func() float64 { return float64(lag()) })
}

func (m *Metrics) observe(size int, took time.Duration, err error) {
	if m == nil {
		return
	}
	m.size.Observe(float64(size))
	m.duration.Observe(took.Seconds())
	if err != nil {
		m.batches.WithLabelValues("error").Inc()
		return
	}
	m.batches.WithLabelValues("ok").Inc()
	m.messages.Add(float64(size))
}
