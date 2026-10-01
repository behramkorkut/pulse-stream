// Package metrics expose les métriques Prometheus d'un programme sur un petit serveur HTTP dédié.
//
// Chaque programme (collector, processor, aggregator) crée son propre registre, y déclare ses métriques,
// puis les publie sur /metrics. Prometheus vient les lire à intervalle régulier ("scrape") : c'est lui qui
// se déplace, le programme se contente de tenir ses compteurs à jour.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry crée un registre qui contient déjà les métriques du runtime Go (goroutines, mémoire,
// ramasse-miettes) et du processus (descripteurs de fichiers, CPU).
//
// On n'utilise pas le registre global de la bibliothèque : un registre explicite se passe en paramètre,
// ce qui permet aux tests d'en créer un neuf et de lire exactement ce qu'ils ont produit.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Server publie un registre sur GET /metrics.
type Server struct {
	srv *http.Server
	ln  net.Listener
}

// NewServer ouvre le port tout de suite (échec rapide : un port déjà pris est signalé au démarrage,
// pas découvert plus tard). Le serveur ne répond qu'après l'appel de Run.
func NewServer(addr string, reg *prometheus.Registry) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return &Server{
		srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		ln:  ln,
	}, nil
}

// Addr retourne l'adresse réellement écoutée (utile avec le port 0, choisi par le système).
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Run sert les requêtes jusqu'à l'appel de Shutdown (retourne alors nil).
func (s *Server) Run() error {
	if err := s.srv.Serve(s.ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown arrête le serveur proprement.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Start ouvre le port, lance le serveur en arrière-plan et retourne la fonction qui l'arrête.
// À appeler une fois les métriques déclarées ; stop() se range dans un defer.
func Start(addr string, reg *prometheus.Registry, log *slog.Logger) (stop func(), err error) {
	s, err := NewServer(addr, reg)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := s.Run(); err != nil {
			log.Error("metrics server stopped", slog.Any("error", err))
		}
	}()
	log.Info("metrics listening", slog.String("addr", s.Addr()))

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			log.Error("closing metrics server", slog.Any("error", err))
		}
	}, nil
}
